from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import col, lit, current_timestamp, expr, sha2, concat_ws
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from ...core.engine import BaseEngine, register_engine
from ...models.pydantic_models import PipelineConfig
import re
from ...logger import logger
from ...exceptions import StepExecutionError
from .steps.reader import ReadStep
from .steps.transformer import TransformStep
from .steps.validator import ValidateStep
from .steps.writer import WriterStep

@register_engine('spark')
class SparkEngine(BaseEngine):
    def __init__(self, config: PipelineConfig):
        self.spark = SparkSession.builder.appName(config.pipeline_name).getOrCreate()
        self.config = config

        self.reader = ReadStep(self)
        self.transformer = TransformStep(self)
        self.validator = ValidateStep(self)
        self.writer = WriterStep(self)
        # self.tester = TestStep(self)
    
    def create_table(self):
        """Creates a table in Unity Catalog with unified logic for Silver and Gold."""
        config = self.config

        target_table_name = f"{config.sink.catalog}.{config.sink.schema_name}.{config.sink.table}"

        if self._table_exists(target_table_name):
            logger.info(f"Table '{target_table_name}' already exists. No action will be taken.")
        else:
            self._create_unified_table(config)

        if config.validation_log_table and not self._table_exists(config.validation_log_table):
            self._create_validation_log_table(config)
    
    def update_table(self, config: PipelineConfig):
        """Applies schema and metadata changes to an existing table."""
        target_table_name = f"{config.sink.catalog}.{config.sink.schema_name}.{config.sink.table}"

        if not self._table_exists(target_table_name):
            raise Exception(f"Table '{target_table_name}' does not exist. Use the 'create' command first.")

        logger.info(f"Updating schema and metadata for table '{target_table_name}'...")

        existing_schema = self.spark.read.table(target_table_name).schema
        existing_cols = {field.name: field for field in existing_schema.fields}

        for spec in config.columns:
            final_name = self._get_final_column_name(spec, config.defaults)

            if final_name not in existing_cols:
                comment = spec.description or ''
                self.spark.sql(f"ALTER TABLE {target_table_name} ADD COLUMN `{final_name}` {spec.type} COMMENT '{comment}'")
                logger.info(f"  -> Column '{final_name}' added.")
            else:
                existing_comment = existing_cols[final_name].metadata.get('comment', '')
                new_comment = spec.description or ''
                if new_comment and new_comment != existing_comment:
                    self.spark.sql(f"ALTER TABLE {target_table_name} ALTER COLUMN `{final_name}` COMMENT '{new_comment}'")
                    logger.info(f"  -> Column comment for '{final_name}' updated.")

        logger.info("Checking if hash_key needs to be reprocessed...")
        try:
            tbl_properties_str = self.spark.sql(f"DESCRIBE TABLE EXTENDED {target_table_name}").filter("col_name = 'Table Properties'").collect()[0]['data_type']
            match = re.search(r"framework\\.primary_keys=([a-zA-Z0-9_,]+)", tbl_properties_str)
            existing_pks_str = match.group(1) if match else ""
            existing_pks = set(existing_pks_str.split(',')) if existing_pks_str else set()
        except (IndexError, AttributeError):
            existing_pks = set()

        new_pks = {self._get_final_column_name(spec, config.defaults) for spec in config.columns if spec.pk}

        if new_pks != existing_pks:
            logger.warning(f"[WARNING] Primary keys changed from {existing_pks} to {new_pks}. Reprocessing 'hash_key' column...")

            full_table_df = self.spark.read.table(target_table_name)

            reprocessed_df = full_table_df.drop("hash_key").withColumn(
                "hash_key", sha2(concat_ws("||", *sorted(list(new_pks))), 256)
            )

            # --- CORRECTED LOGIC ---
            # Uses a MERGE to update the hash_key column in place, preserving the id.
            target_table = DeltaTable.forName(self.spark, target_table_name)
            (target_table.alias("target")
                .merge(
                    reprocessed_df.alias("source"),
                    "target.id = source.id" # Join on immutable surrogate key
                )
                .whenMatchedUpdate(set={
                    "hash_key": "source.hash_key" # Updates only the hash_key
                })
                .execute())

            self.spark.sql(f"ALTER TABLE {target_table_name} SET TBLPROPERTIES ('framework.primary_keys' = '{','.join(sorted(list(new_pks)))}')")
            logger.info("  -> hash_key reprocessing completed.")
        else:
            logger.info("  -> Primary keys have not changed. No action required for hash_key.")

    def read(self) -> DataFrame:
        return self.reader.execute()

    def process(self, df):
        # Implement processing logic using pipeline steps (transformations, etc.)
        try:
            # TODO: Rever lógica de transformação customizada para permitir que o usuário defina uma função customizada
            if self.config.custom_transform_script:
                self.logger.info(f"Executing custom transform script: {self.config.custom_transform_script}")
                module_path, func_name = self.config.custom_transform_script.rsplit('.', 1)
                custom_module = import_module(module_path)
                custom_transform_func = getattr(custom_module, func_name)
                df_transformed = custom_transform_func(df)
            else:
                df_transformed = self.transformer.execute(df)
        
        except Exception as e:
            self.logger.exception("Failure in TRANSFORM step for pipeline '%s'", self.config.pipeline_name)
            raise StepExecutionError(f"Failure in TRANSFORM step: {e}") from e

        
        return df_transformed

    def validate(self, df):
        # Implement validation logic using Spark
        # Placeholder: returns df unchanged and None for validation log
        if validation_log_df and config.validation_log_table:
            validation_log_df.write.mode("append").saveAsTable(config.validation_log_table)
        
        return df, None
    
    def write(self, df: DataFrame):
        sink_config = self.config.sink

        if sink_config.mode == 'merge':
            if sink_config.scd and sink_config.scd.type == '2':
                self._merge_scd2(df, self.config)
            else:
                self._merge_standard(df, self.config)
        else:
            self._write_standard(df, self.config)

    def test(self):
        pass


    def _table_exists(self, table_name: str) -> bool:
        """Checks if a table exists in Unity Catalog in a serverless-compatible way."""
        try:
            # Using DESCRIBE is a standard way to check for table existence
            self.spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}")
            return True
        except AnalysisException as e:
            if 'TABLE_OR_VIEW_NOT_FOUND' in str(e).upper():
                return False
            # If another error, raise the exception
            raise e
    
    def _get_final_column_name(self, spec, defaults):
        """Helper to get the final column name based on rules."""
        from .steps.transformer import _apply_rename_pattern
        if spec.rename:
            return spec.rename
        if defaults.column_rename_pattern == 'snake_case':
            return _apply_rename_pattern(spec.name)
        return spec.name


    
    def _write_standard(self, df: DataFrame, config: PipelineConfig):
        sink_config = config.sink
        target_table = f"{sink_config.catalog}.{sink_config.schema_name}.{sink_config.table}"
        writer = df.write.mode(sink_config.mode)
        if sink_config.partition_by:
            writer = writer.partitionBy(*sink_config.partition_by)
        if sink_config.mode in ['overwrite_partition', 'overwrite_where'] and sink_config.overwrite_condition:
            writer = writer.option("replaceWhere", sink_config.overwrite_condition)
        writer.option("mergeSchema", "true").saveAsTable(target_table)

    def _merge_standard(self, df: DataFrame, config: PipelineConfig):
        """Performs a standard merge (upsert) operation, ignoring the 'id' column."""
        target_table_name = f"{config.sink.catalog}.{config.sink.schema_name}.{config.sink.table}"
        delta_table = DeltaTable.forName(self.spark, target_table_name)
        
        update_map = {c: f"source.{c}" for c in df.columns if c not in ["id", "hash_key", "created_at"]}
        update_map["updated_at"] = "source.updated_at"
        
        insert_map = {c: f"source.{c}" for c in df.columns if c != "id"}
        if "created_at" in df.columns:
            insert_map["created_at"] = "source.updated_at"

        (delta_table.alias("target")
            .merge(df.alias("source"), "target.hash_key = source.hash_key")
            .whenMatchedUpdate(set=update_map)
            .whenNotMatchedInsert(values=insert_map)
            .execute())

    def _scd2_expire_old_records(self, target_table: DeltaTable, records_to_expire: DataFrame, join_condition: str):
        """Expires old records in an SCD2 operation."""
        if records_to_expire.head(1):
            logger.info(f"Expiring old records...")
            (target_table.alias("target")
                .merge(records_to_expire.alias("source"), f"{join_condition} AND target.is_current = true")
                .whenMatchedUpdate(set={"is_current": "source.is_current", "end_date": "source.end_date"})
                .execute())

    def _scd2_insert_new_records(self, target_table: DeltaTable, final_inserts: DataFrame, target_table_name: str):
        """Inserts new records in an SCD2 operation."""
        if final_inserts.head(1):
            logger.info(f"Inserting new/updated records...")
            existing_versions = target_table.toDF().select("hash_key", "data_hash")
            records_to_actually_insert = final_inserts.join(
                existing_versions,
                (final_inserts.hash_key == existing_versions.hash_key) & (final_inserts.data_hash == existing_versions.data_hash),
                "left_anti"
            )
            if records_to_actually_insert.head(1):
                records_to_actually_insert.write.format("delta").mode("append").saveAsTable(target_table_name)

    def _merge_scd2(self, df: DataFrame, config: PipelineConfig):
        """Executes robust and idempotent merge logic for SCD Type 2."""
        target_table_name = f"{config.sink.catalog}.{config.sink.schema_name}.{config.sink.table}"
        
        source_df = df.alias("source")
        target_table = DeltaTable.forName(self.spark, target_table_name)
        target_df = target_table.toDF().alias("target")
        join_condition = "source.hash_key = target.hash_key"

        changed_records = source_df.join(
            target_df.filter(col("target.is_current") == True),
            expr(join_condition), "inner"
        ).where(col("source.data_hash") != col("target.data_hash")).select("source.*")

        records_to_expire = changed_records.select(col("hash_key"), lit(False).alias("is_current"), current_timestamp().alias("end_date"))

        new_records_to_insert = changed_records.withColumn("is_current", lit(True)).withColumn("start_date", current_timestamp()).withColumn("end_date", lit(None).cast("timestamp"))
        brand_new_records = source_df.join(target_df.filter("is_current = true"), "hash_key", "left_anti").withColumn("is_current", lit(True)).withColumn("start_date", current_timestamp()).withColumn("end_date", lit(None).cast("timestamp"))
        final_inserts = new_records_to_insert.unionByName(brand_new_records, allowMissingColumns=True)

        self._scd2_expire_old_records(target_table, records_to_expire, join_condition)
        self._scd2_insert_new_records(target_table, final_inserts, target_table_name)
        
        logger.info("SCD Type 2 Merge operation completed.")
    
    def _create_unified_table(self, config: PipelineConfig):
        """Builds and executes the DDL for any table (Silver or Gold)."""
        sink_config = config.sink
        target_table_name = f"{sink_config.catalog}.{sink_config.schema_name}.{sink_config.table}"

        logger.info(f"Building DDL for table '{target_table_name}' (Type: {config.pipeline_type})...")

        column_definitions = ["id BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate primary key.'"]
        primary_keys = []

        for spec in config.columns:
            final_name = self._get_final_column_name(spec, config.defaults)
            if spec.pk:
                primary_keys.append(final_name)
            is_not_null = any(v.rule == "not_null" for v in spec.validation_rules)
            not_null_str = " NOT NULL" if is_not_null else ""
            comment_str = f" COMMENT '{spec.description}'" if spec.description else ""
            column_definitions.append(f"{final_name} {spec.type}{not_null_str}{comment_str}")

        # Adds SCD control columns if configured
        if sink_config.scd and sink_config.scd.type == '2':
            column_definitions.extend([
                "data_hash STRING COMMENT 'Hash for change detection.'",
                "is_current BOOLEAN COMMENT 'Active record flag.'",
                "start_date TIMESTAMP COMMENT 'Validity start date.'",
                "end_date TIMESTAMP COMMENT 'Validity end date.'"
            ])
        else:
            column_definitions.append("created_at TIMESTAMP COMMENT 'Creation timestamp.'")

        # Adds standard control columns
        column_definitions.extend([
            "hash_key STRING COMMENT 'Hash of primary keys.'",
            "updated_at TIMESTAMP COMMENT 'Last update timestamp.'"
        ])

        # Adds Foreign Key constraints (if any)
        fk_definitions = []
        if sink_config.foreign_keys:
            for fk in sink_config.foreign_keys:
                fk_definitions.append(f"CONSTRAINT {fk.name} FOREIGN KEY ({', '.join(fk.local_columns)}) REFERENCES {fk.references_table}({', '.join(fk.references_columns)})")

        pk_constraint = f", CONSTRAINT pk_{sink_config.table} PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else f", CONSTRAINT pk_{sink_config.table} PRIMARY KEY (id)"

        all_definitions = column_definitions + fk_definitions
        ddl = f"CREATE TABLE {target_table_name} ({', '.join(all_definitions)}{pk_constraint})"

        if config.description:
            ddl += f" COMMENT '{config.description}'"
        if sink_config.partition_by:
            ddl += f" PARTITIONED BY ({', '.join(sink_config.partition_by)})"

        logger.info("Executing table creation DDL...")
        self.spark.sql(ddl)
        if primary_keys:
            self.spark.sql(f"ALTER TABLE {target_table_name} SET TBLPROPERTIES ('framework.primary_keys' = '{','.join(sorted(list(primary_keys)))}')")
        logger.info("Table created successfully.")

    def _create_validation_log_table(self, config: PipelineConfig):
        """Creates the validation log table with a fixed schema."""
        logger.info(f"Creating validation log table: {config.validation_log_table}")
        log_ddl = f"""
        CREATE TABLE {config.validation_log_table} (
            pipeline_name STRING,
            validation_rule STRING,
            failed_column STRING,
            failed_value STRING,
            log_timestamp TIMESTAMP,
            hash_key STRING COMMENT 'Hash of the primary keys of the failed record.'
        )
        """
        self.spark.sql(log_ddl)
        logger.info("Validation log table created successfully.")


    def read_table(self, table_name: str) -> DataFrame:
        """Reads a Delta table and returns it as a DataFrame."""
        return self.spark.read.table(table_name)

    def compare_dataframes(self, df_actual: DataFrame, df_expected: DataFrame) -> bool:
        """Compares the schema and data of two DataFrames."""
        if df_actual.schema != df_expected.schema:
            logger.error("Error: Schemas are different.")
            return False

        if df_actual.count() != df_expected.count():
            logger.error(f"Error: Row count is different. Actual: {df_actual.count()}, Expected: {df_expected.count()}")
            return False

        # exceptAll returns rows that are in one DF but not in the other.
        # If both results are empty, the DFs are identical.
        diff1 = df_actual.exceptAll(df_expected)
        diff2 = df_expected.exceptAll(df_actual)

        return diff1.count() == 0 and diff2.count() == 0

    def show_differences(self, df_actual: DataFrame, df_expected: DataFrame):
        """Shows the rows that differ between the two DataFrames."""
        logger.info("--- Difference Details ---")
        logger.info("\nRows in ACTUAL result that are NOT in EXPECTED:")
        df_actual.exceptAll(df_expected).show()

        logger.info("\nRows in EXPECTED result that are NOT in ACTUAL:")
        df_expected.exceptAll(df_actual).show()
    
    def execute_gold_transformation(self, config: PipelineConfig) -> DataFrame:
        """Executes a sequence of SQL transformations for a Gold pipeline."""
        logger.info("  -> Reading dependencies...")
        for dep in config.dependencies:
            table_name = dep.split('.')[-1]
            self.read_table(dep).createOrReplaceTempView(table_name)
            logger.info(f"  -> Dependency registered: Table '{dep}' as SQL view '{table_name}'")

        df_result = None
        for i, transform_step in enumerate(config.transformation):
            step_name = transform_step.name.replace(' ', '_').lower()
            logger.info(f"  -> Executing Gold transformation step {i+1}/{len(config.transformation)}: '{step_name}'")
            if transform_step.type == 'sql':
                df_result = self.spark.sql(transform_step.sql)
                # The result of each step becomes a view for the next
                df_result.createOrReplaceTempView(step_name)
                logger.info(f"    -> Intermediate view '{step_name}' created.")
            else:
                raise NotImplementedError(f"Transformation type '{transform_step.type}' not supported for Gold.")

        if df_result is None:
            raise ValueError("No transformation step was executed for the Gold pipeline.")

        return df_result
