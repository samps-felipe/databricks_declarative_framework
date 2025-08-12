from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import col, lit, current_timestamp, expr, sha2, concat_ws
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from ..core.engine import BaseEngine
from ..models.pydantic_models import PipelineConfig
import re

class SparkEngine(BaseEngine):
    def __init__(self, spark: SparkSession):
        self.spark = spark
   

    def _table_exists(self, table_name: str) -> bool:
        """Verifica se uma tabela existe no Unity Catalog de forma compatível com serverless."""
        try:
            # Usar DESCRIBE é uma maneira padrão de verificar a existência de uma tabela
            self.spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}")
            return True
        except AnalysisException as e:
            if 'TABLE_OR_VIEW_NOT_FOUND' in str(e).upper():
                return False
            # Se for outro erro, lança a exceção
            raise e
    
    def _get_final_column_name(self, spec, defaults):
        """Helper para obter o nome final da coluna com base nas regras."""
        from ..steps.transformer import _apply_rename_pattern
        if spec.rename:
            return spec.rename
        if defaults.column_rename_pattern == 'snake_case':
            return _apply_rename_pattern(spec.name)
        return spec.name

    def read(self, config: PipelineConfig) -> DataFrame:
        source_config = config.source
        if not source_config:
             raise ValueError("Configuração 'source' não encontrada para um pipeline que requer leitura de arquivos.")
        reader = self.spark.read.format(source_config.format)
        reader.options(**source_config.options)
        return reader.load(source_config.path)

    def write(self, df: DataFrame, config: PipelineConfig, validation_log_df: DataFrame = None):
        sink_config = config.sink
        if validation_log_df and config.validation_log_table:
            validation_log_df.write.mode("append").saveAsTable(config.validation_log_table)
        if sink_config.mode == 'merge':
            if sink_config.scd and sink_config.scd.type == '2':
                self._merge_scd2(df, config)
            else:
                self._merge_standard(df, config)
        else:
            self._write_standard(df, config)
    
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
        """Executa uma operação de merge (upsert) padrão, ignorando a coluna 'id'."""
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
        """Expira registros antigos em uma operação SCD2."""
        if records_to_expire.head(1):
            print(f"Expirando registros antigos...")
            (target_table.alias("target")
                .merge(records_to_expire.alias("source"), f"{join_condition} AND target.is_current = true")
                .whenMatchedUpdate(set={"is_current": "source.is_current", "end_date": "source.end_date"})
                .execute())

    def _scd2_insert_new_records(self, target_table: DeltaTable, final_inserts: DataFrame, target_table_name: str):
        """Insere novos registros em uma operação SCD2."""
        if final_inserts.head(1):
            print(f"Inserindo registros novos/atualizados...")
            existing_versions = target_table.toDF().select("hash_key", "data_hash")
            records_to_actually_insert = final_inserts.join(
                existing_versions,
                (final_inserts.hash_key == existing_versions.hash_key) & (final_inserts.data_hash == existing_versions.data_hash),
                "left_anti"
            )
            if records_to_actually_insert.head(1):
                records_to_actually_insert.write.format("delta").mode("append").saveAsTable(target_table_name)

    def _merge_scd2(self, df: DataFrame, config: PipelineConfig):
        """Executa a lógica de merge para SCD Tipo 2 de forma robusta e idempotente."""
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
        
        print("Operação de Merge SCD Tipo 2 concluída.")

    def update_table(self, config: PipelineConfig):
        """Aplica alterações de schema e metadados a uma tabela existente."""
        target_table_name = f"{config.sink.catalog}.{config.sink.schema_name}.{config.sink.table}"

        if not self._table_exists(target_table_name):
            raise Exception(f"Tabela '{target_table_name}' não existe. Use o comando 'create' primeiro.")

        print(f"Atualizando schema e metadados para a tabela '{target_table_name}'...")
        
        existing_schema = self.spark.read.table(target_table_name).schema
        existing_cols = {field.name: field for field in existing_schema.fields}
        
        for spec in config.columns:
            final_name = self._get_final_column_name(spec, config.defaults)
            
            if final_name not in existing_cols:
                comment = spec.description or ''
                self.spark.sql(f"ALTER TABLE {target_table_name} ADD COLUMN `{final_name}` {spec.type} COMMENT '{comment}'")
                print(f"  -> Coluna '{final_name}' adicionada.")
            else:
                existing_comment = existing_cols[final_name].metadata.get('comment', '')
                new_comment = spec.description or ''
                if new_comment and new_comment != existing_comment:
                    self.spark.sql(f"ALTER TABLE {target_table_name} ALTER COLUMN `{final_name}` COMMENT '{new_comment}'")
                    print(f"  -> Comentário da coluna '{final_name}' atualizado.")
    
        print("Verificando necessidade de reprocessar hash_key...")
        try:
            tbl_properties_str = self.spark.sql(f"DESCRIBE TABLE EXTENDED {target_table_name}").filter("col_name = 'Table Properties'").collect()[0]['data_type']
            match = re.search(r"framework\\.primary_keys=([a-zA-Z0-9_,]+)", tbl_properties_str)
            existing_pks_str = match.group(1) if match else ""
            existing_pks = set(existing_pks_str.split(',')) if existing_pks_str else set()
        except (IndexError, AttributeError):
            existing_pks = set()

        new_pks = {self._get_final_column_name(spec, config.defaults) for spec in config.columns if spec.pk}

        if new_pks != existing_pks:
            print(f"[AVISO] As chaves primárias mudaram de {existing_pks} para {new_pks}. Reprocessando a coluna 'hash_key'...")
            
            full_table_df = self.spark.read.table(target_table_name)
            
            reprocessed_df = full_table_df.drop("hash_key").withColumn(
                "hash_key", sha2(concat_ws("||", *sorted(list(new_pks))), 256)
            )
            
            # --- LÓGICA CORRIGIDA ---
            # Usa um MERGE para atualizar a coluna hash_key em loco, preservando o id.
            target_table = DeltaTable.forName(self.spark, target_table_name)
            (target_table.alias("target")
                .merge(
                    reprocessed_df.alias("source"),
                    "target.id = source.id" # A junção é na chave surrogate imutável
                )
                .whenMatchedUpdate(set={
                    "hash_key": "source.hash_key" # Atualiza apenas a hash_key
                })
                .execute())
            
            self.spark.sql(f"ALTER TABLE {target_table_name} SET TBLPROPERTIES ('framework.primary_keys' = '{','.join(sorted(list(new_pks)))}')")
            print("  -> Reprocessamento do hash_key concluído.")
        else:
            print("  -> Chaves primárias não foram alteradas. Nenhuma ação necessária para hash_key.")
            
    
    def create_table(self, config: PipelineConfig):
        """Cria uma tabela no Unity Catalog com uma lógica unificada para Silver e Gold."""
        target_table_name = f"{config.sink.catalog}.{config.sink.schema_name}.{config.sink.table}"
        
        if self._table_exists(target_table_name):
            print(f"Tabela '{target_table_name}' já existe. Nenhuma ação será tomada.")
        else:
            self._create_unified_table(config)
        
        if config.validation_log_table and not self._table_exists(config.validation_log_table):
            self._create_validation_log_table(config)
    
    def _create_unified_table(self, config: PipelineConfig):
        """Constrói e executa o DDL para qualquer tabela (Silver ou Gold)."""
        sink_config = config.sink
        target_table_name = f"{sink_config.catalog}.{sink_config.schema_name}.{sink_config.table}"
        
        print(f"Construindo DDL para a tabela '{target_table_name}' (Tipo: {config.pipeline_type})...")
        
        column_definitions = ["id BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Chave primária surrogate.'"]
        primary_keys = []
        
        for spec in config.columns:
            final_name = self._get_final_column_name(spec, config.defaults)
            if spec.pk: primary_keys.append(final_name)
            
            is_not_null = any(v.rule == "not_null" for v in spec.validation_rules)
            not_null_str = " NOT NULL" if is_not_null else ""
            comment_str = f" COMMENT '{spec.description}'" if spec.description else ""
            column_definitions.append(f"{final_name} {spec.type}{not_null_str}{comment_str}")
        
        # Adiciona colunas de controle SCD se configurado
        if sink_config.scd and sink_config.scd.type == '2':
            column_definitions.extend([
                "data_hash STRING COMMENT 'Hash para detecção de mudanças.'",
                "is_current BOOLEAN COMMENT 'Flag de registro ativo.'",
                "start_date TIMESTAMP COMMENT 'Data de início da validade.'",
                "end_date TIMESTAMP COMMENT 'Data de fim da validade.'"
            ])
        else:
            column_definitions.append("created_at TIMESTAMP COMMENT 'Timestamp de criação.'")
        
        # Adiciona colunas de controle padrão
        column_definitions.extend([
            "hash_key STRING COMMENT 'Hash das chaves primárias.'",
            "updated_at TIMESTAMP COMMENT 'Timestamp da última atualização.'"
        ])
        
        # Adiciona constraints de Chave Estrangeira (se houver)
        fk_definitions = []
        if sink_config.foreign_keys:
            for fk in sink_config.foreign_keys:
                fk_definitions.append(f"CONSTRAINT {fk.name} FOREIGN KEY ({', '.join(fk.local_columns)}) REFERENCES {fk.references_table}({', '.join(fk.references_columns)})")

        pk_constraint = f", CONSTRAINT pk_{sink_config.table} PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else f", CONSTRAINT pk_{sink_config.table} PRIMARY KEY (id)"
        
        all_definitions = column_definitions + fk_definitions
        ddl = f"CREATE TABLE {target_table_name} ({', '.join(all_definitions)}{pk_constraint})"
        
        if config.description: ddl += f" COMMENT '{config.description}'"
        if sink_config.partition_by: ddl += f" PARTITIONED BY ({', '.join(sink_config.partition_by)})"

        print("Executando DDL de criação da tabela...")
        self.spark.sql(ddl)
        if primary_keys:
            self.spark.sql(f"ALTER TABLE {target_table_name} SET TBLPROPERTIES ('framework.primary_keys' = '{','.join(sorted(list(primary_keys)))}')")
        print("Tabela criada com sucesso.")

    def _create_validation_log_table(self, config: PipelineConfig):
        """Cria a tabela de log de validação com um schema fixo."""
        print(f"Criando tabela de log de validação: {config.validation_log_table}")
        log_ddl = f"""
        CREATE TABLE {config.validation_log_table} (
            pipeline_name STRING,
            validation_rule STRING,
            failed_column STRING,
            failed_value STRING,
            log_timestamp TIMESTAMP,
            hash_key STRING COMMENT 'Hash das chaves primárias do registro que falhou.'
        )
        """
        self.spark.sql(log_ddl)
        print("Tabela de log de validação criada com sucesso.")


    def read_table(self, table_name: str) -> DataFrame:
        """Lê uma tabela Delta e a retorna como um DataFrame."""
        return self.spark.read.table(table_name)

    def compare_dataframes(self, df_actual: DataFrame, df_expected: DataFrame) -> bool:
        """Compara o schema e os dados de dois DataFrames."""
        if df_actual.schema != df_expected.schema:
            print("Erro: Os schemas são diferentes.")
            return False
        
        if df_actual.count() != df_expected.count():
            print(f"Erro: A contagem de linhas é diferente. Atual: {df_actual.count()}, Esperado: {df_expected.count()}")
            return False

        # exceptAll retorna linhas que estão em um DF mas não no outro.
        # Se ambos os resultados forem vazios, os DFs são idênticos.
        diff1 = df_actual.exceptAll(df_expected)
        diff2 = df_expected.exceptAll(df_actual)

        return diff1.count() == 0 and diff2.count() == 0

    def show_differences(self, df_actual: DataFrame, df_expected: DataFrame):
        """Mostra as linhas que diferem entre os dois DataFrames."""
        print("--- Detalhes da Diferença ---")
        print("\nLinhas no resultado ATUAL que NÃO estão no ESPERADO:")
        df_actual.exceptAll(df_expected).show()

        print("\nLinhas no resultado ESPERADO que NÃO estão no ATUAL:")
        df_expected.exceptAll(df_actual).show()
    
    def execute_gold_transformation(self, config: PipelineConfig) -> DataFrame:
        """Executa uma sequência de transformações SQL para um pipeline Gold."""
        print("  -> Lendo dependências...")
        for dep in config.dependencies:
            table_name = dep.split('.')[-1]
            self.read_table(dep).createOrReplaceTempView(table_name)
            print(f"  -> Dependência registrada: Tabela '{dep}' como view SQL '{table_name}'")

        df_result = None
        for i, transform_step in enumerate(config.transformation):
            step_name = transform_step.name.replace(' ', '_').lower()
            print(f"  -> Executando passo de transformação Gold {i+1}/{len(config.transformation)}: '{step_name}'")
            if transform_step.type == 'sql':
                df_result = self.spark.sql(transform_step.sql)
                # O resultado de cada passo se torna uma view para o próximo
                df_result.createOrReplaceTempView(step_name)
                print(f"    -> View intermediária '{step_name}' criada.")
            else:
                raise NotImplementedError(f"Transformação do tipo '{transform_step.type}' não suportada para Gold.")
        
        if df_result is None:
            raise ValueError("Nenhum passo de transformação foi executado para o pipeline Gold.")
        
        return df_result
