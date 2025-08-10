from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from ..core.engine import BaseEngine
from ..models.pydantic_models import PipelineConfig
import re

class SparkEngine(BaseEngine):
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def _get_final_column_name(self, spec, defaults):
        """Helper para obter o nome final da coluna com base nas regras."""
        from ..steps.transformer import _apply_rename_pattern
        if spec.rename:
            return spec.rename
        if defaults.column_rename_pattern == 'snake_case':
            return _apply_rename_pattern(spec.name)
        return spec.name

    def _build_spark_schema(self, config: PipelineConfig) -> StructType:
        """Constrói um StructType do Spark a partir da configuração YAML."""
        fields = []
        for spec in config.columns:
            final_name = self._get_final_column_name(spec, config.defaults)
            metadata = {"comment": spec.description} if spec.description else {}
            fields.append(StructField(final_name, spec.type, True, metadata))
        
        # Adiciona as colunas internas do framework
        fields.append(StructField("hash_key", "string", True, {"comment": "Hash das chaves primárias para otimização de merge."}))
        fields.append(StructField("created_at", "timestamp", True, {"comment": "Timestamp de criação do registro."}))
        fields.append(StructField("updated_at", "timestamp", True, {"comment": "Timestamp da última atualização do registro."}))
        
        return StructType(fields)

    def read(self, config: PipelineConfig) -> DataFrame:
        source_config = config.source
        reader = self.spark.read.format(source_config.format)
        reader.options(**source_config.options)
        return reader.load(source_config.path)

    def write(self, df: DataFrame, config: PipelineConfig, validation_log_df: DataFrame = None):
        sink_config = config.sink
        
        if validation_log_df and config.validation_log_table:
            validation_log_df.write.mode("append").saveAsTable(config.validation_log_table)

        # Direciona para o método de escrita correto
        if sink_config.mode == 'merge':
            if sink_config.scd and sink_config.scd.type == '2':
                self._merge_scd2(df, config)
            else:
                self._merge_standard(df, config)
        else:
            self._write_standard(df, config)
    

    def _merge_scd2(self, df: DataFrame, config: PipelineConfig):
        """Executa a lógica de merge para Slowly Changing Dimension Tipo 2."""
        sink_config = config.sink
        target_table_name = f"{sink_config.catalog}.{sink_config.schema_name}.{sink_config.table}"
        
        source_df = df.alias("source")
        target_table = DeltaTable.forName(self.spark, target_table_name)
        target_df = target_table.toDF().alias("target")

        pk_cols = [spec.rename or spec.name for spec in config.columns if spec.pk]
        join_condition = " AND ".join([f"source.{pk} = target.{pk}" for pk in pk_cols])

        # 1. Identifica registros que mudaram (mesma PK, data_hash diferente)
        # Apenas compara com a versão atual no destino
        changed_records = source_df.join(
            target_df,
            (F.expr(join_condition)) & (F.col("target.is_current") == True) & (F.col("source.data_hash") != F.col("target.data_hash")),
            "inner"
        ).select("source.*")

        # 2. Prepara os registros que serão expirados
        records_to_expire = changed_records.select(
            *pk_cols,
            F.lit(False).alias("is_current"),
            F.current_timestamp().alias("end_date")
        )

        # 3. Prepara os novos registros (incluindo os que mudaram e os que são totalmente novos)
        # A união dos dois conjuntos abaixo representa o que precisa ser inserido
        new_records_to_insert = changed_records.withColumn("is_current", F.lit(True)) \
                                               .withColumn("start_date", F.current_timestamp()) \
                                               .withColumn("end_date", F.lit(None).cast("timestamp"))

        brand_new_records = source_df.join(
            target_df,
            join_condition,
            "left_anti"
        ).withColumn("is_current", F.lit(True)) \
         .withColumn("start_date", F.current_timestamp()) \
         .withColumn("end_date", F.lit(None).cast("timestamp"))

        final_inserts = new_records_to_insert.unionByName(brand_new_records)

        # Executa o MERGE
        # Passo A: Expira os registros antigos
        (target_table.alias("target")
            .merge(
                records_to_expire.alias("source"),
                join_condition
            )
            .whenMatchedUpdate(set={
                "is_current": "source.is_current",
                "end_date": "source.end_date"
            })
            .execute()
        )
        
        # Passo B: Insere os novos registros
        target_table.merge(
            final_inserts.alias("source"),
            "1 = 0" # Condição falsa para garantir que seja sempre INSERT
        ).whenNotMatchedInsertAll().execute()
        
        print("Operação de Merge SCD Tipo 2 concluída.")
    
    
    def _write_standard(self, df: DataFrame, config: PipelineConfig):
        sink_config = config.sink
        target_table = f"{sink_config.catalog}.{sink_config.schema_name}.{sink_config.table}"
        writer = df.write.mode(sink_config.mode)

        if sink_config.partition_by:
            writer = writer.partitionBy(*sink_config.partition_by)
        
        if sink_config.mode in ['overwrite_partition', 'overwrite_where'] and sink_config.overwrite_condition:
             writer = writer.option("replaceWhere", sink_config.overwrite_condition)

        writer.option("mergeSchema", "true").saveAsTable(target_table)

    def _merge(self, df: DataFrame, config: PipelineConfig):
        sink_config = config.sink
        target_table_name = f"{sink_config.catalog}.{sink_config.schema_name}.{sink_config.table}"
        
        delta_table = DeltaTable.forName(self.spark, target_table_name)
        
        update_set = {col: f"source.{col}" for col in df.columns if col not in ["hash_key", "created_at"]}
        update_set["updated_at"] = "source.updated_at"

        insert_values = {col: f"source.{col}" for col in df.columns}
        insert_values["created_at"] = "source.updated_at" # Para novos registros, created_at é o mesmo que updated_at

        (delta_table.alias("target")
            .merge(df.alias("source"), "target.hash_key = source.hash_key")
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_values)
            .execute())
        
    def create_table(self, config: PipelineConfig):
        """Cria uma tabela Delta vazia com schema, constraints e comentários do Unity Catalog."""
        sink_config = config.sink
        target_table_name = f"{sink_config.catalog}.{sink_config.schema_name}.{sink_config.table}"
        
        if self.spark._jsparkSession.catalog().tableExists(target_table_name):
            print(f"Tabela '{target_table_name}' já existe. Nenhuma ação será tomada.")
            return

        print(f"Construindo DDL para a tabela '{target_table_name}'...")
        
        # Constrói a lista de colunas para o DDL
        column_definitions = [
            "id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) COMMENT 'Chave primária surrogate auto-incrementada.'"
        ]
        primary_keys = []

        for spec in config.columns:
            final_name = self._get_final_column_name(spec, config.defaults)
            if spec.pk:
                primary_keys.append(final_name)
            
            is_not_null = any(v.rule == "not_null" for v in spec.validate)
            not_null_str = " NOT NULL" if is_not_null else ""
            comment_str = f" COMMENT '{spec.description}'" if spec.description else ""
            column_definitions.append(f"{final_name} {spec.type}{not_null_str}{comment_str}")

        # Adiciona colunas do framework
        column_definitions.append("hash_key STRING COMMENT 'Hash das chaves primárias para otimização de merge.'")
        column_definitions.append("updated_at TIMESTAMP COMMENT 'Timestamp da última atualização do registro.'")

        if config.sink.scd and config.sink.scd.type == '2':
            column_definitions.append("data_hash STRING COMMENT 'Hash dos dados para detecção de mudanças.'")
            column_definitions.append("is_current BOOLEAN COMMENT 'Flag que indica se o registro é a versão ativa.'")
            column_definitions.append("start_date TIMESTAMP COMMENT 'Data de início da validade do registro.'")
            column_definitions.append("end_date TIMESTAMP COMMENT 'Data de fim da validade do registro.'")
        else:
             column_definitions.append("created_at TIMESTAMP COMMENT 'Timestamp de criação do registro.'")
        
        # Define a constraint de Primary Key
        if primary_keys:
            pk_constraint = f", CONSTRAINT pk_{sink_config.table} PRIMARY KEY ({', '.join(primary_keys)})"
        else:
            # Se nenhuma PK for definida, o ID auto-incrementado se torna a PK.
            pk_constraint = f", CONSTRAINT pk_{sink_config.table} PRIMARY KEY (id)"

        # Monta o DDL final
        ddl = f"""
        CREATE TABLE {target_table_name} (
            {', '.join(column_definitions)}
            {pk_constraint}
        )
        """
        if config.description:
            ddl += f" COMMENT '{config.description}'"
        if sink_config.partition_by:
            ddl += f" PARTITIONED BY ({', '.join(sink_config.partition_by)})"

        print("Executando DDL de criação da tabela...")
        self.spark.sql(ddl)
        
        # Armazena as PKs nas propriedades da tabela para futuras comparações
        self.spark.sql(f"ALTER TABLE {target_table_name} SET TBLPROPERTIES ('framework.primary_keys' = '{','.join(primary_keys)}')")
        print("Tabela criada com sucesso.")

    def update_table(self, config: PipelineConfig):
        """Aplica alterações de schema e metadados a uma tabela existente."""
        sink_config = config.sink
        target_table_name = f"{sink_config.catalog}.{sink_config.schema_name}.{sink_config.table}"

        if not self.spark._jsparkSession.catalog().tableExists(target_table_name):
            raise Exception(f"Tabela '{target_table_name}' não existe. Use o comando 'create' primeiro.")

        print(f"Atualizando schema e metadados para a tabela '{target_table_name}'...")
        
        target_schema = self._build_spark_schema(config)
        existing_schema = self.spark.read.table(target_table_name).schema
        existing_cols = {field.name: field for field in existing_schema.fields}
        
        # Adiciona novas colunas e atualiza comentários
        for field in target_schema.fields:
            if field.name not in existing_cols:
                comment = field.metadata.get('comment', '')
                self.spark.sql(f"ALTER TABLE {target_table_name} ADD COLUMN {field.name} {field.dataType.simpleString()} COMMENT '{comment}'")
                print(f"  -> Coluna '{field.name}' adicionada.")
            else:
                existing_comment = existing_cols[field.name].metadata.get('comment', '')
                new_comment = field.metadata.get('comment', '')
                if new_comment and new_comment != existing_comment:
                    self.spark.sql(f"ALTER TABLE {target_table_name} ALTER COLUMN {field.name} COMMENT '{new_comment}'")
                    print(f"  -> Comentário da coluna '{field.name}' atualizado.")
        
        # Lógica para reprocessar hash_key se as PKs mudarem
        print("Verificando necessidade de reprocessar hash_key...")
        try:
            tbl_properties = self.spark.sql(f"DESCRIBE TABLE EXTENDED {target_table_name}").filter("col_name = 'Table Properties'").collect()[0]['data_type']
            existing_pks_str = re.search(r"framework.primary_keys=([a-zA-Z0-9_,]+)", tbl_properties).group(1)
            existing_pks = set(existing_pks_str.split(','))
        except (IndexError, AttributeError):
            existing_pks = set()

        new_pks = {self._get_final_column_name(spec, config.defaults) for spec in config.columns if spec.pk}

        if new_pks != existing_pks:
            print(f"[AVISO] As chaves primárias mudaram de {existing_pks} para {new_pks}. Reprocessando a coluna 'hash_key'...")
            from pyspark.sql.functions import sha2, concat_ws
            
            full_table_df = self.spark.read.table(target_table_name)
            reprocessed_df = full_table_df.drop("hash_key").withColumn(
                "hash_key", sha2(concat_ws("||", *sorted(list(new_pks))), 256)
            )
            
            reprocessed_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table_name)
            self.spark.sql(f"ALTER TABLE {target_table_name} SET TBLPROPERTIES ('framework.primary_keys' = '{','.join(sorted(list(new_pks)))}')")
            print("  -> Reprocessamento do hash_key concluído.")
        else:
            print("  -> Chaves primárias não foram alteradas. Nenhuma ação necessária para hash_key.")

    def read_table(self, table_name: str) -> DataFrame:
        return self.spark.read.table(table_name)

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
