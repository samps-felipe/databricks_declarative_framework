# Databricks notebook source

# DBTITLE 1,Setup do Ambiente de Teste
%pip install pyyaml pydantic

# COMMAND ----------

# DBTITLE 2,Importações e Definição da Classe de Teste
import yaml
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, lit, col, concat_ws

# Adiciona o diretório do framework ao path
# ATENÇÃO: Ajuste este caminho para o local do seu repositório
FRAMEWORK_PATH = '/Workspace/Repos/seu_usuario/framework_declarativo_v2/src'
if FRAMEWORK_PATH not in sys.path:
    sys.path.append(FRAMEWORK_PATH)

from framework.core.pipeline import Pipeline
from framework.engines.spark_engine import SparkEngine
from framework.models.pydantic_models import PipelineConfig

class TestFramework:
    """Suíte de testes estruturada para o framework declarativo."""
    spark = None
    CATALOG = "dev"
    SCHEMA = "framework_tests_v2"
    VOLUME_NAME = "test_files"
    BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

    @classmethod
    def setup_class(cls):
        """Executado uma vez antes de todos os testes."""
        print("--- CONFIGURANDO AMBIENTE DE TESTE ---")
        cls.spark = SparkSession.builder.appName("FrameworkTestSuite").getOrCreate()
        
        cls.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cls.CATALOG}.{cls.SCHEMA}")
        cls.spark.sql(f"CREATE VOLUME IF NOT EXISTS {cls.CATALOG}.{cls.SCHEMA}.{cls.VOLUME_NAME}")

        print(f"Ambiente de teste configurado em: {cls.CATALOG}.{cls.SCHEMA}")
        dbutils.fs.rm(cls.BASE_PATH, recurse=True)
        os.makedirs(f"/dbfs{cls.BASE_PATH}/pipelines", exist_ok=True)
        os.makedirs(f"/dbfs{cls.BASE_PATH}/source_data", exist_ok=True)

        def encrypt_udf(col): return sha2(col, 256)
        cls.spark.udf.register("encrypt_udf", encrypt_udf)

    @classmethod
    def teardown_class(cls):
        """Executado uma vez após todos os testes."""
        print("\n--- LIMPANDO AMBIENTE DE TESTE ---")
        tables_to_drop = [
            "customers_silver", "customer_domains_gold", "employees_scd2",
            "fail_test_table", "sales_silver", "fct_sales", "validation_logs",
            "expected_silver_products", "silver_products_output", "update_test_table"
        ]
        for table in tables_to_drop:
            cls.spark.sql(f"DROP TABLE IF EXISTS {cls.CATALOG}.{cls.SCHEMA}.{table}")
        
        dbutils.fs.rm(cls.BASE_PATH, recurse=True)
        print("Ambiente limpo.")

    def _create_yaml_file(self, config_dict, filename):
        path = f"/dbfs{self.BASE_PATH}/pipelines/{filename}"
        with open(path, 'w') as f: yaml.dump(config_dict, f, sort_keys=False)
        return f"{self.BASE_PATH}/pipelines/{filename}"

    def _create_source_csv(self, data, filename, columns):
        path = f"{self.BASE_PATH}/source_data/{filename}"
        df = self.spark.createDataFrame(data, columns)
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
        return dbutils.fs.ls(path)[-1].path.replace("dbfs:", "")

    def _run_pipeline_command(self, command, config_path):
        with open(f"/dbfs{config_path}", 'r') as f: config_dict = yaml.safe_load(f)
        config = PipelineConfig(**config_dict)
        engine = SparkEngine(self.spark)
        pipeline = Pipeline(config, engine)
        if command == 'create': pipeline.create()
        elif command == 'run': pipeline.run()
        elif command == 'test': pipeline.test()
        elif command == 'update': pipeline.update()

    # --- Casos de Teste ---
    def test_01_silver_pipeline_with_validations(self):
        print("\n--- TESTE 1: Silver com Validações, Derivações e UDF ---")
        source_data = [
            ("101", "John", "Doe", "12345678900", "john.doe@test.com", "ACTIVE", "30"),
            (None, "No", "ID", "000", "noid@test.com", "ACTIVE", "40"), # Deve ser dropado (id nulo)
            ("103", "Bad", "Email", "111", "bademail", "ACTIVE", "22"), # Deve gerar warning
        ]
        source_path = self._create_source_csv(source_data, "customers_source.csv", ["customer_id", "first_name", "last_name", "document", "email", "status", "age"])
        validation_log_table = f"{self.CATALOG}.{self.SCHEMA}.validation_logs"
        
        config = {
            'pipeline_name': 'silver_dim_customers', 'pipeline_type': 'silver',
            'description': 'Dimensão de Clientes com dados limpos.',
            'validation_log_table': validation_log_table,
            'source': {'format': 'csv', 'path': source_path, 'options': {'header': 'true'}},
            'sink': {'catalog': self.CATALOG, 'schema': self.SCHEMA, 'table': 'customers_silver', 'mode': 'overwrite'},
            'columns': [
                {'name': 'customer_id', 'rename': 'customer_bk', 'type': 'string', 'pk': True, 'description': 'Business Key do cliente.', 'validate': [{'rule': 'not_null', 'on_fail': 'drop'}]},
                {'name': 'first_name', 'rename': 'first_name', 'type': 'string'},
                {'name': 'last_name', 'rename': 'last_name', 'type': 'string'},
                {'rename': 'full_name', 'type': 'string', 'is_derived': True, 'transform': "concat(first_name, ' ', last_name)", 'description': 'Nome completo derivado.'},
                {'name': 'document', 'rename': 'document_encrypted', 'type': 'string', 'udf': 'encrypt_udf', 'description': 'Documento criptografado.'},
                {'name': 'email', 'type': 'string', 'validate': [{'rule': 'pattern:^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$', 'on_fail': 'warn'}]}
            ]
        }
        config_path = self._create_yaml_file(config, "dim_customers.yaml")
        self._run_pipeline_command('create', config_path)
        self._run_pipeline_command('run', config_path)

        result_df = self.spark.read.table(f"{self.CATALOG}.{self.SCHEMA}.customers_silver")
        assert result_df.count() == 2
        log_df = self.spark.read.table(validation_log_table)
        assert log_df.count() == 1
        print("✅ TESTE 1 PASSOU")

    def test_02_scd2_dimension_pipeline(self):
        print("\n--- TESTE 2: dim_products (SCD Tipo 2) ---")
        config = {
            'pipeline_name': 'silver_dim_products_scd2', 'pipeline_type': 'silver',
            'sink': {'catalog': self.CATALOG, 'schema': self.SCHEMA, 'table': 'employees_scd2', 'mode': 'merge', 'scd': {'type': '2', 'track_columns': ['city']}},
            'columns': [{'name': 'employee_id', 'rename': 'employee_id', 'type': 'int', 'pk': True}, {'name': 'name', 'type': 'string'}, {'name': 'city', 'type': 'string'}]
        }
        source_data_1 = [("1", "Alice", "New York"), ("2", "Bob", "Los Angeles")]
        source_path_1 = self._create_source_csv(source_data_1, "scd_source_1.csv", ["employee_id", "name", "city"])
        config['source'] = {'format': 'csv', 'path': source_path_1, 'options': {'header': 'true'}}
        config_path = self._create_yaml_file(config, "scd_employees.yaml")
        self._run_pipeline_command('create', config_path)
        self._run_pipeline_command('run', config_path)

        source_data_2 = [("1", "Alice", "San Francisco"), ("2", "Bob", "Los Angeles"), ("3", "Carol", "Chicago")]
        source_path_2 = self._create_source_csv(source_data_2, "scd_source_2.csv", ["employee_id", "name", "city"])
        config['source']['path'] = source_path_2
        config_path = self._create_yaml_file(config, "scd_employees.yaml")
        self._run_pipeline_command('run', config_path)

        result_df = self.spark.read.table(f"{self.CATALOG}.{self.SCHEMA}.employees_scd2")
        assert result_df.count() == 4
        assert result_df.filter("is_current = true").count() == 3
        print("✅ TESTE 2 PASSOU")

    def test_03_fact_table_pipeline(self):
        print("\n--- TESTE 3: fct_sales (Gold com Lookups) ---")
        source_data = [("2024-01-15", "101", "10", "2"), ("2024-01-16", "102", "20", "5")]
        source_path = self._create_source_csv(source_data, "sales_source.csv", ["sale_date", "customer_id", "product_id", "quantity"])
        
        self.spark.read.format("csv").option("header", "true").load(source_path).createOrReplaceTempView("sales_silver_temp_view")
        self.spark.sql(f"CREATE OR REPLACE TABLE {self.CATALOG}.{self.SCHEMA}.sales_silver AS SELECT * FROM sales_silver_temp_view")

        config = {
            'pipeline_name': 'gold_fct_sales', 'pipeline_type': 'gold',
            'dependencies': [f"{self.CATALOG}.{self.SCHEMA}.dim_customers", f"{self.CATALOG}.{self.SCHEMA}.employees_scd2", f"{self.CATALOG}.{self.SCHEMA}.sales_silver"],
            'sink': {'catalog': self.CATALOG, 'schema': self.SCHEMA, 'table': 'fct_sales', 'mode': 'append'},
            'columns': [{'rename': 'customer_sk', 'type': 'long'}, {'rename': 'product_sk', 'type': 'long'}, {'rename': 'sale_date', 'type': 'date'}, {'rename': 'quantity', 'type': 'int'}],
            'transformation': [{'name': 'join_and_select', 'type': 'sql', 'sql': f"""
                SELECT c.id AS customer_sk, p.id AS product_sk, s.sale_date, s.quantity
                FROM sales_silver s
                JOIN dim_customers c ON s.customer_id = c.customer_bk
                JOIN employees_scd2 p ON s.product_id = p.employee_id AND p.is_current = true
            """}]
        }
        config_path = self._create_yaml_file(config, "fct_sales.yaml")
        self._run_pipeline_command('create', config_path)
        self._run_pipeline_command('run', config_path)

        result_df = self.spark.read.table(f"{self.CATALOG}.{self.SCHEMA}.fct_sales")
        assert result_df.count() == 2
        print("✅ TESTE 3 PASSOU")
    
    def test_04_validation_fail_action(self):
        print("\n--- TESTE 4: Validação com 'fail' ---")
        source_data = [("1", "User A"), ("1", "User B")]
        source_path = self._create_source_csv(source_data, "fail_source.csv", ["id", "name"])
        config = {
            'pipeline_name': 'silver_fail_test', 'pipeline_type': 'silver',
            'source': {'format': 'csv', 'path': source_path, 'options': {'header': 'true'}},
            'sink': {'catalog': self.CATALOG, 'schema': self.SCHEMA, 'table': 'fail_test_table', 'mode': 'overwrite'},
            'columns': [{'name': 'id', 'rename': 'id', 'type': 'int'}, {'name': 'name', 'rename': 'name', 'type': 'string'}],
            'table_validations': [{'type': 'duplicate_check', 'columns': ['id'], 'on_fail': 'fail'}]
        }
        config_path = self._create_yaml_file(config, "fail_test.yaml")
        try:
            self._run_pipeline_command('create', config_path)
            self._run_pipeline_command('run', config_path)
            raise AssertionError("A exceção de validação não foi lançada.")
        except Exception as e:
            assert "Verificação de duplicatas falhou" in str(e)
            print("✅ TESTE 4 PASSOU")

    def test_05_pipeline_test_command(self):
        print("\n--- TESTE 5: Funcionalidade 'test' da Engine ---")
        expected_data = [("PROD-A", "Product A", "Eletronicos"), ("PROD-B", "Product B", "Casa")]
        expected_df = self.spark.createDataFrame(expected_data, ["product_bk", "product_name", "category"])
        expected_df_final = expected_df.withColumn("hash_key", sha2(concat_ws("||", col("product_bk")), 256))
        expected_table_name = f"{self.CATALOG}.{self.SCHEMA}.expected_silver_products"
        expected_df_final.write.mode("overwrite").saveAsTable(expected_table_name)

        source_data = [("PROD-A", "Product A", "eletronicos"), ("PROD-B", "Product B", "CASA")]
        source_path = self._create_source_csv(source_data, "test_products_source.csv", ["product_bk", "product_name", "category"])

        config = {
            'pipeline_name': 'silver_products_testable', 'pipeline_type': 'silver',
            'test': {'source_data_path': source_path, 'expected_results_table': expected_table_name},
            'source': {'format': 'csv', 'path': '/path/to/production/data', 'options': {'header': 'true'}},
            'sink': {'catalog': self.CATALOG, 'schema': self.SCHEMA, 'table': 'silver_products_output', 'mode': 'overwrite'},
            'columns': [
                {'name': 'product_bk', 'rename': 'product_bk', 'type': 'string', 'pk': True},
                {'name': 'product_name', 'rename': 'product_name', 'type': 'string'},
                {'name': 'category', 'rename': 'category', 'type': 'string', 'transform': 'INITCAP(category)'}
            ]
        }
        config_path = self._create_yaml_file(config, "silver_products_testable.yaml")
        self._run_pipeline_command('test', config_path)
        print("✅ TESTE 5 PASSOU")
        
    def test_06_update_command(self):
        print("\n--- TESTE 6: Funcionalidade 'update' da Engine ---")
        source_data_v1 = [("1", "notebook"), ("2", "monitor")]
        source_path_v1 = self._create_source_csv(source_data_v1, "update_source_v1.csv", ["item_id", "item_name"])
        
        config_v1 = {
            'pipeline_name': 'silver_update_test', 'pipeline_type': 'silver',
            'source': {'format': 'csv', 'path': source_path_v1, 'options': {'header': 'true'}},
            'sink': {'catalog': self.CATALOG, 'schema': self.SCHEMA, 'table': 'update_test_table', 'mode': 'overwrite'},
            'columns': [
                {'name': 'item_id', 'rename': 'item_id', 'type': 'string', 'pk': True},
                {'name': 'item_name', 'rename': 'item_name', 'type': 'string'}
            ]
        }
        config_path_v1 = self._create_yaml_file(config_v1, "update_test_v1.yaml")
        self._run_pipeline_command('create', config_path_v1)
        self._run_pipeline_command('run', config_path_v1)
        df_v1 = self.spark.read.table(f"{self.CATALOG}.{self.SCHEMA}.update_test_table")
        hash_v1 = df_v1.filter("item_id = '1'").select("hash_key").first()[0]

        config_v2 = {
            'pipeline_name': 'silver_update_test', 'pipeline_type': 'silver',
            'source': {'format': 'csv', 'path': source_path_v1, 'options': {'header': 'true'}},
            'sink': {'catalog': self.CATALOG, 'schema': self.SCHEMA, 'table': 'update_test_table', 'mode': 'overwrite'},
            'columns': [
                {'name': 'item_id', 'rename': 'item_id', 'type': 'string'},
                {'name': 'item_name', 'rename': 'item_name', 'type': 'string', 'pk': True},
                {'rename': 'category', 'type': 'string', 'is_derived': True, 'transform': "CASE WHEN item_name = 'notebook' THEN 'Eletronicos' ELSE 'Perifericos' END"}
            ]
        }
        config_path_v2 = self._create_yaml_file(config_v2, "update_test_v2.yaml")
        self._run_pipeline_command('update', config_path_v2)

        df_v2 = self.spark.read.table(f"{self.CATALOG}.{self.SCHEMA}.update_test_table")
        hash_v2 = df_v2.filter("item_id = '1'").select("hash_key").first()[0]
        
        assert 'category' in df_v2.columns
        assert hash_v1 != hash_v2
        print("✅ TESTE 6 PASSOU")
    
    def test_07_reserved_column_name_fails(self):
        print("\n--- TESTE 7: Validação de Nomes de Coluna Reservados ---")
        config = {
            'pipeline_name': 'silver_reserved_name_test', 'pipeline_type': 'silver',
            'source': {'format': 'csv', 'path': '/path/to/dummy.csv', 'options': {'header': 'true'}},
            'sink': {'catalog': self.CATALOG, 'schema': self.SCHEMA, 'table': 'reserved_name_table', 'mode': 'overwrite'},
            'columns': [
                {'name': 'some_id', 'rename': 'id', 'type': 'string'} # Tenta usar o nome reservado 'id'
            ]
        }
        config_path = self._create_yaml_file(config, "reserved_name_test.yaml")
        
        try:
            # A falha deve ocorrer já na validação do Pydantic
            with open(f"/dbfs{config_path}", 'r') as f: config_dict = yaml.safe_load(f)
            PipelineConfig(**config_dict)
            raise AssertionError("A validação Pydantic para nomes reservados não falhou como esperado.")
        except ValidationError as e:
            assert "é reservado pelo framework" in str(e)
            print("✅ TESTE 7 PASSOU: Framework impediu o uso de nome de coluna reservado.")
        except Exception as e:
            print(f"❌ TESTE 7 FALHOU: Uma exceção inesperada foi lançada: {e}")
            raise

# COMMAND ----------

# DBTITLE 5,Execução da Suíte de Testes
test_suite = TestFramework()
try:
    test_suite.setup_class()
    test_suite.test_01_silver_pipeline_with_validations()
    test_suite.test_02_scd2_dimension_pipeline()
    test_suite.test_03_fact_table_pipeline()
    test_suite.test_04_validation_fail_action()
    test_suite.test_05_pipeline_test_command()
    test_suite.test_06_update_command()
    test_suite.test_07_reserved_column_name_fails()
    print("\n🎉 Todos os testes passaram com sucesso! 🎉")
except Exception as e:
    print("\n🔥 Um ou mais testes falharam. 🔥")
    raise e
finally:
    test_suite.teardown_class()
