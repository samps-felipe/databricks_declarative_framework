# Databricks notebook source
# DBTITLE 1,Importações Necessárias para o Debug
# Use imports ABSOLUTOS a partir da raiz do seu repositório.
# Ajuste o caminho se sua estrutura for diferente.
from src.framework.steps_handler import handle_silver_transformation
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# DBTITLE 2,Crie Dados de Teste (Mock Data)
# Crie um DataFrame Spark "fake" para simular os dados de entrada.
# Isso permite que você teste a lógica da sua função sem depender de arquivos reais.

schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", StringType(), True), # Comece como string para testar o casting
    StructField("open_date", StringType(), True)
])

data = [
    (101, 1, " corrente ", "1500.50", "2023-01-15"),
    (102, 2, "Poupanca", "950.00", "2023-02-20"),
    (103, 1, "investimento", "12345.67", "2023-03-10"), # Tipo de conta inválido
    (None, 3, "corrente", "500.00", "2023-04-05"), # ID nulo
    (105, 4, "corrente", "-50.00", "2023-05-12") # Saldo negativo
]

# Cria o DataFrame de entrada para o teste
df_entrada = spark.createDataFrame(data, schema)

print("DataFrame de entrada para o teste:")
display(df_entrada)

# COMMAND ----------

# DBTITLE 3,Simule a Configuração YAML
# Em vez de ler o arquivo YAML, crie um dicionário Python que imita a sua configuração.
# Isso torna o teste mais rápido e isolado.

config_colunas_teste = [
    {
        "name": "account_id",
        "rename": "id_conta",
        "type": "long",
        "validate": ["not_null"]
    },
    {
        "name": "account_type",
        "rename": "tipo_conta",
        "type": "string",
        "transform": "UPPER(TRIM(account_type))",
        "validate": ["isin:['CORRENTE', 'POUPANCA']"]
    },
    {
        "name": "balance",
        "rename": "saldo",
        "type": "decimal(18, 2)",
        "validate": ["greater_than_or_equal_to:0"]
    },
    {
        "name": "open_date",
        "rename": "data_abertura",
        "type": "date",
        "format": "yyyy-MM-dd"
    }
]

# COMMAND ----------

# DBTITLE 4,Execute e Depure a Função
# Agora, chame a função que você quer testar com os dados e a configuração de teste.

# Adicione prints dentro da sua função em `steps_handler.py` para ver o passo a passo.
# Se ocorrer um erro, ele será exibido aqui de forma clara.

try:
    print("\nExecutando a função handle_silver_transformation...")
    df_saida = handle_silver_transformation(df_entrada, config_colunas_teste)
    
    print("\nResultado final da transformação:")
    # O comando display() é ótimo para visualizar o resultado no Databricks
    display(df_saida)

except Exception as e:
    print(f"\nOcorreu um erro durante a execução: {e}")
    raise
