# Framework Declarativo para Pipelines de Dados no Databricks

1. Visão Geral

Este framework foi projetado para acelerar e padronizar o desenvolvimento de pipelines de dados (ETL/ELT) no Databricks. A abordagem é declarativa: em vez de escrever código PySpark ou SQL repetitivo para cada pipeline, você declara as fontes, destinos, transformações e regras de qualidade em arquivos de configuração YAML.

O motor do framework lê esses arquivos e executa as operações correspondentes, garantindo uniformidade, qualidade e otimizando o tempo de desenvolvimento.

Principais Características
Configuração via YAML: Defina pipelines de forma simples e legível.

Suporte a Múltiplos Motores: Escolha entre executar pipelines com Spark (via script PySpark) ou gerar código otimizado para Delta Live Tables (DLT).

Padrão de Camadas (Medallion): Modelos de configuração direcionados para as camadas Silver (limpeza e qualidade) e Gold (agregação e regras de negócio).

Qualidade de Dados Embutida: Defina validações (constraints) diretamente nas colunas, que são aplicadas automaticamente.

Extensível: Fácil de adicionar novas transformações e regras de validação.

2. Estrutura de Diretórios
O projeto é organizado da seguinte forma para manter o código e as configurações separadas e de fácil manutenção.

/
├── 📂 pipelines/             # Arquivos de configuração YAML para cada pipeline.
│   ├── silver_contas.yaml
│   └── gold_clientes_enriquecidos.yaml
│
├── 📂 src/
│   └── 📂 framework/         # O código-fonte do motor do framework.
│       ├── engine.py         # Motor principal que orquestra a execução.
│       ├── steps_handler.py  # Lógica de transformação para o motor Spark.
│       ├── quality.py        # Lógica de validação para o motor Spark.
│       └── dlt_generator.py  # Gerador de scripts SQL para DLT.
│
├── 📂 dlt_generated/         # (Criado automaticamente) Scripts SQL gerados para DLT.
│
├── 📓 Run_Pipeline.py        # Notebook Databricks para executar o framework.
└── 📝 requirements.txt      # Dependências Python.

3. Como Usar
O fluxo de trabalho consiste em duas etapas principais: configurar o pipeline no YAML e executá-lo através do notebook.

Etapa 1: Configurar o Pipeline (YAML)
Crie um novo arquivo .yaml na pasta /pipelines. A estrutura do YAML depende da camada (Silver ou Gold) e do motor de execução escolhido (spark ou dlt).

Exemplo: Pipeline Silver
Ideal para ingestão, limpeza, tipagem, padronização e validação dos dados brutos.

# pipelines/silver_contas.yaml
engine: "spark" # ou "dlt"
pipeline_type: "silver"
pipeline_name: "silver_contas"
description: "Ingere dados de contas, aplica qualidade e padronização."

source:
  format: "cloudFiles"
  path: "/mnt/raw/contas/contas_*.csv"
  options:
    cloudFiles.format: "csv"
    header: "true"

sink:
  catalog: "dev"
  schema: "silver"
  table: "contas"
  mode: "append"

columns:
  - name: "account_id"          # Nome original da coluna na fonte
    rename: "id_conta"           # Novo nome da coluna
    type: "long"                 # Tipo de dado final
    description: "ID da conta."
    pk: true                     # Informativo: indica chave primária
    validate:                    # Regras de qualidade
      - "not_null"

  - name: "account_type"
    rename: "tipo_conta"
    type: "string"
    transform: "UPPER(TRIM(account_type))" # Expressão SQL para transformar o valor
    validate:
      - "isin:['CORRENTE', 'POUPANCA']"

  - name: "balance"
    rename: "saldo"
    type: "decimal(18, 2)"
    validate:
      - "greater_than_or_equal_to:0"

Exemplo: Pipeline Gold
Ideal para criar tabelas agregadas e visões de negócio, juntando dados de uma ou mais tabelas da camada Silver.

# pipelines/gold_clientes_enriquecidos.yaml
engine: "dlt" # ou "spark"
pipeline_type: "gold"
pipeline_name: "gold_clientes_enriquecidos"
description: "Cria uma visão 360 de clientes com dados de contas."

dependencies: # Tabelas da camada Silver necessárias
  - "dev.silver.clientes"
  - "dev.silver.contas"

sink:
  catalog: "dev"
  schema: "gold"
  table: "clientes_enriquecidos"
  mode: "overwrite"

transformation:
  type: "sql" # Único tipo suportado para Gold no momento
  sql: |
    SELECT
      c.id_cliente,
      c.nome_completo,
      COUNT(a.id_conta) AS quantidade_contas,
      SUM(a.saldo) AS saldo_total_consolidado
    FROM dev.silver.clientes c
    LEFT JOIN dev.silver.contas a ON c.id_cliente = a.id_cliente
    GROUP BY c.id_cliente, c.nome_completo

Etapa 2: Executar o Pipeline
Use o notebook Run_Pipeline.py para iniciar a execução.

Abra o notebook no Databricks.

No widget pipeline_config na parte superior, insira o caminho relativo para o seu arquivo de configuração (ex: pipelines/silver_contas.yaml).

Execute o notebook.

Comportamento por Motor
engine: "spark": O notebook executará o pipeline PySpark imediatamente. Os logs da execução aparecerão no output do notebook.

engine: "dlt": O notebook não executará o pipeline. Em vez disso, ele irá gerar um novo arquivo .sql na pasta /dlt_generated. O output do notebook informará o caminho para este arquivo e os próximos passos para configurar seu pipeline na interface do Delta Live Tables no Databricks.


4. Extensibilidade
O framework foi construído para ser extensível. Para adicionar novas funcionalidades:

## Custom Validation Rules
You can register or overwrite custom validation rules without modifying the library code. Use the `register_rule` function:

```python
from declarative_data_framework.quality import register_rule

class MyCustomValidation:
  def __init__(self, params: dict = None):
    # Your initialization logic
    pass
  def apply(self, df, column_name):
    # Your validation logic
    return failures_df, success_df

# Register your rule (overwrites if the name already exists)
register_rule("my_custom_rule", MyCustomValidation)
```

You can also use it as a decorator:

```python
from declarative_data_framework.quality import register_rule

@register_rule("my_custom_rule")
class MyCustomValidation:
  ...
```

After registration, use your rule in the pipeline YAML as usual.

Novas Validações (Spark): Adicione uma nova lógica condicional no arquivo src/framework/quality.py.

Novas Validações (DLT): Adicione um novo gerador de CONSTRAINT no arquivo src/framework/dlt_generator.py.

Novos Tipos de Transformação: Adicione novas funções no arquivo src/framework/steps_handler.py (para o motor Spark).