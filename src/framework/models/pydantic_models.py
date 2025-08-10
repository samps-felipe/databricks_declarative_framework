from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field

class ValidationRule(BaseModel):
    rule: str
    on_fail: Literal['fail', 'drop', 'warn'] = 'fail'

class ColumnSpec(BaseModel):
    name: str
    rename: Optional[str] = None
    type: str
    description: Optional[str] = None
    pk: bool = False
    transform: Optional[str] = None
    validate: List[ValidationRule] = []
    format: Optional[str] = None
    try_cast: bool = False

class TableValidation(BaseModel):
    type: Literal['duplicate_check']
    columns: List[str]
    on_fail: Literal['fail', 'warn'] = 'fail'

class Defaults(BaseModel):
    date_format: Optional[str] = None
    column_rename_pattern: Literal['snake_case', 'none'] = 'none'

class SourceConfig(BaseModel):
    format: str
    path: str
    options: Dict[str, Any] = {}
    expected_columns: Optional[int] = None

class SCDConfig(BaseModel):
    type: Literal['2'] = '2'  # Tipo de SCD, atualmente apenas SCD Tipo 2 é suportado
    track_columns: List[str] = Field(..., description="Colunas a serem monitoradas para mudanças. Mudanças nessas colunas criarão uma nova versão do registro.")

class SinkConfig(BaseModel):
    catalog: str
    schema_name: str = Field(..., alias='schema')
    table: str
    mode: Literal['append', 'overwrite', 'merge', 'overwrite_partition', 'overwrite_where']
    partition_by: Optional[List[str]] = None
    zorder_by: Optional[List[str]] = None
    overwrite_condition: Optional[str] = None
    scd: Optional[SCDConfig] = None # <-- Adicionada a configuração de SCD

class TestConfig(BaseModel):
    source_data_path: str = Field(..., description="Caminho para os dados de entrada usados apenas no teste.")
    expected_results_table: str = Field(..., description="Nome completo da tabela que contém os resultados esperados.")

class PipelineConfig(BaseModel):
    engine: Literal['spark', 'pandas', 'polars'] = 'spark'
    pipeline_type: Literal['silver', 'gold']
    pipeline_name: str
    description: Optional[str] = None
    dependencies: Optional[List[str]] = None
    defaults: Defaults = Field(default_factory=Defaults)
    source: Optional[SourceConfig] = None
    sink: SinkConfig
    columns: List[ColumnSpec]
    table_validations: List[TableValidation] = []
    validation_log_table: Optional[str] = None
    test: Optional[TestConfig] = None # <-- Adicionada a configuração de teste opcional
