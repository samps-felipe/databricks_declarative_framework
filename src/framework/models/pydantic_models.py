from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field, field_validator

class ValidationRule(BaseModel):
    rule: str
    on_fail: Literal['fail', 'drop', 'warn'] = 'fail'

class ColumnSpec(BaseModel):
    name: Optional[str] = None
    rename: str
    type: str
    is_derived: bool = False
    optional: bool = False
    udf: Optional[str] = None
    description: Optional[str] = None
    pk: bool = False
    transform: Optional[str] = None
    validation_rules: List[ValidationRule] = Field([], alias='validate')
    format: Optional[str] = None
    try_cast: bool = False

    @field_validator('name', pre=True, always=True)
    def check_name_required_if_not_derived(cls, v, values):
        if not values.get('is_derived') and v is None:
            raise ValueError("'name' é obrigatório para colunas que não são derivadas.")
        return v
    
    @field_validator('rename')
    def check_for_reserved_column_names(cls, v):
        if v.lower() in RESERVED_COLUMN_NAMES:
            raise ValueError(
                f"O nome de coluna '{v}' é reservado pelo framework. "
                f"Por favor, use um nome diferente. Nomes reservados: {RESERVED_COLUMN_NAMES}"
            )
        return v

class ForeignKey(BaseModel):
    name: str
    local_columns: List[str]
    references_table: str
    references_columns: List[str]

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
    scd: Optional[SCDConfig] = None
    foreign_keys: List[ForeignKey] = []

class TestConfig(BaseModel):
    source_data_path: str = Field(..., description="Caminho para os dados de entrada usados apenas no teste.")
    expected_results_table: str = Field(..., description="Nome completo da tabela que contém os resultados esperados.")

class TransformationConfig(BaseModel):
    name: str = Field(..., description="Nome do passo de transformação. Será usado para criar uma view temporária com o resultado.")
    type: Literal['sql']
    sql: str

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
    transformation: Optional[List[TransformationConfig]] = None # Alterado para uma lista
    table_validations: List[TableValidation] = []
    validation_log_table: Optional[str] = None
    test: Optional[TestConfig] = None
    custom_transform_script: Optional[str] = None
