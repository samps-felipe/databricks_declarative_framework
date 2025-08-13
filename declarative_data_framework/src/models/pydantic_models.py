from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field, validator, root_validator

# Define reserved column names used by the framework
RESERVED_COLUMN_NAMES = {
    "id", "hash_key", "data_hash", "created_at", "updated_at",
    "is_current", "start_date", "end_date"
}

class ValidationRule(BaseModel):
    """Defines a validation rule to be applied to a column."""
    rule: str
    on_fail: Literal['fail', 'drop', 'warn'] = 'fail'

class ColumnSpec(BaseModel):
    """Specification for a single column in a pipeline."""
    name: Optional[str] = Field(None, description="Original column name in the source.")
    rename: Optional[str] = Field(None, description="New name for the column after processing.")
    description: Optional[str] = None
    type: str
    optional: bool = Field(False, description="If True, the source column can be missing.")
    pk: bool = Field(False, description="Indicates if the column is part of the primary key.")
    transform: Optional[str] = Field(None, description="SQL expression to transform the column.")
    validation_rules: List[ValidationRule] = Field([], alias='validate')
    format: Optional[str] = None
    try_cast: bool = False

    @root_validator(pre=True)
    def set_name_or_rename(cls, values):
        """Ensures that either 'name' or 'rename' is present."""
        if values.get('name') is None and values.get('rename') is None:
            raise ValueError("Each column must have either a 'name' or 'rename' field.")
        
        if values.get('name') is None:
            values['name'] = values.get('rename')
        elif values.get('rename') is None:
            values['rename'] = values.get('name')

        return values

    @root_validator(pre=False)
    def check_reserved_names(cls, values):
        """Validates that the final column name is not a reserved name."""
        final_name = values.get('rename') or values.get('name')
        if final_name.lower() in RESERVED_COLUMN_NAMES:
            raise ValueError(
                f"The column name '{final_name}' is reserved by the framework. "
                f"Reserved names are: {RESERVED_COLUMN_NAMES}"
            )
        return values

class ForeignKey(BaseModel):
    """Defines a foreign key constraint."""
    name: str
    local_columns: List[str]
    references_table: str
    references_columns: List[str]

class TableValidation(BaseModel):
    """Defines a table-level validation."""
    type: Literal['duplicate_check']
    columns: List[str]
    on_fail: Literal['fail', 'warn'] = 'fail'

class Defaults(BaseModel):
    """Defines default behaviors for the pipeline."""
    date_format: Optional[str] = None
    column_rename_pattern: Literal['snake_case', 'none'] = 'none'

class SourceConfig(BaseModel):
    """Configuration for the data source."""
    format: str
    path: Optional[str] = None
    table: Optional[str] = None
    query: Optional[str] = None
    options: Dict[str, Any] = {}
    expected_columns: Optional[int] = None

class SCDConfig(BaseModel):
    """Configuration for Slowly Changing Dimension (SCD)."""
    type: Literal['2'] = '2'
    track_columns: Optional[List[str]] = Field(None, description="Columns to track for changes. If not provided, all non-PK columns will be tracked.")

class SinkConfig(BaseModel):
    """Configuration for the data sink."""
    format: str = Field(..., description="Output format, e.g., 'delta', 'parquet'.")
    path: Optional[str] = None
    table: Optional[str] = None
    mode: Literal['append', 'overwrite', 'merge', 'overwrite_partition', 'overwrite_where']
    overwrite_condition: Optional[str] = None
    partition_by: Optional[List[str]] = None
    zorder_by: Optional[List[str]] = None
    scd: Optional[SCDConfig] = None

class TestConfig(BaseModel):
    """Configuration for pipeline testing."""
    source_data: SourceConfig = Field(..., description="Path to the input data for the test.")
    expected_results_data: SourceConfig = Field(..., description="Full name of the table containing the expected results.")

class TransformationConfig(BaseModel):
    """Configuration for a single transformation step."""
    name: str = Field(..., description="Name of the transformation step. Used to create a temporary view with the result.")
    type: Literal['sql']
    sql: str

class StepConfig(BaseModel):
    """Configuration for a generic pipeline step."""
    name: str
    type: str = Field(..., description="Type of the step, e.g., 'read', 'transform', 'validate', 'write'.")
    params: Dict[str, Any] = {}

class PipelineConfig(BaseModel):
    """Main configuration model for a pipeline."""
    engine: str
    pipeline_type: Literal['silver', 'gold'] = Field('silver', description="Type of the pipeline.")
    pipeline_name: str
    description: Optional[str] = None
    dependencies: Optional[List[str]] = Field([], description="List of tables this pipeline depends on.")
    defaults: Defaults = Field(default_factory=Defaults)
    source: Optional[SourceConfig] = None
    sink: SinkConfig
    columns: List[ColumnSpec] = []
    transformation: Optional[List[TransformationConfig]] = None
    steps: Optional[List[StepConfig]] = None
    table_validations: List[TableValidation] = []
    validation_log_table: Optional[str] = None
    test: Optional[TestConfig] = None
    foreign_keys: List[ForeignKey] = []
    custom_transform_script: Optional[str] = None

    @root_validator
    def validate_pipeline_type(cls, values):
        """Validates configuration based on the pipeline type."""
        pipeline_type = values.get('pipeline_type')
        if pipeline_type == 'silver':
            if not values.get('source'):
                raise ValueError("'source' is required for silver pipelines.")
            if not values.get('columns'):
                raise ValueError("'columns' are required for silver pipelines.")
        elif pipeline_type == 'gold':
            if not values.get('transformation'):
                raise ValueError("'transformation' is required for gold pipelines.")
            if not values.get('dependencies'):
                raise ValueError("'dependencies' are required for gold pipelines.")
        return values
