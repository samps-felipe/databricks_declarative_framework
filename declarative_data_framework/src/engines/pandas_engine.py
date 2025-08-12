import pandas as pd
from ..core.engine import BaseEngine
from ..models.pydantic_models import PipelineConfig

class PandasEngine(BaseEngine):
    def __init__(self, config: PipelineConfig):
        self.config = config


    def read(self, config: PipelineConfig):
        source_config = config.source
        if not source_config:
            raise ValueError("'source' configuration not found for a pipeline that requires file reading.")
        # Example: CSV reading, extend for other formats as needed
        return pd.read_csv(source_config.path, **source_config.options)

    def process(self, df, config: PipelineConfig):
        # Implement processing logic using pipeline steps (transformations, etc.)
        # Placeholder: return df unchanged
        return df

    def validate(self, df, config: PipelineConfig):
        # Implement validation logic using pandas
        # Placeholder: return df unchanged and None for validation log
        return df, None

    def write(self, df, config: PipelineConfig, validation_log_df=None):
        sink_config = config.sink
        # Example: CSV writing, extend for other formats as needed
        df.to_csv(sink_config.path, index=False)
        if validation_log_df is not None and config.validation_log_table:
            validation_log_df.to_csv(config.validation_log_table, index=False)

    def create_table(self, config: PipelineConfig):
        # Pandas does not create tables, but you could implement schema creation logic for databases
        pass

    def update_table(self, config: PipelineConfig):
        # Pandas does not update tables, but you could implement schema update logic for databases
        pass

    def read_table(self, table_name: str):
        # Example: read a table from a database or file
        # Placeholder: not implemented
        pass

    def compare_dataframes(self, df_actual, df_expected) -> bool:
        # Compare two pandas DataFrames
        return df_actual.equals(df_expected)

    def show_differences(self, df_actual, df_expected):
        # Show differences between two pandas DataFrames
        print("Rows in actual but not in expected:")
        print(df_actual[~df_actual.apply(tuple,1).isin(df_expected.apply(tuple,1))])
        print("Rows in expected but not in actual:")
        print(df_expected[~df_expected.apply(tuple,1).isin(df_actual.apply(tuple,1))])

    def execute_gold_transformation(self, config: PipelineConfig):
        # Implement transformation logic for Gold pipelines using pandas
        pass

    def write(self, df: pd.DataFrame, config: PipelineConfig, validation_log_df: pd.DataFrame = None):
        sink_config = config.sink
        # Example: CSV writing, extend for other formats as needed
        df.to_csv(sink_config.path, index=False)
        if validation_log_df is not None and config.validation_log_table:
            validation_log_df.to_csv(config.validation_log_table, index=False)

    def validate(self, df: pd.DataFrame, config: PipelineConfig):
        # Implement validation logic using pandas
        # This should call the validation steps as in SparkEngine
        pass

    def run(self, config: PipelineConfig):
        # Implement the main pipeline logic using pandas
        pass
