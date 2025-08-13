from pyspark.sql import DataFrame
from ....core.step import BaseStep
from ....exceptions import ConfigurationError, StepExecutionError

class ReadStep(BaseStep):
    """Step responsible for reading data from the source."""
    def execute(self) -> DataFrame:
        self.logger.info("--- Step: Read ---")

        source_config = self.config.source
        pipeline_name = self.config.pipeline_name
        pipeline_type = self.config.pipeline_type

        if pipeline_type == 'gold':
            self.logger.info("Gold pipeline does not read from files. Skipping READ step.")
            return None

        try:
            # TODO: Implement logic to handle different source types (e.g., file, database)
            # TODO: Deixar mais generico
            if not source_config:
                raise ValueError("'source' configuration not found for a pipeline that requires file reading.")
            
            reader = self.engine.spark.read.format(source_config.format)
            reader.options(**source_config.options)

            df = reader.load(source_config.path)
            
        except Exception as e:
            self.logger.exception("Failure in READ step for pipeline '%s'", pipeline_name)
            raise StepExecutionError(f"Failure in READ step: {e}") from e
    
                
        # Validate column presence (to ensure the delimiter is correct)
        if source_config and source_config.expected_columns:
            num_actual_columns = len(df.columns)
            if num_actual_columns != source_config.expected_columns:
                delimiter = source_config.options.get('delimiter', '[NOT SPECIFIED]')
                msg = (
                    f"Schema validation failed! File was read with {num_actual_columns} columns, "
                    f"but {source_config.expected_columns} were expected. "
                    f"Please check if the delimiter ('{delimiter}') is correct."
                )
                self.logger.error(msg)
                raise ConfigurationError(msg)
        
        self.logger.info("Read and initial schema validation completed.")
        return df
