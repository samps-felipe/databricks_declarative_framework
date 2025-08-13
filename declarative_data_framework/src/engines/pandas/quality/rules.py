import pandas as pd
from .....core.quality import BaseValidation, register_rule

@register_rule('not_null')
class NotNullValidation(BaseValidation):
    def apply(self, df: pd.DataFrame, column_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        success_df = df[df[column_name].notna()]
        failures_df = df[df[column_name].isna()]
        return failures_df, success_df

@register_rule('isin')
class IsInValidation(BaseValidation):
    def __init__(self, params: dict):
        super().__init__(params)
        self.allowed_values = self.params.get('allowed_values_str', '').split(',')

    def apply(self, df: pd.DataFrame, column_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        success_df = df[df[column_name].isin(self.allowed_values)]
        failures_df = df[~df[column_name].isin(self.allowed_values)]
        return failures_df, success_df
