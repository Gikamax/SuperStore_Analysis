from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import pandas as pd

from src.data.loader import Dataschema

@dataclass
class DataSource:
    _data: pd.DataFrame

    @property
    def row_count(self) -> int:
        return self._data.shape[0]
    
    @property
    def all_months(self) -> list[str]:
        return self._data[Dataschema.ORDER_MONTH].tolist()
    
    @property
    def all_categories(self) -> list[str]:
        return self._data[Dataschema.CATEGORY].tolist()
    
    @property
    def unique_months(self) -> list[str]:
        return sorted(set(self.all_months))
    
    @property
    def unique_categories(self) -> list[str]:
        return sorted(set(self.all_categories))
    
    def filter(self, categories: Optional[list[str]] = None, months: Optional[list[str]] = None) -> DataSource:
        if categories is None:
            categories = self.unique_categories
        if months is None:
            months = self.unique_months
        filtered_data = self._data[self._data[Dataschema.ORDER_MONTH].isin(months) & self._data[Dataschema.CATEGORY].isin(categories)]
        return DataSource(filtered_data)
    
    def prepare_data_for_barchart(self) -> pd.DataFrame:
        return self._data.groupby([Dataschema.ORDER_MONTH, Dataschema.CATEGORY]).agg({Dataschema.SALES: "sum"}).reset_index()
