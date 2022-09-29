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
    def all_segments(self) -> list[str]:
        return self._data[Dataschema.CUSTOMER_SEGMENT].tolist()
    
    @property
    def unique_months(self) -> list[str]:
        return sorted(set(self.all_months))
    
    @property
    def unique_categories(self) -> list[str]:
        return sorted(set(self.all_categories))

    @property
    def unique_segments(self) -> list[str]:
        return sorted(set(self.all_segments))
    
    def filter(self, categories: Optional[list[str]] = None, months: Optional[list[str]] = None, segments: Optional[list[str]] = None) -> DataSource:
        if categories is None:
            categories = self.unique_categories
        if months is None:
            months = self.unique_months
        if segments is None:
            segments = self.unique_segments
        filtered_data = self._data[
            self._data[Dataschema.ORDER_MONTH].isin(months) & 
            self._data[Dataschema.CATEGORY].isin(categories) & 
            self._data[Dataschema.CUSTOMER_SEGMENT].isin(segments)]
        return DataSource(filtered_data)
    
    def prepare_data_for_category_barchart(self) -> pd.DataFrame:
        return self._data.groupby([Dataschema.CATEGORY, Dataschema.ORDER_MONTH]).agg({Dataschema.SALES: "sum"}).reset_index()
    
    def prepare_data_for_segment_barchart(self) -> pd.DataFrame:
        return self._data.groupby([Dataschema.CUSTOMER_SEGMENT, Dataschema.ORDER_MONTH]).agg({Dataschema.SALES: "sum"}).reset_index()
    
    def prepare_data_for_linechart(self) -> pd.DataFrame:
        return self._data.groupby(Dataschema.ORDER_DATE).agg({Dataschema.SALES: "sum", Dataschema.PROFIT: "sum"}).reset_index()
