# Standard Imports
import sqlite3
from typing import Self

# Third Party Imports
import pandas as pd

# Project Imports
from pipeline_flow.plugins import (
    IExtractPlugin,
    ILoadPlugin,
    IMergeExtractPlugin,
    ITransformLoadPlugin,
    ITransformPlugin,
)


class SQLiteExtractor(IExtractPlugin, plugin_name="sqlite_extractor"):
    def __init__(self: Self, plugin_id: str, db_uri: str, table_name: str, columns: list[str]) -> None:
        super().__init__(plugin_id)
        self.db_uri = db_uri
        self.table_name = table_name
        self.columns = columns

    async def __call__(self: Self) -> pd.DataFrame:
        conn = sqlite3.connect(self.db_uri, uri=True)
        cursor = conn.cursor()

        cursor.execute(f"SELECT {','.join(self.columns)} FROM {self.table_name}")  # noqa: S608 - table names cannot be parametrized
        results = cursor.fetchall()

        data = {self.columns[index]: [row[index] for row in results] for index in range(len(self.columns))}
        return pd.DataFrame(data)


class SQLiteInnerMerger(IMergeExtractPlugin, plugin_name="sqlite_inner_merger"):
    def __init__(self: Self, plugin_id: str, on: str) -> None:
        super().__init__(plugin_id)
        self.on = on

    def __call__(self: Self, extracted_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        # For simplicity, we assume that the extracted_data dictionary contains only two DataFrames.
        df_ids = iter(extracted_data.keys())

        return extracted_data[next(df_ids)].merge(extracted_data[next(df_ids)], on=self.on)


class SQLiteLoader(ILoadPlugin, plugin_name="sqlite_loader"):
    def __init__(self: Self, plugin_id: str, db_uri: str, table: str) -> None:
        super().__init__(plugin_id)
        self.db_uri = db_uri
        self.table = table

    async def __call__(self: Self, data: pd.DataFrame) -> None:
        conn = sqlite3.connect(self.db_uri, uri=True)
        data.to_sql(self.table, conn, if_exists="replace", index=False)


class SQLiteTransformLoader(ITransformLoadPlugin, plugin_name="sqlite_transform_loader"):
    def __init__(self: Self, plugin_id: str, db_uri: str, query: str) -> None:
        super().__init__(plugin_id)
        self.db_uri = db_uri
        self.query = query

    def __call__(self: Self) -> None:
        conn = sqlite3.connect(self.db_uri, uri=True)
        cursor = conn.cursor()
        cursor.execute(self.query)
        conn.commit()


class ColumnDropper(ITransformPlugin, plugin_name="column_dropper"):
    def __init__(self: Self, plugin_id: str, columns: list[str]) -> None:
        super().__init__(plugin_id)
        self.columns = columns

    def __call__(self: Self, data: pd.DataFrame) -> pd.DataFrame:
        return data.drop(columns=self.columns, axis=1)


class CustomerOrderSummarizer(ITransformPlugin, plugin_name="customer_order_summarizer"):
    def __call__(self: Self, data: pd.DataFrame) -> pd.DataFrame:
        return (
            data.groupby(["customer_id", "name"])
            .agg(total_orders=("order_id", "count"), total_amount=("amount", "sum"))
            .reset_index()
        )
