# Standard Imports
import sqlite3
from functools import wraps

# Third Party Imports
import pandas as pd

# Project Imports
from common.type_def import AsyncPlugin, SyncPlugin
from core.models.phases import PipelinePhase
from core.plugins import plugin


@plugin(PipelinePhase.EXTRACT_PHASE, "sqlite_extractor")
def sqlite_extractor(db_uri: str, table_name: str, columns: list[str]) -> AsyncPlugin:
    @wraps(sqlite_extractor)
    async def inner() -> pd.DataFrame:
        conn = sqlite3.connect(db_uri, uri=True)
        cursor = conn.cursor()

        cursor.execute(f"SELECT {','.join(columns)} FROM {table_name}")  # noqa: S608 - table names cannot be parametrized
        results = cursor.fetchall()

        data = {columns[index]: [row[index] for row in results] for index in range(len(columns))}
        return pd.DataFrame(data)

    return inner


@plugin(PipelinePhase.EXTRACT_PHASE, "sqlite_inner_merger")
def sqlite_inner_merger(on: str) -> SyncPlugin:
    @wraps(sqlite_inner_merger)
    def inner(extracted_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        # For simplicity, we assume that the extracted_data dictionary contains only two DataFrames.
        df_ids = iter(extracted_data.keys())

        return extracted_data[next(df_ids)].merge(extracted_data[next(df_ids)], on=on)

    return inner


@plugin(PipelinePhase.LOAD_PHASE, "sqlite_loader")
def sqlite_loader(db_uri: str, table: str) -> AsyncPlugin:
    @wraps(sqlite_loader)
    async def inner(data: pd.DataFrame) -> None:
        conn = sqlite3.connect(db_uri, uri=True)
        data.to_sql(table, conn, if_exists="replace", index=False)

    return inner


@plugin(PipelinePhase.TRANSFORM_AT_LOAD_PHASE, "sqlite_transform_loader")
def sqlite_transform_loader(db_uri: str, query: str) -> SyncPlugin:
    @wraps(sqlite_transform_loader)
    def inner() -> None:
        conn = sqlite3.connect(db_uri, uri=True)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()

    return inner


@plugin(PipelinePhase.TRANSFORM_PHASE, "column_dropper")
def column_dropper(columns: list[str]) -> SyncPlugin:
    @wraps(column_dropper)
    def inner(data: pd.DataFrame) -> pd.DataFrame:
        return data.drop(columns=columns, axis=1)

    return inner


@plugin(PipelinePhase.TRANSFORM_PHASE, "customer_order_summarizer")
def customer_order_summarizer() -> SyncPlugin:
    @wraps(customer_order_summarizer)
    def inner(data: pd.DataFrame) -> pd.DataFrame:
        return (
            data.groupby(["customer_id", "name"])
            .agg(total_orders=("order_id", "count"), total_amount=("amount", "sum"))
            .reset_index()
        )

    return inner
