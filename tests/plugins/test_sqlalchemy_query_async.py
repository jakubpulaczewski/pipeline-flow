# Standard Imports
from __future__ import annotations

import random

# Third Party Imports
import pandas as pd
import pytest
from sqlalchemy import Column, MetaData, String, Table, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Project Imports
from pipeline_flow.plugins.load import AsyncSQLAlchemyQueryLoader


def generate_pandas_data(total: int) -> pd.DataFrame:
    """Generate data and return as a pandas DataFrame."""
    data = [{"name": f"some name {i} - {random.randint(0, 1000)}"} for i in range(total)]  # noqa: S311
    return pd.DataFrame(data, columns=["name"])


@pytest.fixture
def db_config() -> dict[str, str]:
    return {
        "db_user": "myuser",
        "db_password": "mypassword",
        "db_host": "localhost",
        "db_port": "3306",
        "db_name": "mydatabase",
    }


@pytest.fixture
def build_database_uri(db_config: dict) -> str:
    return f"mysql+asyncmy://{db_config['db_user']}:{db_config['db_password']}@{db_config['db_host']}:{db_config['db_port']}/{db_config['db_name']}"


@pytest.fixture
def async_session_factory(build_database_uri: str) -> async_sessionmaker[AsyncSession]:
    engine = create_async_engine(build_database_uri, echo=True)
    return async_sessionmaker(engine)


@pytest.fixture
async def setup_table(async_session_factory: async_sessionmaker[AsyncSession]) -> None:
    """Setup a table with one column for testing."""
    meta = MetaData()
    Table("t1", meta, Column("name", String(50), primary_key=True))

    async with async_session_factory() as session:
        conn = await session.connection()
        await conn.run_sync(meta.drop_all)
        await conn.run_sync(meta.create_all)
        await conn.commit()
        assert conn is not None


@pytest.mark.asyncio
async def test_async_sqlalchemy_loader(
    setup_table, async_session_factory: async_sessionmaker[AsyncSession], db_config: dict[str, str]
) -> None:
    # Awaits the setup_table fixture to complete the creation of the table
    await setup_table

    # Initialize the async SQLAlchemy loader
    load = AsyncSQLAlchemyQueryLoader(
        db_user=db_config["db_user"],
        db_password=db_config["db_password"],
        db_host=db_config["db_host"],
        db_port=db_config["db_port"],
        db_name=db_config["db_name"],
        query="INSERT INTO t1 (name) VALUES (:name)",
    )

    # Generate test data
    pandas_df = generate_pandas_data(total=100000)

    await load(data=pandas_df)

    async with async_session_factory() as session:
        result = await session.execute(text("SELECT COUNT(*) FROM t1"))
        row_count = result.scalar()
        assert row_count == 100000
