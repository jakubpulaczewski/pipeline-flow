# Standard Imports
from __future__ import annotations

import random

# Third Party Imports
import pandas as pd
import pytest
from sqlalchemy import Column, MetaData, String, Table, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Project Imports
from pipeline_flow.core.parsers import YamlParser
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


@pytest.mark.slow
@pytest.mark.asyncio
async def test_async_sqlalchemy_loader(
    setup_table, async_session_factory: async_sessionmaker[AsyncSession], db_config: dict[str, str]
) -> None:
    # Awaits the setup_table fixture to complete the creation of the table
    await setup_table

    # Initialize the async SQLAlchemy loader
    load = AsyncSQLAlchemyQueryLoader(
        plugin_id="test_plugin",
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


def test_parse_sqlalchemy_query_loader_yaml() -> None:
    yaml_config = """
    load:
      steps:
        - plugin: sqlalchemy_query_loader
          args:
            db_user: myuser
            db_password: mypassword
            db_host: localhost
            db_port: 3306
            db_name: mydatabase
            query: SELECT 1
    """

    # Parse the YAML configuration
    parsed_yaml = YamlParser(stream=yaml_config).yaml_body
    load_step = parsed_yaml["load"]["steps"][0]

    # Assert that the plugin is correctly parsed
    assert load_step["plugin"] == "sqlalchemy_query_loader", "Plugin name did not match 'sqlalchemy_query_loader'"

    # Instantiate the plugin (plugin_id is assigned by `instantiate_plugin` in PluginRegistry)
    loader = AsyncSQLAlchemyQueryLoader(plugin_id="test_plugin", **load_step["args"])

    # Verify that the instantiated object is of the correct type
    assert isinstance(loader, AsyncSQLAlchemyQueryLoader), "Loader instance is not of type AsyncSQLAlchemyQueryLoader"
