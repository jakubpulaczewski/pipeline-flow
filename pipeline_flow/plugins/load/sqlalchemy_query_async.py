# Standard Imports
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncGenerator, Generator

if TYPE_CHECKING:
    from typing import Self

    from pandas import DataFrame

# Third Party Imports
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Project Imports
from pipeline_flow.plugins import ILoadPlugin


class AsyncSQLAlchemyQueryLoader(ILoadPlugin, plugin_name="sqlalchemy_query_loader"):
    """A plugin that loads data into a database using SQLAlchemy query asynchronously.

    Args:
        db_user (str):  The username for the database.
        db_password (str): The password for the database.
        db_host (str): The host for the database.
        db_port (str): PORT number for the database.
        db_name (str): The name of the database.
        query (str): The query to execute uses SQLAlchemy text syntax.
        concurrency_limit (int, optional): A sephomore limit on asyncio task concurrency. Defaults to 5.
        batch_size (int, optional): The batch size. Defaults to 100000.
        driver (str, optional): The database driver. Ensure that you are using asychronous driver.
                                Defaults to "mysql+asyncmy".
    """

    def __init__(  # noqa: PLR0913
        self: Self,
        db_user: str,
        db_password: str,
        db_host: str,
        db_port: str,
        db_name: str,
        query: str,
        concurrency_limit: int = 5,
        batch_size: int = 100000,
        driver: str = "mysql+asyncmy",
    ) -> None:
        self.db_user = db_user
        self.db_password = db_password

        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name

        self._query = query
        self._batch_size = batch_size
        self._driver = driver

        self._semaphore = asyncio.Semaphore(concurrency_limit)

        self._session_maker = self._build_async_sessionmaker()

    def _build_connection_string(self: Self) -> str:
        """A helper method that builds the connection string for the database.

        Returns:
            str: The connection string.
        """
        return f"{self._driver}://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    def _build_async_sessionmaker(self: Self) -> async_sessionmaker[AsyncSession]:
        """A helper method that builds an async session maker.

        In this link you can find more information about async context managers:
            https://docs.sqlalchemy.org/en/20/orm/session_basics.html#when-do-i-make-a-sessionmaker

        Returns:
            async_sessionmaker[AsyncSession]: An async session maker.
        """
        database_url = self._build_connection_string()
        engine = create_async_engine(database_url)
        return async_sessionmaker(engine)

    @asynccontextmanager
    async def get_async_session(self: Self) -> AsyncGenerator[AsyncSession]:
        """A context manager that yields an async session using the async session maker.

        Yields:
            Iterator[AsyncGenerator[AsyncSession, None]]: An async session.
        """
        async with self._session_maker() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            else:
                await session.commit()

    def chunk_dataframe(self: Self, df: DataFrame) -> Generator[list[dict]]:
        """A generator that chunks a pandas DataFrame into smaller dataframes.

        The chunk size is defined by the `_batch_size` attribute of the class.

        Args:
            df (pd.DataFrame): Extracted or transformed data.

        Yields:
            Generator[list[dict]]: A list of dictionaries containing the data.
        """
        for i in range(0, len(df), self._batch_size):
            yield df.iloc[i : i + self._batch_size].to_dict("records")

    async def execute_batch_query(self: Self, batch: list[dict]) -> None:
        """Executes a batch query. As per the SQLAlchemy documentation, new AsyncSession
        is created for each concurrent asyncio task.

        Here is the link to the documentation:
            https://docs.sqlalchemy.org/en/20/orm/session_basics.html#is-the-session-thread-safe-is-asyncsession-safe-to-share-in-concurrent-tasks

        Args:
            batch (list[dict]): A batch of data from the DataFrame.
        """
        async with self._semaphore:  # noqa: SIM117 - asyncio is exempt from this rule.
            async with self.get_async_session() as session:
                await session.execute(text(self._query), batch)

    async def __call__(self, data: DataFrame) -> None:
        """A method that loads data into a database using SQLAlchemy using query.

        Args:
            data (pd.DataFrame): Extracted or transformed data from the pipeline.
        """

        async with asyncio.TaskGroup() as tg:
            for batch in self.chunk_dataframe(data):
                tg.create_task(self.execute_batch_query(batch))
