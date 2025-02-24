# Standard Imports
import sqlite3
from typing import Generator
from uuid import uuid4

# Third Party Imports
import pytest

# Project Imports


@pytest.fixture
def db_uri() -> str:
    # A unique in-memory database URI for each test
    # This is to ensure that each test has a clean slate
    return f"file:memdb_{uuid4()}?mode=memory&cache=shared"


@pytest.fixture
def db_connection(db_uri: str) -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(db_uri, uri=True)
    yield conn

    # Clean up
    conn.close()


@pytest.fixture
def setup_e2e_tables(db_connection: sqlite3.Connection) -> None:
    cursor = db_connection.cursor()
    cursor.execute("""
        CREATE TABLE source_customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE source_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount DECIMAL(10,2),
            order_date DATE
        )
    """)

    cursor.execute("""
        CREATE TABLE customer_orders (
            customer_id INTEGER,
            name TEXT,
            total_orders INTEGER,
            total_amount DECIMAL(10,2)
        )
    """)
    db_connection.commit()


@pytest.fixture
def populate_source_e2e_tables(db_connection: sqlite3.Connection, setup_e2e_tables) -> None:  # noqa: ARG001
    cursor = db_connection.cursor()

    # Insert customer test data
    cursor.executemany(
        "INSERT INTO source_customers VALUES (?, ?, ?)",
        [
            (1, "John Doe", "john@example.com"),
            (2, "Jane Smith", "jane@example.com"),
        ],
    )

    # Insert order test data
    cursor.executemany(
        "INSERT INTO source_orders VALUES (?, ?, ?, ?)",
        [(1, 1, 100.00, "2023-01-01"), (2, 1, 150.00, "2023-01-02"), (3, 2, 200.00, "2023-01-03")],
    )

    db_connection.commit()
