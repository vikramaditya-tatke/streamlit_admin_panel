from contextlib import closing
from pathlib import Path
from uuid import uuid4

import pytest

from clickhouse_admin_panel import create_clickhouse_client
from settings import Settings

SQL_DIRECTORY = Path(__file__).parent / "sql"


def sql(name: str) -> str:
    return (SQL_DIRECTORY / name).read_text(encoding="utf-8")


def pytest_addoption(parser):
    parser.addoption(
        "--clickhouse",
        action="store_true",
        help="Run integration tests against the configured ClickHouse server",
    )


@pytest.fixture
def config():
    return Settings(
        _env_file=None,
        clickhouse_base_url="127.0.0.1",
        clickhouse_port=8123,
        clickhouse_user="default",
        clickhouse_password="test-only",
        clickhouse_batch_size=10000,
        clickhouse_compression_protocol="lz4",
        clickhouse_secure=False,
    )


@pytest.fixture(scope="session")
def live_config(request):
    if not request.config.getoption("--clickhouse"):
        pytest.skip("Pass --clickhouse to run against a real server")
    return Settings()


@pytest.fixture(scope="session")
def live_client(live_config):
    with closing(create_clickhouse_client(live_config)) as client:
        yield client


@pytest.fixture(scope="session")
def dataset(live_client, live_config):
    database = f"compression_e2e_{uuid4().hex}"
    other_database = f"{database}_other"
    substitutions = {"__DATABASE__": database, "__OTHER_DATABASE__": other_database}

    def execute_file(name):
        contents = sql(name)
        for token, value in substitutions.items():
            contents = contents.replace(token, value)
        # One fixture statement per line preserves semicolons inside identifiers.
        for statement in contents.splitlines():
            if statement.strip():
                live_client.command(statement)

    execute_file("create_databases.sql")
    try:
        execute_file("seed.sql")
        yield live_config.model_copy(update={"clickhouse_database": database})
    finally:
        execute_file("drop_databases.sql")
