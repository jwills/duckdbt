import typing as t

from buenavista.backends.duckdb import DuckDBConnection
from buenavista.core import Connection, Extension
from buenavista.postgres import BuenaVistaServer

from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.environments import Environment

from . import extensions


def _initialize(creds: DuckDBCredentials) -> t.Tuple[Connection, t.List[Extension]]:
    db = Environment.initialize_db(creds)
    conn = DuckDBConnection(db)
    exts = [extensions.DbtPythonRunner(), extensions.DbtLoadSource(creds.plugins)]
    return conn, exts


def create_server(creds: DuckDBCredentials) -> BuenaVistaServer:
    conn, extensions = _initialize(creds)
    remote = creds.remote
    host, pg_port = remote.host, remote.port
    auth = None
    if remote.password:
        auth = {remote.user: remote.password}
    return BuenaVistaServer((host, pg_port), conn, extensions=extensions, auth=auth)
