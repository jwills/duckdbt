import typing as t

from buenavista.backends.duckdb import DuckDBConnection
from buenavista.core import Connection, Extension
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.environments import Environment

from . import extensions


def create(creds: DuckDBCredentials) -> t.Tuple[Connection, t.List[Extension]]:
    db = Environment.initialize_db(creds)
    conn = DuckDBConnection(db)
    exts = [extensions.DbtPythonRunner(), extensions.DbtLoadSource(creds.plugins)]
    return conn, exts


if __name__ == "__main__":
    import argparse
    import logging
    import threading

    import uvicorn
    from buenavista.examples import duckdb_http
    from buenavista.postgres import BuenaVistaServer
    from buenavista.http.main import quacko

    from .api import app
    from .load_db_profile import load_duckdb_target

    logging.basicConfig(format="%(thread)d: %(message)s", level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--port",
        help="The port the HTTP server should listen on",
        default=8080,
        type=int,
    )
    parser.add_argument(
        "--log_level",
        help="The log level to use for the HTTP server",
        default="info",
        type=str,
    )
    args = parser.parse_args()

    creds = load_duckdb_target()
    conn, exts = create(creds)
    app.conn = conn

    # Postgres server setup
    remote = creds.remote
    host, pg_port = remote.host, remote.port
    server = BuenaVistaServer((host, pg_port), app.conn, extensions=exts)
    bv_server_thread = threading.Thread(target=server.serve_forever)
    bv_server_thread.daemon = True
    bv_server_thread.start()

    # Configure quacko and start the HTTP server
    quacko(app, app.conn, duckdb_http.rewriter, exts)
    uvicorn.run(app, host=host, port=args.port, log_level=args.log_level)
