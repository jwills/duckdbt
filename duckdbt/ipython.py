import threading

from buenavista.postgres import BuenaVistaServer

from .load_db_profile import load_duckdb_target
from .server import create


def load_ipython_extension(ipython):
    creds = load_duckdb_target()
    conn, exts = create(creds)
    remote = creds.remote
    host, pg_port = remote.host, remote.port
    server = BuenaVistaServer((host, pg_port), conn, extensions=exts)
    bv_server_thread = threading.Thread(target=server.serve_forever)
    bv_server_thread.start()
    ipython.push({"db": conn.db})
    print(
        f"Buena Vista server listening on {host}:{pg_port} and DuckDB connection available as 'db' variable."
    )


def unload_ipython_extension(ipython):
    pass
