import threading

from .load_db_profile import load_duckdb_target
from .common import create_server


def load_ipython_extension(ipython):
    server = create_server(load_duckdb_target())
    bv_server_thread = threading.Thread(target=server.serve_forever)
    bv_server_thread.start()
    ipython.push({"db": server.conn.db})
    ip, port = server.server_address
    print(
        f"Buena Vista server listening on {ip}:{port} and DuckDB connection available as 'db' variable."
    )


def unload_ipython_extension(ipython):
    pass
