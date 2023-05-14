import argparse
import logging
import threading

import uvicorn
from buenavista.examples import duckdb_http
from buenavista.http.main import quacko

from . import common
from .api import app
from .load_db_profile import load_duckdb_target


if __name__ == "__main__":
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
    server = common.create_server(creds)
    bv_server_thread = threading.Thread(target=server.serve_forever)
    bv_server_thread.daemon = True
    bv_server_thread.start()

    # Configure quacko and start the HTTP server
    app.conn = server.conn
    quacko(app, app.conn, duckdb_http.rewriter, server.extensions.values())
    uvicorn.run(app, host=creds.remote.host, port=args.port, log_level=args.log_level)
