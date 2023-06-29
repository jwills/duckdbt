from threading import Thread, RLock
from typing import Any

import duckdb_engine
from sqlalchemy.dialects import registry

from buenavista.backends.duckdb import DuckDBConnection
from buenavista.postgres import BuenaVistaServer


class DuckdbtDialect(duckdb_engine.Dialect):
    name = "duckdbt"
    _bv_server = None
    _bv_server_lock = RLock()

    @classmethod
    def _check_bv_server(cls, conn):
        with cls._bv_server_lock:
            if not cls._bv_server:
                bv_conn = DuckDBConnection(conn)
                cls._bv_server = BuenaVistaServer(("localhost", 5433), bv_conn)
                _bv_server_thread = Thread(target=cls._bv_server.serve_forever)
                _bv_server_thread.daemon = True
                _bv_server_thread.start()

    def connect(self, *cargs: Any, **cparams: Any) -> "Connection":
        conn_wrapper = super().connect(*cargs, **cparams)
        self._check_bv_server(conn_wrapper.c)
        return conn_wrapper


registry.register("duckdbt", "duckdbt.driver", "DuckdbtDialect")
