import logging
import threading
from typing import Any, Dict

import duckdb
import uvicorn

from buenavista.backends.duckdb import DuckDBConnection
from buenavista.core import Connection
from buenavista.examples import duckdb_http, duckdb_postgres
from buenavista.postgres import BuenaVistaServer
from buenavista.http.main import quacko
from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.environments import Environment

from .extensions import DbtPythonRunner
from .api import app


def create(config: Dict[str, Any]) -> Connection:
    creds = DuckDBCredentials.from_dict(config)
    db = Environment.initialize_db(creds, duckdb.connect(config["path"]))
    return DuckDBConnection(db)


if __name__ == "__main__":
    # Gratefully copied (with slight modifications) from
    # https://github.com/fal-ai/fal/blob/main/adapter/src/dbt/adapters/fal/impl.py#L24
    import os

    from dbt.config.profile import Profile, read_profile
    from dbt.config.project import Project
    from dbt.config.renderer import ProfileRenderer
    from dbt.config.utils import parse_cli_vars
    from dbt.main import _build_base_subparser
    from dbt import flags

    # includes vars, profile, target
    parser = _build_base_subparser()
    args, unknown = parser.parse_known_args()

    # dbt-core does os.chdir(project_dir) before reaching this location
    # from https://github.com/dbt-labs/dbt-core/blob/73116fb816498c4c45a01a2498199465202ec01b/core/dbt/task/base.py#L186
    project_root = os.getcwd()

    # from https://github.com/dbt-labs/dbt-core/blob/19c48e285ec381b7f7fa2dbaaa8d8361374136ba/core/dbt/config/runtime.py#L193-L203
    version_check = bool(flags.VERSION_CHECK)
    partial = Project.partial_load(project_root, verify_version=version_check)

    cli_vars: Dict[str, Any] = parse_cli_vars(getattr(args, "vars", "{}"))
    profile_renderer = ProfileRenderer(cli_vars)
    project_profile_name = partial.render_profile_name(profile_renderer)

    # from https://github.com/dbt-labs/dbt-core/blob/19c48e285ec381b7f7fa2dbaaa8d8361374136ba/core/dbt/config/profile.py#L423-L425
    profile_name = Profile.pick_profile_name(
        getattr(args, "profile", None), project_profile_name
    )
    raw_profiles = read_profile(flags.PROFILES_DIR)
    raw_profile = raw_profiles[profile_name]

    # from https://github.com/dbt-labs/dbt-core/blob/19c48e285ec381b7f7fa2dbaaa8d8361374136ba/core/dbt/config/profile.py#L287-L293
    target_override = getattr(args, "target", None)
    if target_override is not None:
        target_name = target_override
    elif "target" in raw_profile:
        target_name = profile_renderer.render_value(raw_profile["target"])
    else:
        target_name = "default"

    logging.basicConfig(format="%(thread)d: %(message)s", level=logging.INFO)

    dict = Profile._get_profile_data(raw_profile, profile_name, target_name)
    app.conn = create(dict)
    extensions = [DbtPythonRunner()]

    remote = dict.get("remote", {})
    host = remote.get("host", "127.0.0.1")
    pg_port = int(remote.get("port", 5433))
    api_port = int(remote.get("api_port", 8080))

    if pg_port:
        address = (host, int(pg_port))
        server = BuenaVistaServer(
            address, app.conn, extensions=extensions
        )
        bv_server_thread = threading.Thread(target=server.serve_forever)
        bv_server_thread.daemon = True
        bv_server_thread.start()

    # Configure the Presto protocol ("quacko") on the app
    if api_port:
        quacko(app, app.conn, duckdb_http.rewriter, extensions)

        uvicorn.run(
            app,
            host=host,
            port=api_port,
            log_level=remote.get("log_level", "info"),
        )
