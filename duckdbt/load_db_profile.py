from argparse import Namespace

from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.flags import set_from_args
from dbt.config.project import load_raw_project
from dbt.config.profile import read_profile, Profile
from dbt.config.renderer import ProfileRenderer


def load_duckdb_target(project_root: str = ".", profile_dir=".") -> DuckDBCredentials:
    """Load the DuckDBCredentials from the profiles.yml file.

    This method was derived from the fal dbt adapter configuration
    defined here:

    https://github.com/fal-ai/fal/blob/main/projects/adapter/src/dbt/adapters/fal/load_db_profile.py
    """
    set_from_args(Namespace(STRICT_MODE=True), {})

    profile_renderer = ProfileRenderer()
    raw_project = load_raw_project(project_root)
    raw_profile_name = raw_project.get("profile")
    profile_name = profile_renderer.render_value(raw_profile_name)
    raw_profiles = read_profile(profile_dir)
    raw_profile = raw_profiles[profile_name]
    if "target" in raw_profile:
        target_name = profile_renderer.render_value(raw_profile["target"])
    else:
        target_name = "default"
    target_dict = Profile._get_profile_data(
        profile=raw_profile,
        profile_name=profile_name,
        target_name=target_name,
    )
    rendered_dict = profile_renderer.render_data(target_dict)
    return DuckDBCredentials.from_dict(rendered_dict)
