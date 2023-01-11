#!/usr/bin/python3

import os
import shutil

import typer


app = typer.Typer()
mods = ("admin", "load", "transform", "report")

# DB flow
# metadata DuckDB instance
# raw DuckDB
# analytics DuckDB
# metadata keeps track of stuff we do in here
# raw is for ingest-- incremental or full
# analytics derives from raw
# on full-refresh, make a copy of raw, generate analytics against it
# on incremental, make a copy of the last analytics, run incremental against it
# so clearly I need some sort of DatabaseManager abstraction
# where I can request a particular DB by label (meta, raw, analytics) and a version (TS?)
# and then get back a (local!) DuckDB file I can use to work with that DB
# it's going to look like a builder -- like I will say "repo, give me a copy of label/version that is now labeled as new_label/new_version,
or just give me the exact copy of label/version)"
# so the API should look like repo.get(label:version), repo.create(label:version, from=None|label:version), repo.delete(label:version)
# should there be multiple impls of a repo?
# like how would an iceberg-backed repo work vs. a local directory-backed repo?
# sort of unclear at this point, would need to figure it out-- like I'm thinking it might just be a local directory that is always the
# repo-- but then I would need like a combo of the DuckDB file _AND_ an init.sql script that one would need to run in order to work
# with the file after it was created (e.g., to load the extensions one would need for working with iceberg/parquet data files.)
# It feels like the repo concept is so fundamental that it should be config'd in a repo.yml file at the top-level of the project
#
# the necessity of the repo means that we have to include it in every step of the flow-- like in the CI and in the dev/prod runs.
# how do we feel about that?
# do I need/want to be able to look at ingest as it runs? Do I need a version of target-duckdb that can talk to Buena Vista?
# So like quack dev up would need to know which label:version you wanted to work with for your dev workflow-- there is some
# interesting workflow stuff in there: like, you could use the latest local version of analytics, or if there wasn't one you could
# either fetch the latest local version of raw or the latest remote version of analytics?
# there clearly needs to be a repo push operation-- so maybe it's repo push/pull/branch? So pull would bring it local, push would
# upstream it to the remote, copy would copy it to a new label? So then mentally, you're always working in a particular database
# when you're using stuff in here, which makes some semblance of sense, I think?
#
# Then, if you were starting from scratch, you would need to load first, which would create a raw db-- so your init state is in
# a raw DB that you would need to load some stuff into by setting up the loader-- which would venv your taps along with their
# config and state. I need to give some thought here--
# we also need to create the metadata DB right away, since the first loader stuff will need to be aware of it.
#
# The admin just needs a way to run a specific SQL/Python script in version control against the current DB in the project as-is.
#
# When I'm running in CI/cron, how would I have different loaders writing to different schemas of the same DB instance? I clearly
# need the upstream stuff (which should obvi be exporting to parquet/iceberg as it goes) be partitioned by schema (so db:version + schema
# as the upload key-- and maybe the download key as well?) So it's really ns + schema + version? So a push/pull operation would default
# to all schemas *unless* they were clearly specified?
#
# Note that the above is relevant for dbt runs as well-- like a dev run should be done in a schema with like full defer operations
# to a prior run state, and a dev schema should be publishable upstream.
#
# Given a current DB (assume the metadata DB is always present?) then "quack up" should launch a BV server around it, and _then_
# you should be enabled to work with it from dbt like normal (like, you can run all of the normal dbt commands from the dbt project
# directory, no quack invoke or whatever required.)
@app.command()
def init(dir: str):
    """Initialize a new duckdbt project."""
    for d in mods:
        p = os.path.join(dir, d)
        if not os.path.exists(p):
            os.mkdir(p)
        else:
            raise Exception(f"Path {p} already exists!")


@app.command()
def cleanup(dir: str):
    """Delete a duckdbt project to help me as I iterate on developing this"""
    for d in mods:
       p = os.path.join(dir, d)
       shutil.rmtree(p)

       
if __name__ == '__main__':
    app() 
