#!/usr/bin/python3

import os
import shutil

import typer


app = typer.Typer()

mods = ("admin", "load", "transform", "report")

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
