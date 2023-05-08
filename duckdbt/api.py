from pathlib import Path
from typing import Any, List

from fastapi import FastAPI, Depends, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from buenavista.core import Session


app = FastAPI(title="DuckDBT")
BASE_PATH = Path(__file__).resolve().parent
TEMPLATES = Jinja2Templates(directory=str(BASE_PATH / "templates"))
app.mount(
    "/static",
    StaticFiles(directory=BASE_PATH / "static"),
    name="static",
)


def bv_session() -> Session:
    sess = app.conn.create_session()
    try:
        yield sess
    finally:
        sess.close()


@app.get("/")
def index(request: Request, session=Depends(bv_session)):
    qr = session.execute_sql(
        """
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        ORDER BY table_schema, table_name, ordinal_position
        """
    )
    if qr.has_results():
        res = {}
        for row in qr.rows():
            relation = f"{row[0]}.{row[1]}"
            if relation not in res:
                res[relation] = []
            res[relation].append(row[2])
    else:
        res = {}
    return TEMPLATES.TemplateResponse(
        "index.html", {"request": request, "relations": res}
    )
