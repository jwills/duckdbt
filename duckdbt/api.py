from pathlib import Path
from typing import Any, List

from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from pydantic import BaseModel
from starlette.responses import FileResponse

from buenavista.adapter import AdapterHandle

app = FastAPI(title="DuckDBT")

BASE_PATH = Path(__file__).resolve().parent
TEMPLATES = Jinja2Templates(directory=str(BASE_PATH / "templates"))

app.mount(
    "/static",
    StaticFiles(directory=BASE_PATH / "static"),
    name="static",
)


def bv_handle() -> AdapterHandle:
    handle = app.bv.adapter.create_handle()
    try:
        yield handle
    finally:
        handle.close()


@app.get("/")
def index(request: Request, handle=Depends(bv_handle)):
    qr = handle.execute_sql(
        "SELECT table_schema, table_name, column_name FROM information_schema.columns ORDER BY table_schema, table_name, ordinal_position"
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


class QueryRequest(BaseModel):
    sql: str


class QueryResult(BaseModel):
    columns: List[str]
    rows: List[List[Any]]


@app.post("/api/query", response_model=QueryResult)
def query(q: QueryRequest, handle=Depends(bv_handle)):
    try:
        query_result = handle.execute_sql(q.sql)
        if query_result.has_results():
            columns = [
                query_result.column(i)[0] for i in range(query_result.column_count())
            ]
            return QueryResult(columns=columns, rows=list(query_result.rows()))
        return QueryResult(columns=[], rows=[])
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
