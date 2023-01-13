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
def index(request: Request, handle = Depends(bv_handle)):
    qr = handle.execute_sql("SELECT table_schema, table_name FROM information_schema.tables ORDER BY 1, 2")
    if qr.has_results():
        relations = [f"{s}.{t}" for (s, t) in qr.rows()]
    else:
        relations = []
    return TEMPLATES.TemplateResponse("index.html", {"request": request, "relations": relations})


@app.get("/api/list_columns")
def list_columns(relation: str, handle = Depends(bv_handle)):
    try:
        schema, table = relation.split(".")
        qr = handle.execute_sql("SELECT column_name FROM information_schema.columns WHERE table_schema = ? and table_name = ? ORDER BY ordinal_position", params=(schema, table))
        if qr.has_results():
            return [r[0] for r in qr.rows()]
        return []
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


class QueryRequest(BaseModel):
    sql: str

class QueryResult(BaseModel):
    columns: List[str]
    rows: List[List[Any]]

@app.post("/api/query", response_model=QueryResult)
def query(q: QueryRequest, handle = Depends(bv_handle)):
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