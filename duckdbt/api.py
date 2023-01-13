import os

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Use this to serve a public/index.html
from starlette.responses import FileResponse

app = FastAPI()
STATIC_FILE_DIR = os.path.join(os.path.dirname(__file__), "static")

app.mount(
    "/static",
    StaticFiles(directory=STATIC_FILE_DIR),
    name="static",
)


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_FILE_DIR, "index.html"))


class QueryRequest(BaseModel):
    sql: str


@app.post("/api/query")
def query(q: QueryRequest):
    handle = app.bv.adapter.create_handle()
    try:
        query_result = handle.execute_sql(q.sql)
        res = {}
        if query_result.has_results():
            res["columns"] = [
                query_result.column(i)[0] for i in range(query_result.column_count())
            ]
            res["rows"] = list(query_result.rows())
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
    finally:
        handle.close()
    return res
