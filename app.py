import asyncpg
import settings
import json
from fastapi import FastAPI
import uvicorn
from starlette.requests import Request


def setup(app):
    @app.on_event("startup")
    async def startup():
        app.state.db_connect = await asyncpg.connect(settings.settings.pg_connection_string)

    @app.on_event("shutdown")
    async def shutdown():
        await app.state.db_connect.close()


app = FastAPI()
setup(app)


@app.get('/api/business/')
async def get(request: Request, business_id: str):
    result = await request.app.state.db_connect.fetchval("SELECT aggregation from parsed_total order by id desc limit 1")

    r = json.loads(result)
    res = {'total': r.get('cnt', {}).get(business_id, 0),
           'cnt_total': r.get('cnt_total', {}).get(business_id, 0),
           'cnt_line_items': r.get('cnt_line_items', {}).get(business_id, 0)
           }

    return res


if __name__ == '__main__':
    uvicorn.run("app:app", host="0.0.0.0", port=9000)
