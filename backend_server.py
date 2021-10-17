import logging
import os

import aiomysql
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import recommendations_api

DB_NAME = "mos_lib_hack"

logging.getLogger().setLevel(logging.INFO)

app = FastAPI()


@app.on_event("startup")
async def _startup():
    try:
        db = DB_NAME
        usr = "root"
        # pwd = "password"
        hst = "localhost"
        prt = 3306
        app.state.pool = await aiomysql.create_pool(host=hst, port=prt, user=usr, db=db, minsize=2)
        logging.info(f"Connected to Mysql")

    except:
        logging.info(f"cannot connect to MySQL")
        return


def configure():
    configure_routing()
    configure_cors()


def configure_routing():
    app.include_router(recommendations_api.router)


def configure_cors():
    origins = ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


configure()

if __name__ == "__main__":
    uvicorn.run("backend_server:app", host="0.0.0.0", port=5000, reload=True)
