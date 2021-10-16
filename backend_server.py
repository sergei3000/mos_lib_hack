import logging
import os

import aiomysql
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import recommendations_api

MIN_DB_CONN_POOL_SIZE = 5

logger = logging.getLogger("main.py")

app = FastAPI()


# @app.on_event("startup")
# async def _startup():
#     try:
#         db = "mos_lib_hack"
#         usr = "root"
#         # pwd = "password"
#         hst = "localhost"
#         prt = 3306
#         app.state.pool = await aiomysql.create_pool(host=hst, port=prt, user=usr, db=db)
#         logger.info(f"Connected to Mysql")

#     except ConnectionRefusedError as e:
#         logger.info(f"cannot connect to MySQL")
#         return


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
