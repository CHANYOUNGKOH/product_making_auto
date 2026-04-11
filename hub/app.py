"""Product Hub — FastAPI 앱 진입점."""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from hub.routers import dashboard, products, pipeline, export, stores
from hub.services import db_service as _db_service


@asynccontextmanager
async def _lifespan(app: FastAPI):
    _db_service.run_migrations()
    yield


app = FastAPI(title="Product Hub", version="1.0.0", lifespan=_lifespan)

app.include_router(dashboard.router)
app.include_router(products.router)
app.include_router(pipeline.router)
app.include_router(export.router)
app.include_router(stores.router)

_static_dir = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="static")
