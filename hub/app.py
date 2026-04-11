"""Product Hub — FastAPI 앱 진입점."""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from hub.routers import dashboard, products, pipeline, export, stores
from hub.services import db_service as _db_service

_db_service.run_migrations()

app = FastAPI(title="Product Hub", version="1.0.0")

app.include_router(dashboard.router)
app.include_router(products.router)
app.include_router(pipeline.router)
app.include_router(export.router)
app.include_router(stores.router)

_static_dir = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="static")
