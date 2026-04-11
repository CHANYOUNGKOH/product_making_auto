"""Product Hub -- FastAPI 앱 진입점."""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

# -- 라우터 임포트 --
from hub.routers import dashboard, products, pipeline, export, stores

app = FastAPI(title="Product Hub", version="1.0.0")

# -- API 라우터 등록 --
app.include_router(dashboard.router)
app.include_router(products.router)
app.include_router(pipeline.router)
app.include_router(export.router)
app.include_router(stores.router)

# -- 정적 파일 (가장 마지막에 마운트) --
_static_dir = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="static")
