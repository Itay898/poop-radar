from contextlib import asynccontextmanager
import asyncio
import logging
import time
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse

from .services.oref_poller import poll_loop
from .services.alert_store import store
from .routers import predict, locations, alerts, stats
from .config import DEV_MODE

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Oref poller background task")
    task = asyncio.create_task(poll_loop())
    yield
    logger.info("Shutting down Oref poller")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Shower Radar API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(predict.router, prefix="/api")
app.include_router(locations.router, prefix="/api")
app.include_router(alerts.router, prefix="/api")
app.include_router(stats.router, prefix="/api")


if DEV_MODE:
    @app.post("/api/debug/inject-alert")
    async def inject_alert(area: str = Query(..., description="Area name to inject alert for")):
        """DEV/DEBUG: Inject a fake alert for testing the siren and alert banner."""
        alert = {
            "id": f"debug_{int(time.time())}",
            "cat": 1,
            "title": "ירי רקטות וטילים",
            "data": [area],
        }
        await store.add_alert(alert, time.time())
        store.set_current_active([{
            "id": alert["id"],
            "areas": [area],
            "title": alert["title"],
            "cat": 1,
            "timestamp": time.time(),
        }])
        logger.info(f"DEBUG: Injected fake alert for {area}")
        return {"status": "ok", "alert_id": alert["id"], "area": area}

    @app.post("/api/debug/clear-active")
    async def clear_active():
        """DEV/DEBUG: Clear active alerts."""
        store.set_current_active([])
        return {"status": "ok"}


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "connected": store.is_connected(),
        "history_count": len(store._history),
    }


# --- Serve frontend ---
@app.get("/")
async def serve_index():
    html = (FRONTEND_DIR / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(
        content=html,
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


# Static assets (og-image.png, etc.)
app.mount("/", StaticFiles(directory=str(FRONTEND_DIR)), name="frontend")
