"""FastAPI application entry point."""
import logging
import os
from pathlib import Path
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse

from backend.database import init_db
from backend.routers.auth import router as auth_router
from backend.routers.api import router as api_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="HERP Agent Tracker", version="1.0.0")

# CORS for local dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(auth_router)
app.include_router(api_router)

# Resolve project root (where main.py's parent dir lives)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


@app.on_event("startup")
def startup():
    logger.info("Initializing database...")
    init_db()
    logger.info("Database ready.")


@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    """Serve the main dashboard HTML."""
    html_path = PROJECT_ROOT / "herp_agent_tracker.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>Dashboard not found</h1>", status_code=404)


@app.get("/health")
def health():
    return {"status": "ok", "service": "herp-agent-tracker"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
