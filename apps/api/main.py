from fastapi import FastAPI, Depends, HTTPException
import pkg_resources
import os
import asyncio
from typing import List

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession as SQLModelAsyncSession
from certus.db.models import Symbol
from certus.db.session import get_session

app = FastAPI(title="certus-api", version="0.0.0")


async def _check_db(url: str) -> bool:
    """Check DB connectivity using the app's async SQLModel engine if available."""
    try:
        from certus.db.session import engine
    except Exception:
        return False
    try:
        async with engine.connect() as conn:
            await conn.execute("SELECT 1")
        return True
    except Exception:
        return False


@app.on_event("startup")
async def startup_event():
    # Check DB connectivity in background so /healthz can report readiness
    database_url = os.getenv("DATABASE_URL", "postgresql://certus:certus@localhost:5432/certus")
    app.state.db_ok = False
    try:
        app.state.db_ok = await _check_db(database_url)
    except Exception:
        app.state.db_ok = False


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "db": bool(app.state.db_ok)}


@app.get("/version")
async def version():
    try:
        # attempt to read package version if installed
        ver = pkg_resources.get_distribution("certus").version
    except Exception:
        ver = "dev"
    return {"version": ver}


@app.get("/internal/symbols", response_model=List[Symbol])
async def list_symbols(limit: int = 50, session: SQLModelAsyncSession = Depends(get_session)):
    """Return a list of symbols (for internal/dev use)."""
    try:
        result = await session.exec(select(Symbol).limit(limit))
        return result.all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("apps.api.main:app", host="0.0.0.0", port=8000, reload=True)
