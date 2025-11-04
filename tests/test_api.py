import asyncio
import os

from sqlmodel import SQLModel
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

import certus.db.session as session_mod
from certus.db.models import Symbol


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def setup_in_memory_db():
    url = "sqlite+aiosqlite:///:memory:"
    engine = create_async_engine(url, future=True)

    async def _init():
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)
    _run(_init())

    # monkeypatch session engine and get_session
    session_mod.engine = engine

    async def fake_get_session():
        async with AsyncSession(engine) as s:
            yield s

    session_mod.get_session = fake_get_session
    return engine


def test_health_and_symbols():
    # setup in-memory DB and patch session
    setup_in_memory_db()

    # insert a symbol
    async def _insert():
        async with AsyncSession(session_mod.engine) as s:
            s.add(Symbol(symbol="AAPL", name="Apple Inc."))
            await s.commit()
    _run(_insert())

    # import app after session patching
    from fastapi.testclient import TestClient
    from apps.api.main import app

    client = TestClient(app)

    r = client.get("/healthz")
    assert r.status_code == 200
    j = r.json()
    assert j.get("status") == "ok"
    assert j.get("db") is True

    r2 = client.get("/internal/symbols?limit=10")
    assert r2.status_code == 200
    data = r2.json()
    assert isinstance(data, list)
    assert any(s.get("symbol") == "AAPL" for s in data)
