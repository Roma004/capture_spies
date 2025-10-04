import asyncio
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_scoped_session,
    create_async_engine,
)
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from backend.settings import db_settings


def make_async_engine(
    database_url: str = db_settings.full_url_async,
):
    return create_async_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=20,
        max_overflow=0,
        connect_args={
            "timeout": db_settings.timeout,
        },
    )


def make_sync_engine(database_url: str = db_settings.full_url_sync):
    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=0,
        connect_args={
            "connect_timeout": db_settings.timeout,
            "options": f"-c statement_timeout={db_settings.timeout*1000}",
        },
    )


def make_async_session_factory(
    database_url: str = db_settings.full_url_async,
    engine: Optional[AsyncEngine] = None,
) -> sessionmaker:
    engine = engine or make_async_engine(database_url)

    return sessionmaker(
        autoflush=False,
        autocommit=False,
        bind=engine,
        class_=AsyncSession,
    )


def make_sync_session_factory(
    database_url: str = db_settings.full_url_sync,
    engine: Optional[Engine] = None,
):
    engine = engine or make_sync_engine(database_url)

    return sessionmaker(
        autoflush=False,
        autocommit=False,
        bind=engine,
    )


def make_async_scoped_session_factory(
    database_url=db_settings.full_url_async,
) -> async_scoped_session:
    session_factory = make_async_session_factory(database_url)
    return async_scoped_session(session_factory, scopefunc=asyncio.current_task)


def make_sync_scoped_session_factory(
    database_url: str = db_settings.full_url_sync,
) -> scoped_session:
    session_factory = make_sync_session_factory(database_url)
    return scoped_session(session_factory)


def make_async_scoped_session(
    database_url: str = db_settings.full_url_async,
) -> AsyncSession:
    # noinspection PyShadowingNames
    AsyncSession = make_async_scoped_session_factory(database_url)
    return AsyncSession()


def make_sync_scoped_session(
    database_url: str = db_settings.full_url_sync,
) -> Session:
    ScopedSession = make_sync_scoped_session_factory(database_url)
    return ScopedSession()


async_scoped_session_factory = make_async_scoped_session_factory()
async_session_factory = make_async_session_factory()


async def get_session():
    async with make_async_scoped_session() as session:
        yield session


get_session_contextmanager = async_session_factory.begin
