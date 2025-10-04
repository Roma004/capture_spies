from datetime import date, datetime
from typing import Coroutine

from sqlalchemy import Column, Date, MetaData, extract, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import as_declarative

metadata = MetaData()


@as_declarative(metadata=metadata)
class BaseTable:
    @classmethod
    def get_stmt(cls, *args, **kwargs):
        stmt = select(cls).options(*args)
        for key, value in kwargs.items():
            stmt = stmt.where(getattr(cls, key) == value)
        return stmt

    @classmethod
    async def get(cls, session: AsyncSession, *args, **kwargs):
        stmt = cls.get_stmt(*args, **kwargs)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()
