from __future__ import annotations

import calendar
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    String,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from backend.database.base import BaseTable


class DBUsers(BaseTable):
    __tablename__ = "users"

    doc_num = Column(String(length=16), primary_key=True)

    last_name = Column(String(length=20), nullable=False)
    first_name = Column(String(length=20), nullable=False)
    second_name = Column(String(length=20))
    second_name1 = Column(String(length=4))
    birth_date = Column(Date())
    is_man = Column(Boolean())
