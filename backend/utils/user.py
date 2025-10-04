from loguru import logger
from backend.utils.common import load_file, check_non_empty
from backend.models.responses import UpdateUsersResponse
from backend.database import queries
from backend.database.session import AsyncSession
from backend.errors import UploadFileError
from datetime import datetime

import numpy as np
import pandas as pd

USERS_TB_COLUMNS = set({
    "doc_num", "last_name", "first_name", "second_name", "second_name1",
    "birth_date", "is_man"
})

def parse_date(date_str):
    if date_str is None:
        return None
    return datetime.fromisoformat(date_str).date()

def nan_to_none(sr: pd.Series) -> pd.Series:
    return sr.where(sr.notna(), None)

async def update_users(
    session: AsyncSession,
    upload_file
) -> UpdateUsersResponse:
    problems = {}
    df = load_file(upload_file, cols_required=USERS_TB_COLUMNS)
    check_non_empty(df, "doc_num", "last_name", "first_name")
    if not df["doc_num"].is_unique:
        raise UploadFileError(detail="file contains duplicates")
    try:
        df["birth_date"] = nan_to_none(df["birth_date"]).apply(parse_date)
    except ValueError as e:
        raise UploadFileError(detail="birth_date value is invalid: " + str(e))
    result = await queries.update_users(
        session,
        docs=tuple(df["doc_num"]),
        last_names=tuple(df["last_name"]),
        first_names=tuple(df["first_name"]),
        second_names=tuple(nan_to_none(df["second_name"])),
        second_abbrevs=tuple(nan_to_none(df["second_name1"])),
        births=tuple(nan_to_none(df["birth_date"])),
        sex=tuple(nan_to_none(df["is_man"]))
    )
    if not result:
        return UpdateUsersResponse.make_ok()
    print(result)
    return UpdateUsersResponse.make_partial(result)
