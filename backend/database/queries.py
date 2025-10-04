import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from sqlalchemy import text

from backend.models.inner import UserCollision

CREATE_CHECK_CONFLICTS_FUNCTION_QUERY = """
CREATE OR REPLACE FUNCTION check_conflict(a ANYELEMENT, b ANYELEMENT)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN a is not NULL AND b is not NULL AND a != b;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
"""

DROP_CHECK_CONFLICTS_FUNCTION_QUERY = """
DROP FUNCTION IF EXESTS check_conflict(ANYELEMENT, ANYELEMENT);
"""

UPDATE_USERS_QUERY = """
WITH input_data AS (
        SELECT
            unnest(CAST(:docs           AS varchar(16)[])) as doc_num,
            unnest(CAST(:last_names     AS varchar(20)[])) as last_name,
            unnest(CAST(:first_names    AS varchar(20)[])) as first_name,
            unnest(CAST(:second_names   AS varchar(20)[])) as second_name,
            unnest(CAST(:second_abbrevs AS varchar(4)[]))  as second_name1,
            unnest(CAST(:births         AS date[]))        as birth_date,
            unnest(CAST(:sex            AS boolean[]))     as is_man
    ),
    conflicts AS (
        SELECT inp.doc_num as doc,
               inp.last_name, inp.first_name, inp.second_name, inp.second_name1,
               inp.birth_date, inp.is_man,
               usr.last_name, usr.first_name, usr.second_name, usr.second_name1,
               usr.birth_date, usr.is_man
        FROM input_data inp
        JOIN users usr ON inp.doc_num = usr.doc_num
        WHERE check_conflict(inp.last_name, usr.last_name)
           OR check_conflict(inp.first_name, usr.first_name)
           OR check_conflict(inp.second_name, usr.second_name)
           OR check_conflict(inp.second_name1, usr.second_name1)
           OR check_conflict(inp.birth_date, usr.birth_date)
           OR check_conflict(inp.is_man, usr.is_man)
    ),
    upsert_data AS (
        SELECT * FROM input_data
        WHERE doc_num NOT IN (SELECT doc FROM conflicts)
    ),
    upsert_result AS (
        INSERT INTO users
        SELECT * FROM upsert_data
        ON CONFLICT (doc_num) DO UPDATE SET
            last_name    = COALESCE(EXCLUDED.last_name,    users.last_name),
            first_name   = COALESCE(EXCLUDED.first_name,   users.first_name),
            second_name  = COALESCE(EXCLUDED.second_name,  users.second_name),
            second_name1 = COALESCE(EXCLUDED.second_name1, users.second_name1),
            birth_date   = COALESCE(EXCLUDED.birth_date,   users.birth_date),
            is_man       = COALESCE(EXCLUDED.is_man,       users.is_man)
        RETURNING 1
    )
SELECT * FROM conflicts
"""

async def execute_async(session: AsyncSession, query, **kwargs):
    return await session.execute(text(query), kwargs)

def execute_sync(session: Session, query, **kwargs):
    return session.execute(text(query), kwargs)

async def update_users(
    session: AsyncSession,
    docs,
    last_names,
    first_names,
    second_names,
    second_abbrevs,
    births,
    sex
) -> list[UserCollision]:
    result = await execute_async(
        session,
        UPDATE_USERS_QUERY,
        docs=docs,
        last_names=last_names,
        first_names=first_names,
        second_names=second_names,
        second_abbrevs=second_abbrevs,
        births=births,
        sex=sex
    )
    result = result.fetchall()
    print(result[:10])
    return [UserCollision.from_tuple(res) for res in result]
