from fastapi import Body, Depends, UploadFile
from fastapi.exceptions import HTTPException
from fastapi.routing import APIRouter
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status

from backend.database.session import get_session
from backend.models.requests import UpdateDataRequest
from backend.models.responses import UpdateUsersResponse
from backend.utils import user as user_utils


router = APIRouter()


@router.post(
    "/update_users",
    response_model=UpdateUsersResponse
)
async def update_data(
    upload_file: UploadFile,
    session: AsyncSession = Depends(get_session),
) -> UpdateUsersResponse:
    logger.info("Start update users")
    result = await user_utils.update_users(session, upload_file.file)
    await session.commit()
    logger.info("End Update users")
    return result
