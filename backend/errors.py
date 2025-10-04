from fastapi import status
from fastapi.exceptions import HTTPException


class UploadFileError(HTTPException):
    def __init__(
        self,
        status_code: int = status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail: str = "File is invalid. Check file columns and stucture.",
        headers: dict[str, str] = None,
    ):
        super().__init__(status_code, detail, headers)

