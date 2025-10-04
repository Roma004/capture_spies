from pydantic import BaseModel

from backend.models.inner import UserCollision

class BaseResponse(BaseModel):
    status: str = "ok"

class UpdateUsersResponse(BaseResponse):
    problems: list[UserCollision] | None = None

    @classmethod
    def make_ok(cls):
        return cls()

    @classmethod
    def make_partial(cls, problems: list[UserCollision]):
        return cls(
            status="warn",
            problems=problems
        )
