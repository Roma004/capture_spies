from pydantic import BaseModel

class BaseResponse(BaseModel):
    status: str = "ok"

class UpdateUsersResponse(BaseResponse):
    problems: list[str] | None = None

    @classmethod
    def make_ok(cls):
        return cls()

    @classmethod
    def make_partial(cls, problems: list[str]):
        return cls(
            status="warn",
            problems=problems
        )
