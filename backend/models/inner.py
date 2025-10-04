from pydantic import BaseModel
from datetime import date

class User(BaseModel):
    doc_num: str
    last_name: str
    first_name: str
    second_name: str | None
    second_name_abbrev: str | None
    birth_date: date | None
    is_man: bool | None

class UserCollision(BaseModel):
    existing: User
    new: User

    @classmethod
    def from_tuple(cls, tp):
        doc, *fields = tp
        names = ("last_name", "first_name", "second_name", "second_name_abbrev",
                 "birth_date", "is_man")
        new_usr_felds = fields[:6]
        existing_usr_felds = fields[6:]
        return cls(
            new=User(**{
                "doc_num": doc,
                **{
                    key: val
                    for key, val in zip(names, new_usr_felds)
                }
            }),
            existing=User(**{
                "doc_num": doc,
                **{
                    key: val
                    for key, val in zip(names, existing_usr_felds)
                }
            }),
        )
