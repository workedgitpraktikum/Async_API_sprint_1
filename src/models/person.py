from datetime import date

import orjson
from uuid import UUID
from typing import Optional, List
from pydantic import BaseModel


def orjson_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()


class Person(BaseModel):
    id: UUID
    full_name: str
    birth_date: Optional[date]

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps
