from typing import Optional, List
from pydantic import BaseModel


class DSNSettings(BaseModel):
    host: str
    port: int
    dbname: str
    password: str
    user: str
    options: str


class PostgresSettings(BaseModel):
    dsn: DSNSettings
    limit: Optional[int]
    order_field: List[str]
    state_field: List[str]
    fetch_delay: Optional[float]
    state_file_path: Optional[str]


class ElasticSettings(BaseModel):
    url_elastic: str


class Config(BaseModel):
    film_work_pg: PostgresSettings
    elastic_connection: ElasticSettings


config = Config.parse_file('config.json')

SLEEP_TIME = 10
POSTGRES_BATCH_SIZE = 100
POSTGRES_BATCH_SIZE_GENRE = 5
INDEX_NAME_MOVIE = 'movies'
INDEX_NAME_GENRE = 'genres'
INDEX_NAME_PERSON = 'persons'

