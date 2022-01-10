from http import HTTPStatus
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from uuid import UUID

from services.genre import get_genre_service, GenreService

router = APIRouter()


class Genre(BaseModel):
    uuid: UUID
    name: str


@router.get('/<uuid:UUID>/', response_model=Genre)
async def genre_details(genre_id: str, genre_service: GenreService = Depends(get_genre_service)) -> Genre:
    genre = await genre_service.genre_detail(genre_id)
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genre not found')

    return Genre(uuid=genre.id, name=genre.name)


@router.get('/')
async def genre_main(genre_service: GenreService = Depends(get_genre_service)) -> List[Genre]:
    genres = await genre_service.genre_main()
    if not genres:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genre not found')
    genres_list = [Genre(uuid=x.id, name=x.name) for x in genres]
    return genres_list
