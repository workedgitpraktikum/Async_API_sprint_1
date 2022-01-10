from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from uuid import UUID
from typing import List, Optional, Tuple

from services.person import PersonService, get_person_service

router = APIRouter()


class Person(BaseModel):
    uuid: UUID
    full_name: str
    role: Optional[str]
    film_ids: Optional[List[str]]


@router.get('/search/')
async def person_details(
        query: str,
        page_size: int = Query(50, alias="page[size]"),
        page_number: int = Query(1, alias="page[number]"),
        person_service: PersonService = Depends(get_person_service)) -> Tuple[Person, Optional[List[dict]]]:

    person = await person_service.person_search(query, page_size, page_number)

    return person


@router.get('/<uuid:UUID>/')
async def person_details(
        person_id: str,
        person_service: PersonService = Depends(get_person_service)) -> Tuple[Person, Optional[List[dict]]]:

    person = await person_service.person_detail(person_id)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')
    return person
