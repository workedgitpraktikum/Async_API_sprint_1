import logging
from datetime import datetime
from typing import List

from postgres_to_es.load import ESLoader
from postgres_to_es.transform import _transform_records
from pydantic_config import POSTGRES_BATCH_SIZE, POSTGRES_BATCH_SIZE_GENRE, INDEX_NAME_MOVIE, INDEX_NAME_GENRE, INDEX_NAME_PERSON

logger = logging.getLogger()


class StatePostgres:
    def __init__(self, pg_cursor, es_loader: ESLoader):
        self.pg_cursor = pg_cursor
        self.es_loader = es_loader
        self.batch_size = POSTGRES_BATCH_SIZE

    def extract_data(self, table: str, updated_state: datetime):
        if table == 'content.genre':
            sql_query = f"""SELECT id, name, description
                                FROM {table}
                                WHERE updated_at > '{updated_state}'
                                ORDER BY updated_at
                                LIMIT {self.batch_size}; """

            self.pg_cursor.execute(sql_query)
            records = [dict(record) for record in self.pg_cursor]
            self.es_loader.load_to_es(records, INDEX_NAME_GENRE)
        if table == 'content.person':
            sql_query = f"""SELECT id, full_name, birth_date
                                FROM {table}
                                WHERE updated_at > '{updated_state}'
                                ORDER BY updated_at
                                LIMIT {self.batch_size}; """

            self.pg_cursor.execute(sql_query)
            records = [dict(record) for record in self.pg_cursor]
            self.es_loader.load_to_es(records, INDEX_NAME_PERSON)

    def extract_records_ids(self, table: str, updated_state: datetime):
        if table == 'content.genre':
            self.batch_size = POSTGRES_BATCH_SIZE_GENRE
        sql_query = f"""SELECT id, updated_at
                        FROM {table}
                        WHERE updated_at > '{updated_state}'
                        ORDER BY updated_at
                        LIMIT {self.batch_size}; """
        self.pg_cursor.execute(sql_query)
        ids_records = self.pg_cursor.fetchall()
        if not ids_records:
            return updated_state, []
        last_updated_state = ids_records[-1][1]
        required_records_ids = [x[0] for x in ids_records]

        if table == 'content.person' or table == 'content.genre':
            if table == 'content.person':
                join_table = 'content.person_film_work'
                field = 'person_id'
                self.extract_data('content.person', updated_state)
            elif table == 'content.genre':
                join_table = 'content.genre_film_work'
                field = 'genre_id'
                self.extract_data('content.genre', updated_state)
            else:
                join_table = None
                field = None

            id_query = "'" + "','".join(s for s in required_records_ids) + "'"
            sql_query = f"""SELECT DISTINCT fw.id, fw.updated_at
                            FROM content.film_work fw
                            LEFT JOIN {join_table} jt ON jt.film_work_id = fw.id
                            WHERE jt.{field} IN ({id_query})
                            ORDER BY fw.updated_at; """
            self.pg_cursor.execute(sql_query)
            films = self.pg_cursor.fetchall()
            required_films_id = [x[0] for x in films]
        else:
            required_films_id = required_records_ids

        return last_updated_state, required_films_id

    def extract_postgres_data(self, required_films_id: List[str]):
        id_query = "'" + "','".join(s for s in required_films_id) + "'"
        sql_query = f"""SELECT
                            fw.id as id, 
                            fw.title, 
                            fw.description, 
                            fw.rating as imdb_rating, 
                            CASE
                                WHEN pfw.person_role = 'actor' 
                                THEN ARRAY_AGG(distinct p.full_name || ' id:' || p.id)
                            END actors,
                            CASE
                                WHEN pfw.person_role = 'writer' 
                                THEN ARRAY_AGG(distinct p.full_name || ' id:' || p.id)
                            END writers,
                            CASE
                                WHEN pfw.person_role = 'director' 
                                THEN ARRAY_AGG(distinct p.full_name || ' id:' || p.id)
                            END AS directors,
                            ARRAY_AGG(distinct g.name || ' id:' || g.id) genre

                        FROM content.film_work fw
                        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                        LEFT JOIN content.person p ON p.id = pfw.person_id
                        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                        LEFT JOIN content.genre g ON g.id = gfw.genre_id
                        WHERE fw.id IN ({id_query})
                        GROUP BY
                            fw.id,
                            fw.title, 
                            fw.description, 
                            fw.rating, 
                            fw.type,
                            pfw.person_role; """
        self.pg_cursor.execute(sql_query)

        logger.info('Success extract data from Postgres.')

        records = [dict(record) for record in self.pg_cursor]
        transformed_records = _transform_records(records)

        self.es_loader.load_to_es(transformed_records, INDEX_NAME_MOVIE)


