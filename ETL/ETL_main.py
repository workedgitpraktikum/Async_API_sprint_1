import logging
from datetime import datetime
from time import sleep

import psycopg2
from psycopg2.extras import DictCursor
import elasticsearch

from postgres_to_es.ETL_run import etl_run, create_index_movies, create_index_genres, create_index_persons
from pydantic_config import config, SLEEP_TIME, INDEX_NAME_MOVIE, INDEX_NAME_GENRE, INDEX_NAME_PERSON


def start_etl():
    """
    Запускаем бесконечный цикл поиска изменений и запуска ETL-процессов.
    :return:
    """
    es = elasticsearch.Elasticsearch(hosts='localhost', port='9200', timeout=360)
    es.ping()
    if not es.indices.exists(index=INDEX_NAME_MOVIE):
        create_index_movies(es)
    if not es.indices.exists(index=INDEX_NAME_GENRE):
        create_index_genres(es)
    if not es.indices.exists(index=INDEX_NAME_PERSON):
        create_index_persons(es)

    while True:
        logging.info(f'Start ETL: {datetime.now()}')
        dsl = dict(config.film_work_pg.dsn)
        try:

            with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
                etl_run(pg_conn.cursor())
        except (psycopg2.OperationalError,
                elasticsearch.exceptions.ConnectionError):
            pass
        logging.info(f'ETL sleeping {SLEEP_TIME} seconds')
        sleep(SLEEP_TIME)


if __name__ == '__main__':
    start_etl()
