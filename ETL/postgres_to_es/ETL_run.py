import logging
from datetime import datetime

import elasticsearch
import psycopg2
import backoff
from psycopg2.extensions import connection as _connection

from postgres_to_es.extract import StatePostgres
from postgres_to_es.load import ESLoader
from postgres_to_es.state import State, JsonFileStorage
from pydantic_config import config, INDEX_NAME_MOVIE, INDEX_NAME_GENRE, INDEX_NAME_PERSON

logger = logging.getLogger()


@backoff.on_exception(backoff.expo, (elasticsearch.exceptions.ConnectionError, psycopg2.OperationalError), max_time=10)
def etl_run(cursor: _connection):
    logger.info('Start ETL')
    state_postgres = StatePostgres(cursor, ESLoader(config.elastic_connection.url_elastic))
    state = State(JsonFileStorage(config.film_work_pg.state_file_path))

    updated_person = state.get_state('person')
    if updated_person is None:
        logger.info('person state is None. Create state.')
        updated_person = datetime.min
        states = state.storage.retrieve_state()
        states['person'] = str(updated_person)
        state.storage.save_state(states)

    updated_movie = state.get_state('movie')
    if updated_movie is None:
        logger.info('movie state is None. Create state.')
        updated_movie = datetime.min
        states = state.storage.retrieve_state()
        states['movie'] = str(updated_movie)
        state.storage.save_state(states)

    updated_genre = state.get_state('genre')
    if updated_genre is None:
        logger.info('genre state is None. Create state.')
        updated_genre = datetime.min
        states = state.storage.retrieve_state()
        states['genre'] = str(updated_genre)
        state.storage.save_state(states)

    logger.info('Success get state. Start extract.')

    updated_state_person, required_ids_person = state_postgres.extract_records_ids('content.person', updated_person)
    updated_state_movie, required_ids_movie = state_postgres.extract_records_ids('content.film_work', updated_movie)
    updated_state_genre, required_ids_genre = state_postgres.extract_records_ids('content.genre', updated_genre)

    all_required_ids = {*required_ids_person, *required_ids_movie, *required_ids_genre}

    state_postgres.extract_postgres_data(list(all_required_ids))

    state.set_state('person', updated_state_person)
    state.set_state('movie', updated_state_movie)
    state.set_state('genre', updated_state_genre)


def create_index_movies(es: elasticsearch.Elasticsearch):
    index_movies = {
        "settings": {
            "refresh_interval": "1s",
            "analysis": {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
                    },
                    "english_possessive_stemmer": {
                        "type": "stemmer",
                        "language": "possessive_english"
                    },
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "russian_stemmer": {
                        "type": "stemmer",
                        "language": "russian"
                    }
                },
                "analyzer": {
                    "ru_en": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "english_stop",
                            "english_stemmer",
                            "english_possessive_stemmer",
                            "russian_stop",
                            "russian_stemmer"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "id": {
                    "type": "keyword"
                },
                "imdb_rating": {
                    "type": "float"
                },
                "genre": {
                    "type": "keyword"
                },
                "genres": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "text",
                            "analyzer": "ru_en"
                        }
                    }
                },
                "title": {
                    "type": "text",
                    "analyzer": "ru_en",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    }
                },
                "description": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "directors_names": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "actors_names": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "writers_names": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "directors": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "text",
                            "analyzer": "ru_en"
                        }
                    }
                },
                "actors": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "text",
                            "analyzer": "ru_en"
                        }
                    }
                },
                "writers": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "text",
                            "analyzer": "ru_en"
                        }
                    }
                }
            }
        }
    }

    es.indices.create(index=INDEX_NAME_MOVIE, body=index_movies)


def create_index_genres(es: elasticsearch.Elasticsearch):
    index_genres = {
        "settings": {
            "refresh_interval": "1s",
            "analysis": {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
                    },
                    "english_possessive_stemmer": {
                        "type": "stemmer",
                        "language": "possessive_english"
                    },
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "russian_stemmer": {
                        "type": "stemmer",
                        "language": "russian"
                    }
                },
                "analyzer": {
                    "ru_en": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "english_stop",
                            "english_stemmer",
                            "english_possessive_stemmer",
                            "russian_stop",
                            "russian_stemmer"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "id": {
                    "type": "keyword"
                },
                "name": {
                    "type": "keyword"
                },
                "description": {
                    "type": "text",
                    "analyzer": "ru_en"
                }
            }
        }
    }

    es.indices.create(index=INDEX_NAME_GENRE, body=index_genres)


def create_index_persons(es: elasticsearch.Elasticsearch):
    index_persons = {
        "settings": {
            "refresh_interval": "1s",
            "analysis": {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
                    },
                    "english_possessive_stemmer": {
                        "type": "stemmer",
                        "language": "possessive_english"
                    },
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "russian_stemmer": {
                        "type": "stemmer",
                        "language": "russian"
                    }
                },
                "analyzer": {
                    "ru_en": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "english_stop",
                            "english_stemmer",
                            "english_possessive_stemmer",
                            "russian_stop",
                            "russian_stemmer"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "id": {
                    "type": "keyword"
                },
                "full_name": {
                    "type": "text"
                },
                "birth_date": {
                    "type": "date"
                }
            }
        }
    }

    es.indices.create(index=INDEX_NAME_PERSON, body=index_persons)
