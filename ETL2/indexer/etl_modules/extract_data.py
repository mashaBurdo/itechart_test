import logging
from math import ceil
import time

import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from psycopg2.extras import RealDictCursor

from etl_modules.etl_conrstants import CONN_PG, ES_HOST, ES_INDEX_NAME, ES_INDEX_SCHEMA
from etl_modules.etl_state import State
from etl_modules.backoff_decorator import backoff
from elasticsearch.exceptions import ConnectionError


# @backoff()
def get_es_film_number(es_object, index_name='movies'):
    try:
        test = es_object.search(index=index_name)
        size = test["hits"]["total"]
        return size["value"]
    except Exception as e:
        logging.error("An error occurred while movies counting.", exc_info=True)
        return 0


def get_data_from_pg(query, conn_pg=CONN_PG):
    with conn_pg.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
        return [dict(row) for row in rows]


def get_data_from_pg_with_data(query, data, conn_pg=CONN_PG):
    with conn_pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, data)
        rows = cur.fetchall()
        return [dict(row) for row in rows]


@backoff()
def get_film_number():
    return get_data_from_pg("SELECT COUNT(*) FROM film_work")[0]["count"]


# @backoff()
def get_data(target, bulk_number, limit, start_ind=0):
    for ind in range(start_ind, bulk_number):
        film_works_data = get_data_from_pg_with_data(
            "SELECT id, rating imdb_rating, title, description FROM film_work LIMIT %(l)s OFFSET %(o)s",
            {"l": limit, "o": limit * ind},
        )

        for film_work in film_works_data:

            if film_work["imdb_rating"]:
                film_work["imdb_rating"] = float(film_work["imdb_rating"])

            person_query = "SELECT p.id, p.name, pf.role FROM person_film_work pf JOIN person p ON pf.person_id = p.id WHERE film_work_id=%(fw)s"
            person_data = get_data_from_pg_with_data(person_query, {"fw": film_work["id"]})
            film_work["actors"] = [
                {"id": a["id"], "name": a["name"]}
                for a in person_data
                if a["role"] == "Actor"
            ]
            actors_names = [a["name"] for a in person_data if a["role"] == "Actor"]
            film_work["actors_names"] = ", ".join(map(str, actors_names))
            film_work["writers"] = [
                {"id": a["id"], "name": a["name"]}
                for a in person_data
                if a["role"] == "Writer"
            ]
            writers_names = [a["name"] for a in person_data if a["role"] == "Writer"]
            film_work["writers_names"] = ", ".join(map(str, writers_names))
            film_work["director"] = [
                a["name"] for a in person_data if a["role"] == "Director"
            ]

            genre_query = "SELECT g.name FROM genre_film_work gf JOIN genre g ON gf.genre_id = g.id WHERE film_work_id=%(fw)s"
            genre_data = get_data_from_pg_with_data(genre_query, {"fw": film_work["id"]})
            film_work["genre"] = [g["name"] for g in genre_data]

        postgres_state = State()
        postgres_state.set_state("postgres_ind", ind)

        target.send(film_works_data)


