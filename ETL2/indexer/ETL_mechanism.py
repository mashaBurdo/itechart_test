""" Техническое задание

В предыдущем модуле мы реализовывали механизм для для полнотекстового поиска.
Теперь улучшим его: научим его работать с новой схемой и оптимизируем количество элементов
для обновления.

Подсказки к выполнению задания по ETL

Перед тем как вы приступите к выполнению задания по теме ETL, мы дадим вам несколько полезных
подсказок:

1. Прежде чем выполнять задание, подумайте, сколько ETL-процессов вам нужно.
2. Для валидации конфига советуем использовать pydantic.
3. Для построения ETL-процесса используйте корутины.
4. Чтобы спокойно переживать падения Postgres или Elasticsearch, используйте решение с
техникой backoff или попробуйте использовать одноимённую библиотеку.
5. Ваше приложение должно уметь восстанавливать контекст и начинать читать с того места,
где оно закончило свою работу.
6. При конфигурировании ETL-процесса подумайте, какие параметры нужны для запуска приложения.
Старайтесь оставлять в коде как можно меньше «магических» значений.
7. Желательно, но необязательно сделать составление запросов в БД максимально обобщённым,
чтобы не пришлось постоянно дублировать код. При обобщении не забывайте о том, что все
передаваемые значения в запросах должны экранироваться.
8. Использование тайпингов поможет сократить время дебага и повысить понимание кода ревьюерами,
а значит работы будут проверяться быстрее :)
9. Обязательно пишите, что делают функции в коде.
10. Для логирования используйте модуль `logging` из стандартной библиотеки Python.

Желаем вам удачи в написании ETL. Вы обязательно справитесь

Решение задачи залейте в папку postgres_to_es вашего репозитория."""

import logging
from math import ceil
import time

import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from psycopg2.extras import RealDictCursor

from etl_conrstants import CONN_PG, ES_HOST, ES_INDEX_NAME, ES_INDEX_SCHEMA
from etl_modules.etl_state import State
from etl_modules.backoff_decorator import backoff
from elasticsearch.exceptions import ConnectionError


@backoff()
def connect_elasticrearch(hostname: str):
    try:
        es_obj = Elasticsearch(hosts=[{"host": hostname}], retry_on_timeout=True)

        for _ in range(100):
            try:
                # make sure the cluster is available
                es_obj.cluster.health(wait_for_status="yellow")
            except ConnectionError:
                logging.error("Couldn't connect to Elasticsearch", exc_info=True)
                time.sleep(2)
        return es_obj
    except Exception as e:
        raise Exception(str(e))


ES_OBJ = connect_elasticrearch(ES_HOST)


@backoff()
def create_index(es_object=ES_OBJ, index_name=ES_INDEX_NAME):
    created = False
    index = ES_INDEX_SCHEMA
    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, ignore=400, body=index)
            logging.info("Index created successfully.")
        else:
            logging.info("Index was already created.")
        created = True
    except:
        logging.error("Index wasn't created.", exc_info=True)
    finally:
        return created


@backoff()
def get_es_film_number(es_object=ES_OBJ, index_name=ES_INDEX_NAME):
    try:
        test = es_object.search(index=index_name)
        size = test['hits']['total']
        return size['value']
    except:
        logging.error("An error occurred while movies counting.", exc_info=True)


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
    film_number = get_data_from_pg("SELECT COUNT(*) FROM film_work")
    # print(film_number)
    return get_data_from_pg("SELECT COUNT(*) FROM film_work")[0]["count"]


@backoff()
def get_data(limit, ind):
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

    last_record = film_works_data
    postgres_state = State()
    postgres_state.set_state("postgres_last_record", last_record)
    postgres_state.set_state("postgres_ind", ind)

    return film_works_data


@backoff()
def store_record(record, ind, elastic_object=ES_OBJ, index_name=ES_INDEX_NAME):
    is_stored = True
    try:
        bulk(elastic_object, record, chunk_size=1000, index=index_name)

        last_record = record
        elastic_state = State()
        elastic_state.set_state("elastic_last_record", last_record)
        elastic_state.set_state("elastic_ind", ind)
    except Exception as ex:
        print("Error in indexing data")
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


@backoff()
def continue_from_state(initial_state, bulk_number):
    pg_ind = initial_state.get_state("postgres_ind")
    es_ind = initial_state.get_state("elastic_ind")

    if pg_ind != es_ind:
        logging.info("Start with storage data to es")
        store_record(initial_state.get_state("postgres_last_record"), pg_ind)
    else:
        logging.info("Start from getting fresh data from pg")

    for i in range(pg_ind + 1, bulk_number):
        data = get_data(limit, i)
        store_record(data, i)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    create_index()

    limit = 100
    film_number = get_film_number()
    film_number = film_number if film_number else 0
    bulk_number = ceil(film_number / limit)

    es_film_number = get_es_film_number()

    if es_film_number == 0:
        logging.info("Beginning data transfer")
        for i in range(bulk_number):
            data = get_data(limit, i)
            store_record(data, i)
    elif film_number - es_film_number > limit:
        print(es_film_number, film_number)
        logging.info("Continuing data transfer")
        initial_state = State()
        continue_from_state(initial_state, bulk_number)
    elif es_film_number == film_number:
        logging.info("Data were already transferred")




    final_state = State()
    # logging.info(final_state.state)
    final_state.clear_state()
    # logging.info(final_state.state)
