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
import time

from elasticsearch import Elasticsearch
import psycopg2
from psycopg2.extras import RealDictCursor

from etl_conrstants import ES_HOST, ES_INDEX_NAME, ES_INDEX_SCHEMA, CONN_PG
from etl_modules.backoff_decorator import backoff


@backoff()
def connect_elasticrearch(hostname: str):
    try:
        es_obj = Elasticsearch(hosts=[{"host": hostname}], retry_on_timeout=True)
        es_obj.cluster.health(wait_for_status="yellow")
        return es_obj
    except Exception as e:
        raise Exception(str(e))


def create_index(es_object, index_name=ES_INDEX_NAME):
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


# @backoff()
def get_data_from_pg(query, conn_pg=CONN_PG):
    with conn_pg.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
        return [dict(row) for row in rows]


def store_movies(es_obj):
    film_works_data = get_data_from_pg(
        "SELECT id, rating imdb_rating, title, description FROM film_work"
    )

    # print(film_works_data[0])
    for film_work in film_works_data:

        if film_work["imdb_rating"]:
            film_work["imdb_rating"] = float(film_work["imdb_rating"])
        person_query = f'SELECT p.id, p.name, pf.role FROM person_film_work pf JOIN person p ON pf.person_id = p.id WHERE film_work_id=\'{film_work["id"]}\''
        person_data = get_data_from_pg(person_query)
        film_work["actors"] = [{"id": a["id"], "name": a["name"]} for a in person_data if a["role"] == "Actor"]
        actors_names = [a["name"] for a in person_data if a["role"] == "Actor"]
        film_work["actors_names"] = ', '.join(map(str, actors_names))
        film_work["writers"] = [{"id": a["id"], "name": a["name"]} for a in person_data if a["role"] == "Writer"]
        writers_names = [a["name"] for a in person_data if a["role"] == "Writer"]
        film_work["writers_names"] = ', '.join(map(str, writers_names))
        film_work["director"] = [a["name"] for a in person_data if a["role"] == "Director"]

        genre_query = f'SELECT g.name FROM genre_film_work gf JOIN genre g ON gf.genre_id = g.id WHERE film_work_id=\'{film_work["id"]}\''
        genre_data = get_data_from_pg(genre_query)
        film_work["genre"] = [g['name'] for g in genre_data]

        store_record(es_obj, film_work)


def store_record(elastic_object, record, index_name=ES_INDEX_NAME):
    is_stored = True
    try:
        elastic_object.index(index=index_name, body=record)
    except Exception as ex:
        print("Error in indexing data")
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    es = connect_elasticrearch(ES_HOST)
    create_index(es)
    store_movies(es) # this
