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

from extract_data import (
    get_es_film_number,
    get_data_from_pg,
    get_data_from_pg_with_data,
    get_film_number,
)
from extract_data import (
    get_es_film_number,
    get_data_from_pg,
    get_data_from_pg_with_data,
    get_film_number,
)


@backoff()
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


@backoff()
def store_record(record, ind, elastic_object, index_name=ES_INDEX_NAME):
    is_stored = True
    try:
        bulk(elastic_object, record, chunk_size=1000, index=index_name)

        elastic_state = State()
        elastic_state.set_state("elastic_ind", ind)
    except Exception as ex:
        print("Error in indexing data")
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


@backoff()
def continue_from_state(initial_state, bulk_number, limit, es):
    pg_ind = initial_state.get_state("postgres_ind")
    es_ind = initial_state.get_state("elastic_ind")

    if pg_ind != es_ind:
        logging.info("Start with storage data to es")
        data = get_data(limit, pg_ind)
        store_record(data, pg_ind, es)
    else:
        logging.info("Start from getting fresh data from pg")

    for i in range(pg_ind + 1, bulk_number):
        data = get_data(limit, i)
        store_record(data, i)
