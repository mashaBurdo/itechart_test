import logging

from elasticsearch.helpers import bulk

from etl_modules.etl_conrstants import ES_INDEX_NAME, ES_INDEX_SCHEMA
from etl_modules.etl_state import State
from etl_modules.backoff_decorator import backoff
from etl_modules.extract_data import get_data


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


# @backoff()
def store_record(bulk_number, elastic_object, start_ind=0, index_name=ES_INDEX_NAME):
    for ind in range(start_ind, bulk_number):
        record = (yield)
        bulk(elastic_object, record, chunk_size=1000, index=index_name)
        elastic_state = State()
        elastic_state.set_state("elastic_ind", ind)


# @backoff()
def continue_from_state(initial_state, bulk_number, limit, es):
    pg_ind = (
        initial_state.get_state("postgres_ind")
        if initial_state.get_state("postgres_ind")
        else 0
    )
    es_ind = initial_state.get_state("elastic_ind")

    if pg_ind != es_ind:
        logging.info("Postgres index is NOT equal to elastic index")
    else:
        logging.info("Postgres index is equal to elastic index")

    logging.info("Continue data transfer")
    sender = store_record(bulk_number, es, pg_ind + 1)
    sender.send(None)
    try:
        get_data(sender, bulk_number, limit, pg_ind + 1)
    except StopIteration:
        logging.info("StopIteration in data transfer")
