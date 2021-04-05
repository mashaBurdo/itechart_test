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

from etl_modules.extract_data import (
    get_es_film_number,
    get_data_from_pg,
    get_data_from_pg_with_data,
    get_film_number,
    get_data,
)


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
