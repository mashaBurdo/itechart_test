import sqlite3
import json
import logging
from elasticsearch import Elasticsearch


def pretty_db():
    conn = sqlite3.connect('db.sqlite')
    # for row in conn.execute('select id, name from writers where name="N/A" '):
    #     print('Before N/A writers deleting:', row)
    # for row in conn.execute('select id, name from actors where name="N/A" '):
    #     print('Before N/A actors deleting:', row)

    cur = conn.cursor()
    cur.execute("delete from actors where name='N/A'")
    cur.execute("delete from writers where name='N/A'")
    conn.commit()

    # for row in conn.execute('select id, name from writers where name="N/A" '):
    #     print('After N/A writers deleting:', row)
    # for row in conn.execute('select id, name from actors where name="N/A" '):
    #     print('After N/A actors deleting:', row)
    #
    # for row in conn.execute('select title, director from movies where director="N/A"'):
    #     print('Before N/A directors updating:', row)

    cur = conn.cursor()
    cur.execute("update movies set director='None' where director='N/A'")
    conn.commit()

    # for row in conn.execute('select title, director from movies where director="None"'):
    #     print('After N/A directors updating to None:', row)

    conn.close()


def get_json_from_db(query):
    conn = sqlite3.connect('db.sqlite')
    conn.row_factory = sqlite3.Row
    db = conn.cursor()

    rows = db.execute(query).fetchall()

    conn.commit()
    conn.close()

    return [dict(ix) for ix in rows]



def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Connected')
    else:
        print('Not connected:(')
    return _es


def create_index(es_object, index_name='movies'):
    created = False
    settings = {
      "settings": {
        "refresh_interval": "1s",
        "analysis": {
          "filter": {
            "english_stop": {
              "type":       "stop",
              "stopwords":  "_english_"
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
              "type":       "stop",
              "stopwords":  "_russian_"
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
          "title": {
            "type": "text",
            "analyzer": "ru_en",
            "fields": {
              "raw": {
                "type":  "keyword"
              }
            }
          },
          "description": {
            "type": "text",
            "analyzer": "ru_en"
          },
          "director": {
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
    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, ignore=400, body=settings)
        print('Index created')
        created = True
    except Exception as ex:
        print('Index not created:(')
        print(str(ex))
    finally:
        return created


def store_record(elastic_object, index_name, record):
    is_stored = True
    try:
        outcome = elastic_object.index(index=index_name, body=record)
        # print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    pretty_db()
    # es_obj = connect_elasticsearch()
    a = create_index(es_obj)


    # actors_table_data = get_json_from_db('select id, name from actors')
    # for row in actors_table_data:
    #     out = store_record(es_obj, 'actors', row)

    # writers_table_data = get_json_from_db('select id, name from writers')
    # for row in writers_table_data:
    #     out = store_record(es_obj, 'writers', row)