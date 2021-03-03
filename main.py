import sqlite3
import json
import logging
from elasticsearch import Elasticsearch


def get_json_from_db(query):
    conn = sqlite3.connect('db.sqlite')
    conn.row_factory = sqlite3.Row
    db = conn.cursor()

    rows = db.execute(query).fetchall()

    conn.commit()
    conn.close()

    return [dict(ix) for ix in rows]


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

    cur = conn.cursor()
    cur.execute("update movies set imdb_rating='0' where imdb_rating='N/A'")
    conn.commit()

    # for row in conn.execute('select title, director from movies where director="None"'):
    #     print('After N/A directors updating to None:', row)

    cur = conn.cursor()
    cur.execute('UPDATE movies SET writers = \'[{"id": "\' || writer || \'"}]\'  where writer != ""')
    conn.commit()


    cur = conn.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS movie_writers (movie_id text NOT NULL, writer_id text NOT NULL)')
    conn.commit()

    writers_data = get_json_from_db('select writers, id  from movies')
    for row  in writers_data:
        movie_id = row["id"]
        writers_str = row["writers"].replace('[','').replace(']','').replace('}','').replace('{','').replace('"','')
        writers_list = [ {e.split(': ')[0]: e.split(': ')[1]} for e in writers_str.split(', ')]

        for writer in writers_list:
            data = (movie_id, writer["id"])
            cur = conn.cursor()
            cur.execute('INSERT INTO movie_writers(movie_id, writer_id) VALUES(?,?)', data)
            conn.commit()



    conn.close()

    print('DB is pretty')


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
    es_obj = connect_elasticsearch()
    a = create_index(es_obj)

    pretty_json = get_json_from_db('select id, imdb_rating, genre, title, plot description,'
                                   ' director from movies')
    for movie_data  in pretty_json:
        actor_query = f'select a.id, a.name from movie_actors ma join actors a on ma.actor_id = a.id where movie_id=\'{movie_data ["id"]}\''
        actors_data = get_json_from_db(actor_query)

        actors_names = ''
        for actor in actors_data:
            actors_names += actor['name']+', '

        writer_query = f'select w.id, w.name from movie_writers mw join writers w on mw.writer_id = w.id where movie_id=\'{movie_data["id"]}\''
        writers_data = get_json_from_db(writer_query)

        writers_names = ''
        for writer in writers_data:
            writers_names += writer['name'] + ', '

        movie_data['actors_names'] = actors_names
        movie_data['actors'] = actors_data
        movie_data['writers_names'] = writers_names
        movie_data['writers'] = writers_data
        movie_data['imdb_rating'] = float(movie_data['imdb_rating'] )

        # print(movie_data , end='\n\n')

        out = store_record(es_obj, 'movies', movie_data)
