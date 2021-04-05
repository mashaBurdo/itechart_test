import sqlite3
from elasticsearch import Elasticsearch
import time
from elasticsearch.exceptions import ConnectionError

def create_index(es_object, index_name="movies"):
    settings = {
        "settings": {
            "refresh_interval": "1s",
            "analysis": {
                "filter": {
                    "english_stop": {"type": "stop", "stopwords": "_english_"},
                    "english_stemmer": {"type": "stemmer", "language": "english"},
                    "english_possessive_stemmer": {
                        "type": "stemmer",
                        "language": "possessive_english",
                    },
                    "russian_stop": {"type": "stop", "stopwords": "_russian_"},
                    "russian_stemmer": {"type": "stemmer", "language": "russian"},
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
                            "russian_stemmer",
                        ],
                    }
                },
            },
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "id": {"type": "keyword"},
                "imdb_rating": {"type": "float"},
                "genre": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "analyzer": "ru_en",
                    "fields": {"raw": {"type": "keyword"}},
                },
                "description": {"type": "text", "analyzer": "ru_en"},
                "director": {"type": "text", "analyzer": "ru_en"},
                "actors_names": {"type": "text", "analyzer": "ru_en"},
                "writers_names": {"type": "text", "analyzer": "ru_en"},
                "actors": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "id": {"type": "keyword"},
                        "name": {"type": "text", "analyzer": "ru_en"},
                    },
                },
                "writers": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "id": {"type": "keyword"},
                        "name": {"type": "text", "analyzer": "ru_en"},
                    },
                },
            },
        },
    }
    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print("Index created")
            return True
        else:
            print("Index is already created")
            return False
    except Exception as ex:
        print("Index not created:(")
        print(str(ex))
        return False


def store_record(elastic_object, index_name, record):
    is_stored = True
    try:
        elastic_object.index(index=index_name, body=record)
    except Exception as ex:
        print("Error in indexing data")
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


def get_data_from_db(query):
    conn = sqlite3.connect("db.sqlite")
    conn.row_factory = sqlite3.Row
    db = conn.cursor()

    rows = db.execute(query).fetchall()

    conn.commit()
    conn.close()

    return [dict(ix) for ix in rows]


def make_db_pretty():
    conn = sqlite3.connect("db.sqlite")
    cur = conn.cursor()

    cur.execute("delete from actors where name='N/A'")
    cur.execute("delete from writers where name='N/A'")
    cur.execute("update movies set director = null where director='N/A'")
    cur.execute("update movies set plot = null where plot='N/A'")
    cur.execute("update movies set imdb_rating = '0' where imdb_rating='N/A'")
    cur.execute(
        'UPDATE movies SET writers = \'[{"id": "\' || writer || \'"}]\'  where writer != ""'
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS movie_writers (movie_id text NOT NULL, writer_id text NOT NULL)"
    )
    conn.commit()

    writers_data = get_data_from_db("select writers, id  from movies")
    movie_actors = cur.execute("select * from movie_writers limit 1").fetchall()
    if not movie_actors:
        for row in writers_data:
            movie_id = row["id"]
            writers_str = (
                row["writers"]
                .replace("[", "")
                .replace("]", "")
                .replace("}", "")
                .replace("{", "")
                .replace('"', "")
            )
            writers_list = [
                {e.split(": ")[0]: e.split(": ")[1]} for e in writers_str.split(", ")
            ]

            cur = conn.cursor()
            for writer in writers_list:
                data = (movie_id, writer["id"])
                cur.execute(
                    "INSERT INTO movie_writers(movie_id, writer_id) VALUES(?,?)", data
                )
            conn.commit()
        print("Movie actors updated")

    conn.close()


def store_movies(es_obj):
    movies_data = get_data_from_db(
        "select id, imdb_rating, genre, title, plot description,"
        " director from movies"
    )
    for movie_data in movies_data:

        actor_query = f'select a.id, a.name from movie_actors ma join actors a on ma.actor_id = a.id where movie_id=\'{movie_data ["id"]}\''

        actors_data = get_data_from_db(actor_query)

        actors_names = ""
        for actor in actors_data:
            actors_names += actor["name"] + ", "

        writer_query = f'select w.id, w.name from movie_writers mw join writers w on mw.writer_id = w.id where movie_id=\'{movie_data["id"]}\''
        writers_data = get_data_from_db(writer_query)

        writers_names = []
        for writer in writers_data:
            if writer["name"] not in writers_names:
                writers_names.append(writer["name"])
            else:
                writers_data.remove(writer)

        movie_data["actors_names"] = actors_names
        movie_data["actors"] = actors_data
        movie_data["writers_names"] = writers_names
        movie_data["writers"] = writers_data
        movie_data["imdb_rating"] = float(movie_data["imdb_rating"])
        movie_data["genre"] = movie_data["genre"].split(", ")
        if movie_data["director"]:
            movie_data["director"] = movie_data["director"].split(", ")

        store_record(es_obj, "movies", movie_data)


def get_es_film_number(es_object, index_name):
    try:
        test = es_object.search(index=index_name)
        size = test['hits']['total']
        return size['value']
    except Exception as e:
        print("An error occurred while movies counting.", e)
        return 0


if __name__ == "__main__":
    # make_db_pretty()
    es = Elasticsearch(hosts=[{"host": "elasticsearch"}], retry_on_timeout=True)
    for _ in range(100):
        try:
            # make sure the cluster is available
            es.cluster.health(wait_for_status="yellow")
        except ConnectionError:
            print("Couldn't connect to Elasticsearch")
            time.sleep(2)
    result = create_index(es)
    if result:
        films = get_es_film_number(es, 'movies')
        if not films:
            store_movies(es)
            print("Movies stored")
        else:
            print("Movies not stored")
    else:
        print("Movies not stored")

