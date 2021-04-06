import logging
import sqlite3
import uuid

import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from pg_table_create_queries import create_queries

CONN_SQLITE = sqlite3.connect("db.sqlite")

CONN_PG = psycopg2.connect(
    dbname="movies",
    user="postgres",
    password="123",
    # host="localhost",
    host="db",
    port=5432,
)


def insert_batch_pg(query, values_list, conn_pg=CONN_PG):
    with conn_pg.cursor() as cur:
        execute_batch(cur, query, values_list, page_size=5000)
        conn_pg.commit()


def get_data_from_pg(query, conn_pg=CONN_PG):
    with conn_pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
        return [dict(row) for row in rows]


def get_data_from_pg_with_data(query, data, conn_pg=CONN_PG):
    with conn_pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, data)
        rows = cur.fetchall()
        return [dict(row) for row in rows]


def alter_pg(query, conn_pg=CONN_PG):
    with conn_pg.cursor() as cur:
        cur.execute(query)
        conn_pg.commit()


def create_database_and_tables(queries):
    for query in queries:
        alter_pg(query)


def get_data_from_sqlite(query, conn_sqlite=CONN_SQLITE):
    conn_sqlite.row_factory = sqlite3.Row
    cur = conn_sqlite.cursor()
    rows = cur.execute(query).fetchall()
    return [dict(ix) for ix in rows]


def insert_film_works_and_directors():
    """This function transfers movie and director data from sqlite database to postgres.
    All this data is taken from the movies table in sqlite.
    In sqlite directors were stored in the directors column of the movies table. In postgres data for all
    persons (including directors) will be stored in a separate person table.
    The person table in postgres is linked to the film_work table through the person_film_work intermediate table
    (there is a column indicating the person role).
    Also, this function is the first step towards solving the id problem. In sqlite id's are of different formats.
    But in postgres id are in uuid format. The film_works postgres table stores the old movie id in the old_id column.
    This is done for the subsequent successful connection of the film_works with genres and persons tables.
    Directors duplications in persons table are prevented.
    """
    movies_data = get_data_from_sqlite(
        "SELECT director, title, description, rating, id FROM movies"
    )
    film_work = []
    person = []
    person_film_works = []
    for movie_data in movies_data:
        movie_uuid = str(uuid.uuid4())
        film_work.append(
            (
                movie_uuid,
                movie_data["title"],
                movie_data["description"],
                movie_data["rating"],
                movie_data["id"],
            )
        )
        director_uuid = str(uuid.uuid4())
        if movie_data["director"]:
            director_exists = False
            for director in person:
                if movie_data["director"] == director[1]:
                    director_exists = True
                    director_uuid = director[0]
            if not director_exists:
                person.append((director_uuid, movie_data["director"]))
            person_film_works.append(
                (str(uuid.uuid4()), movie_uuid, director_uuid, "Director")
            )

    insert_batch_pg(
        "INSERT INTO film_work (id, title, description, rating, old_id) VALUES (%s, %s, %s, %s, %s)",
        film_work,
    )
    insert_batch_pg("INSERT INTO person (id, name) VALUES (%s, %s)", person)
    insert_batch_pg(
        "INSERT INTO person_film_work (id, film_work_id, person_id, role) VALUES (%s, %s, %s, %s)",
        person_film_works,
    )


def insert_genre_and_genre_film_work():
    genres_data = get_data_from_sqlite("SELECT id, genre FROM genres")
    genres = [(str(uuid.uuid4()), genre["genre"], genre["id"]) for genre in genres_data]
    insert_batch_pg("INSERT INTO genre (id, name, old_id) VALUES (%s, %s, %s)", genres)

    movie_genres_data = get_data_from_sqlite(
        "SELECT id, movie_id, genre_id FROM movie_genres"
    )
    genre_film_work = []
    for movie_genre_data in movie_genres_data:
        sqlite_movie_id = movie_genre_data["movie_id"]
        pg_film_work_id = get_data_from_pg_with_data(
            "SELECT id FROM film_work WHERE old_id = %(mi)s", {"mi": sqlite_movie_id}
        )[0]["id"]
        sqlite_genre_id = movie_genre_data["genre_id"]
        pg_genre_id = get_data_from_pg_with_data(
            "SELECT id FROM genre WHERE old_id = %(gi)s", {"gi": sqlite_genre_id}
        )[0]["id"]
        genre_film_work.append((str(uuid.uuid4()), pg_film_work_id, pg_genre_id))

    insert_batch_pg(
        "INSERT INTO genre_film_work (id, film_work_id, genre_id) VALUES (%s, %s, %s)",
        genre_film_work,
    )


def insert_actors_into_person_and_person_film_work():
    actors_data = get_data_from_sqlite("SELECT id, name FROM actors")
    actors = []
    for actor in actors_data:
        person_exists = get_data_from_pg_with_data(
            "SELECT * FROM person WHERE name = %(an)s", {"an": actor["name"]}
        )
        if not person_exists:
            actors.append((str(uuid.uuid4()), actor["name"]))
    insert_batch_pg("INSERT INTO person (id, name) VALUES (%s, %s)", actors)

    movie_actors_data = get_data_from_sqlite(
        "SELECT ma.id, a.name, ma.movie_id, ma.actor_id FROM movie_actors ma JOIN actors a ON a.id = ma.actor_id"
    )
    person_film_work = []
    for movie_actor_data in movie_actors_data:
        sqlite_movie_id = movie_actor_data["movie_id"]
        pg_film_work_id = get_data_from_pg(
            "SELECT id FROM film_work WHERE old_id = '%s'" % sqlite_movie_id
        )
        pg_actor_id = get_data_from_pg_with_data(
            "SELECT id FROM person WHERE name = %(ma)s",
            {"ma": movie_actor_data["name"]},
        )
        if pg_actor_id and pg_film_work_id:
            person_film_work.append(
                (
                    str(uuid.uuid4()),
                    pg_film_work_id[0]["id"],
                    pg_actor_id[0]["id"],
                    "Actor",
                )
            )
    insert_batch_pg(
        "INSERT INTO person_film_work (id, film_work_id, person_id, role) VALUES (%s, %s, %s, %s)",
        person_film_work,
    )


def insert_writers_into_person_and_person_film_work():
    writers_data = get_data_from_sqlite("SELECT id, name FROM writers")
    writers = []
    for writer in writers_data:
        query_name = writer["name"].replace("'", "''")
        person_exists = get_data_from_pg(
            "SELECT * FROM person WHERE name = '%s'" % query_name
        )
        if not person_exists:
            writers.append((str(uuid.uuid4()), writer["name"]))
    insert_batch_pg("INSERT INTO person (id, name) VALUES (%s, %s)", writers)

    movie_writers_data = get_data_from_sqlite(
        "SELECT mw.id, w.name, mw.movie_id, mw.writer_id FROM movie_writers mw JOIN writers w ON w.id = mw.writer_id"
    )
    person_film_work = []
    for movie_writer_data in movie_writers_data:
        sqlite_movie_id = movie_writer_data["movie_id"]
        pg_film_work_id = get_data_from_pg_with_data(
            "SELECT id FROM film_work WHERE old_id = %(mi)s", {"mi": sqlite_movie_id}
        )
        pg_writer_id = get_data_from_pg_with_data(
            "SELECT id FROM person WHERE name = %(mw)s",
            {"mw": movie_writer_data["name"]},
        )
        if pg_writer_id and pg_film_work_id:
            person_film_work.append(
                (
                    str(uuid.uuid4()),
                    pg_film_work_id[0]["id"],
                    pg_writer_id[0]["id"],
                    "Writer",
                )
            )
    insert_batch_pg(
        "INSERT INTO person_film_work (id, film_work_id, person_id, role) VALUES (%s, %s, %s, %s)",
        person_film_work,
    )


def drop_old_id_columns():
    alter_pg("ALTER TABLE film_work DROP COLUMN old_id")
    alter_pg("ALTER TABLE genre DROP COLUMN old_id")


def tests():
    director_lucas = get_data_from_pg(
        """SELECT pf.role, COUNT(f.title) FROM person p INNER JOIN person_film_work pf ON p.id = pf.person_id
        INNER JOIN film_work f ON f.id = pf.film_work_id where p.name ='George Lucas' GROUP BY pf.role"""
    )
    print(
        "FILMS WITH GEORGE LUCAS. ACTOR 6, DIRECTOR 5, WRITER 45 EXPECTED. GOT",
        director_lucas,
        end="\n\n",
    )
    news_genre = get_data_from_pg(
        """SELECT f.title FROM film_work f INNER JOIN genre_film_work gf ON f.id = gf.film_work_id 
        INNER JOIN genre g ON gf.genre_id = g.id WHERE g.name='News'"""
    )
    print("NEWS GENRE. 5 FILMS EXPECTED. GOT", len(news_genre), news_genre, end="\n\n")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    table_exists = get_data_from_pg(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE table_name='film_work'"
    )
    need_data_transfer = False if table_exists else True

    if need_data_transfer:
        try:
            logging.info("Starting data transfer")
            create_database_and_tables(create_queries)
            insert_film_works_and_directors()
            insert_genre_and_genre_film_work()
            insert_actors_into_person_and_person_film_work()
            insert_writers_into_person_and_person_film_work()
            drop_old_id_columns()
            tests()
            logging.info("Data transfer completed")
        except:
            logging.error("Data transfer failed", exc_info=True)
        finally:
            CONN_SQLITE.close()
            CONN_PG.close()
            logging.info("Connection is closed")
    else:
        logging.info("No data transfer needed")