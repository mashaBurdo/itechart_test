import sqlite3


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

    # cur.execute("delete from actors where name='N/A'")
    # cur.execute("delete from writers where name='N/A'")
    # cur.execute("update movies set director = null where director='N/A'")
    # cur.execute("update movies set plot = null where plot='N/A'")
    # cur.execute("update movies set imdb_rating = '0' where imdb_rating='N/A'")
    # cur.execute(
    #     'UPDATE movies SET writers = \'[{"id": "\' || writer || \'"}]\'  where writer != ""'
    # )
    # cur.execute(
    #     "CREATE TABLE IF NOT EXISTS movie_writers (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, movie_id text NOT NULL, writer_id text NOT NULL)"
    # )
    # cur.execute(
    #     "CREATE TABLE IF NOT EXISTS new_movie_actors (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, movie_id text NOT NULL, actor_id text NOT NULL)"
    # )
    # cur.execute("INSERT INTO new_movie_actors (movie_id, actor_id) SELECT movie_id, actor_id FROM movie_actors")
    # cur.execute("DROP TABLE IF EXISTS movie_actors")
    # cur.execute("ALTER TABLE new_movie_actors RENAME TO movie_actors;")

    cur.execute(
        "CREATE TABLE IF NOT EXISTS movie_genres (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, movie_id text NOT NULL, genre_id text NOT NULL)"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS genres (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, genre text NOT NULL)"
    )
    conn.commit()

    genres_data = get_data_from_db("select genre, id  from movies")
    for row in genres_data:
        genres = row['genre'].split(', ')
        for genre in genres:
            genre_exists = get_data_from_db(f"select * from genres where genre='{genre}' limit 1")
            if not genre_exists:
                cur.execute("INSERT INTO genres (genre) VALUES(?)", (genre,))
                conn.commit()
                genre_exists = get_data_from_db(f"select * from genres where genre='{genre}'")
            cur.execute("INSERT INTO movie_genres (genre_id, movie_id) VALUES(?, ?)", (genre_exists[0]['id'], row['id']))
            conn.commit()



    #
    # writers_data = get_data_from_db("select writers, id  from movies")
    # movie_actors = cur.execute("select * from movie_writers limit 1").fetchall()
    # if not movie_actors:
    #     for row in writers_data:
    #         movie_id = row["id"]
    #         writers_str = (
    #             row["writers"]
    #             .replace("[", "")
    #             .replace("]", "")
    #             .replace("}", "")
    #             .replace("{", "")
    #             .replace('"', "")
    #         )
    #         writers_list = [
    #             {e.split(": ")[0]: e.split(": ")[1]} for e in writers_str.split(", ")
    #         ]
    #
    #         cur = conn.cursor()
    #         for writer in writers_list:
    #             data = (movie_id, writer["id"])
    #             cur.execute(
    #                 "INSERT INTO movie_writers(movie_id, writer_id) VALUES(?,?)", data
    #             )
    #         conn.commit()
    #     print("Movie actors updated")



    conn.close()


if __name__ == "__main__":
    make_db_pretty()