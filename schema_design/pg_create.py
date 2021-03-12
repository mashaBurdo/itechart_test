import random
import uuid
import psycopg2
from psycopg2.extras import execute_batch

conn = psycopg2.connect(
    dbname="movies",
    user="postgres",
    password = '123',
    host="localhost",
    port=5432,
    options="-c search_path=content",
)
cur = conn.cursor()
cur.execute("SELECT id FROM film_work")
film_works_ids = []
for data in cur.fetchall():
    film_works_ids.append(data[0])
genres = ["comedy", "horror", "action", "drama"]
print("insert genres")
execute_batch(
    cur,
    "INSERT INTO genre_film_work (id, film_work_id, genre) VALUES (%s, %s, %s)",
    [
        (str(uuid.uuid4()), film_work_id, random.choice(genres))
        for film_work_id in film_works_ids
    ],
    page_size=5000,
)
conn.commit()
cur.close()
conn.close()
