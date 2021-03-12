import random
import uuid
import psycopg2
from psycopg2.extras import execute_batch

conn = psycopg2.connect(
    dbname="movies",
    user="postgres",
    password="123",
    host="localhost",
    port=5432,
    options="-c search_path=content",
)
cur = conn.cursor()
cur.execute("SELECT id FROM film_work")
film_works_ids = []
for data in cur.fetchall():
    film_works_ids.append(data[0])
persons_ids = [str(uuid.uuid4()) for _ in range(600_000)]
print("insert persons")
execute_batch(
    cur,
    "INSERT INTO person (id) VALUES (%s)",
    [(i,) for i in persons_ids],
    page_size=5000,
)
conn.commit()
print("persons has been inserted")
film_work_person_data = []
for film_work_id in film_works_ids:
    for person_id in random.sample(persons_ids, 5):
        film_work_person_data.append((str(uuid.uuid4()), film_work_id, person_id))

print("insert relations")
execute_batch(
    cur,
    "INSERT INTO person_film_work (id, film_work_id, person_id) VALUES (%s, %s, %s)",
    film_work_person_data,
    page_size=5000,
)
conn.commit()
cur.close()
conn.close()
