To start Flask application and ETL1 (transfer data about movies from sqlite to elasticsearch) 
run the following commands. That will run Flask application on port 8000 and elasticsearch on port 9200. 
If index "movies" wasn't created it will be created. If data about movies
weren't transferred from sqlite to elasticsearch they will be transferred (indexer_etl1 container).
Flask depends on contains Elasticsearch, Flask and script for ETL.

    docker-compose up --build --attach-dependencies flask

To start Django application, ETL2(transfer data about movies from postgres to elasticsearch) and data transfer 
from sqlite  to postgres run the following commands. That will run Django application on port 8000, elasticsearch on port 9200,
postgres on port 5432, redis on port 6379 and nginx on port 80.
If data about movies weren't transferred from sqlite to postgres they will be transferred (sqlite_to_pg_data_transfer container).
If data about movies weren't transferred from sqlite to elasticsearch they will be transferred (indexer_etl2 container).
Django_gunicorn depends on Elasticsearch, Redis, Django, Gunicorn, Postgres, Nginx, script for data transfer and script for ETL.

    docker-compose up --build --attach-dependencies django_gunicorn

If the following error occurred,

    ProgrammingError: relation "django_session" does not exist

Run the following commands. 

    docker ps
    docker exec -it <django_gunicorn CONTAINER ID> bash
    python manage.py makemigrations
    python manage.py migrate
    python manage.py createsuperuser

If you want to run Flask application, ETL1, Django application 
and data transfer from sqlite  to postgres run the following commands.
Docker-compose contains Elasticsearch, Redis, Django, Gunicorn, Postgres, Nginx, Flask, script for data transfer and script for ETL (first one).

    docker-compose  build
    docker-compose  up

This is a combination of the above commands with several nuances:
- Flask app will be run on 5000 port
- ETL2(transfer data about movies from postgres to elasticsearch) will be skipped


    

    


