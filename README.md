To start Django application, ETL2(transfer data about movies from postgres to elasticsearch) and data transfer 
from sqlite  to postgres run the following command. That will run Django application on port 8000, elasticsearch on port 9200,
postgres on port 5432, redis on port 6379 and nginx on port 80.
If data about movies weren't transferred from sqlite to postgres they will be transferred (sqlite_to_pg_data_transfer container).
If data about movies weren't transferred from sqlite to elasticsearch they will be transferred (indexer_etl2 container).
Django_gunicorn depends on Elasticsearch, Redis, Django, Gunicorn, Postgres, Nginx, script for data transfer and script for ETL.

    docker-compose build django_gunicorn
    docker-compose up django_gunicorn 

If the following error occurred,

    ProgrammingError: relation "django_session" does not exist

Run the following commands. 

    docker ps
    docker exec -it <django_gunicorn CONTAINER ID> bash
    python manage.py migrate
    python manage.py createsuperuser

To start Flask application and ETL1 container(transfer data about movies from sqlite to elasticsearch) 
run the following command. That will run Flask application on port 5000 and elasticsearch on port 9200. 
If index "movies" wasn't created it will be created. If data about movies
weren't transferred from sqlite to elasticsearch they will be transferred (indexer_etl1 container).
Flask depends on contains Elasticsearch, Flask and script for ETL.

    docker-compose build flask
    docker-compose up flask

If you want to run ETL_mechanism of ETL1, you have to execute the following commands:

    docker ps
    docker exec -it <indexer_etl1  CONTAINER ID> bash
    python ETL_mechanism.py 


If you want to run all the services run the following commands.
Docker-compose contains Elasticsearch, Redis, Django, Gunicorn, Postgres, Nginx, Flask, script for data transfer and ETL2.

    docker-compose  build
    docker-compose  up


    

    


