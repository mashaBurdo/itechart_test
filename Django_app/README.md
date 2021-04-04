    docker-compose -f django-etl2-docker-compose.yml build
    docker-compose -f django-etl2-docker-compose.yml up


ProgrammingError: relation "django_session" does not exist

    docker ps
    docker exec -it <django_gunicorn CONTAINER ID> bash
    python manage.py makemigrations
    python manage.py migrate
    python manage.py createsuperuser
