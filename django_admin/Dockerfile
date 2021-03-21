FROM python:3.8.5-slim-buster

RUN pip install --upgrade pip

COPY ./requirements.txt .
RUN pip install -r requirements.txt

COPY ./django_project /app/

WORKDIR /app

CMD ["python", "manage.py", "migrate", "--no-input"]
CMD ["python", "manage.py", "collectstatic", "--no-input"]

CMD ["gunicorn" , "-b", "0.0.0.0:8000", "django_project.wsgi:application", "--reload"]
