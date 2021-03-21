FROM python:3.8.5-slim-buster

RUN pip install --upgrade pip

COPY ./requirements.txt .
#RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev pip install psycopg2
RUN pip install -r requirements.txt

COPY ./django_project /app/

WORKDIR /app

COPY ./entrypoint.sh /
ENTRYPOINT ["sh", "/entrypoint.sh"]

