FROM docker.elastic.co/elasticsearch/elasticsearch:7.7.0
FROM python:3.8-alpine

RUN adduser -D movies_app

COPY . /app

WORKDIR /app
RUN pip install -r requirements.txt

EXPOSE 9200
CMD ["python", "ETL_mechanism.py", "-h", "0.0.0.0", "-p", "9200"]