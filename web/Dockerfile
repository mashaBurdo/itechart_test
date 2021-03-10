FROM python:3.8-alpine

RUN adduser -D movies_app

COPY . /app

WORKDIR /app
RUN pip install -r requirements.txt
HEALTHCHECK CMD curl -fsSL "http://127.0.0.1:9200/_cat/health?h=status" | grep -E '^green'
EXPOSE 8000
CMD ["flask", "run", "-h", "0.0.0.0", "-p", "8000"]




