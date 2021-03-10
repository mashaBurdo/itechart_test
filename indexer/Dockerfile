FROM python:3.6-slim
WORKDIR /app
ADD . /app
RUN pip install -r requirements.txt
HEALTHCHECK CMD curl -fsSL "http://127.0.0.1:9200/_cat/health?h=status" | grep -E '^green'
ENTRYPOINT [ "python" ]
CMD [ "ETL_mechanism.py" ]