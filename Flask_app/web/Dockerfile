FROM python:3.8-alpine

RUN adduser -D movies_app

COPY . /app

WORKDIR /app
RUN pip install -r requirements.txt
EXPOSE 8000
ENTRYPOINT ["python"]
CMD ["app.py"]
#CMD ["flask", "run"]

