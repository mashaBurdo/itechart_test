
from django.db.models.signals import post_save, pre_delete
from elasticsearch import Elasticsearch
from movies.models import (FilmWork, Genre, GenreFilmWork, Person,
                           PersonFilmWork)


def post_film_work_save(sender, instance, **kwargs):
    record = {
        "id": instance.id,
        "title": instance.title,
        "imdb_rating": instance.rating,
        "description": instance.description,
    }

    es_obj = Elasticsearch(hosts=[{"host": "elasticsearch"}], retry_on_timeout=True)
    es_obj.index(index="movies", body=record)


post_save.connect(post_film_work_save, FilmWork)


def pre_film_work_delete(sender, instance, **kwargs):
    id = instance.id

    es_obj = Elasticsearch(hosts=[{"host": "elasticsearch"}], retry_on_timeout=True)
    es_obj.delete_by_query(index="movies", body={"query": {"match": {"id": id}}})


pre_delete.connect(pre_film_work_delete, FilmWork)