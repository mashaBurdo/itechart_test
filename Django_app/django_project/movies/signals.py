
from django.db.models.signals import post_save, pre_delete
from elasticsearch import Elasticsearch
from movies.models import (FilmWork, Genre, GenreFilmWork, Person,
                           PersonFilmWork)

from django.dispatch import receiver

from django.contrib.postgres.aggregates.general import ArrayAgg
from django.core.paginator import Paginator
from django.db.models import F, Q, Case, Value, When, CharField


ES_HOST = 'localhost'

def genre_film_work_save(sender, instance, **kwargs):
    film_id = instance.film_work.id
    genres = FilmWork.objects.values("id").annotate(
                    genres=ArrayAgg("genres__name", distinct=True),
                    ).get(id=film_id)['genres']
    script = f"ctx._source.genre = {genres}"
    query = {
         "script": {
            "source": script,
            "lang": "painless"
         },
         "query": {
            "match": {
                "id": film_id
            }
         }
    }
    print(query)
    es_obj = Elasticsearch(hosts=[{"host": ES_HOST}], retry_on_timeout=True)
    es_obj.update_by_query(body=query, index='movies')


post_save.connect(genre_film_work_save, GenreFilmWork)


def post_film_work_save(sender, instance, **kwargs):
    record = {
        "id": instance.id,
        "title": instance.title,
        "imdb_rating": instance.rating,
        "description": instance.description,
        "genre": [],
    }

    es_obj = Elasticsearch(hosts=[{"host": ES_HOST}], retry_on_timeout=True)
    es_obj.index(index="movies", body=record)


post_save.connect(post_film_work_save, FilmWork)


def pre_film_work_delete(sender, instance, **kwargs):
    id = instance.id

    es_obj = Elasticsearch(hosts=[{"host": ES_HOST}], retry_on_timeout=True)
    es_obj.delete_by_query(index="movies", body={"query": {"match": {"id": id}}})


pre_delete.connect(pre_film_work_delete, FilmWork)
