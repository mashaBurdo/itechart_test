
from django.db.models.signals import post_save, pre_delete, post_delete
from elasticsearch import Elasticsearch
from movies.models import (FilmWork, Genre, GenreFilmWork, Person,
                           PersonFilmWork)

from django.dispatch import receiver

from django.contrib.postgres.aggregates.general import ArrayAgg
from django.core.paginator import Paginator
from django.db.models import F, Q, Case, Value, When, CharField


ES_HOST = 'elasticsearch'


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
    es_obj = Elasticsearch(hosts=[{"host": ES_HOST}], retry_on_timeout=True)
    es_obj.update_by_query(body=query, index='movies')


post_save.connect(genre_film_work_save, GenreFilmWork)


def film_work_save(sender, instance, **kwargs):
    record = {
        "id": instance.id,
        "title": instance.title,
        "imdb_rating": instance.rating,
        "description": instance.description,
        "genre": [],
    }

    es_obj = Elasticsearch(hosts=[{"host": ES_HOST}], retry_on_timeout=True)
    es_obj.index(index="movies", body=record)



post_save.connect(film_work_save, FilmWork)


def film_work_delete(sender, instance, **kwargs):
    id = instance.id

    es_obj = Elasticsearch(hosts=[{"host": ES_HOST}], retry_on_timeout=True)
    es_obj.delete_by_query(index="movies", body={"query": {"match": {"id": id}}})


pre_delete.connect(film_work_delete, FilmWork)


def genre_film_work_delete(sender, instance, **kwargs):
    film_id = instance.film_work.id
    try:
        result = FilmWork.objects.values("id").annotate(
                        genres=ArrayAgg("genres__name", distinct=True),
                        ).get(id=film_id)
        genres = result['genres'] if result['genres'] != [None] else []
    except Exception as e:
        genres = []
        print(e)

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
    es_obj = Elasticsearch(hosts=[{"host": ES_HOST}], retry_on_timeout=True)
    try:
        es_obj.update_by_query(body=query, index='movies')
    except:
        print('Conflict in index updating occurred because of multiple deletions. Genres deleted successfully')


post_delete.connect(genre_film_work_delete, GenreFilmWork)


def person_film_work_signal(sender, instance, **kwargs):
    film_id = instance.film_work.id
    result = FilmWork.objects.values("id").annotate(
            actors=ArrayAgg(
                "persons__name",
                distinct=True,
                filter=Q(personfilmwork__role="Actor"),
            ),
            writers=ArrayAgg(
                "persons__name",
                distinct=True,
                filter=Q(personfilmwork__role="Writer"),
            ),
            directors=ArrayAgg(
                "persons__name",
                distinct=True,
                filter=Q(personfilmwork__role="Director"),
            ),
        ).get(id=film_id)
    actors = result['actors'] if result['actors'] != [None] else []
    writers = result['writers'] if result['writers'] != [None] else []
    actors_names = ', '.join(actors)
    writers_names = ', '.join(writers)
    director = result['directors'] if result['directors'] != [None] else []
    query = {
         "script": {
                "inline": "ctx._source.director = params.director; ctx._source.actors_names = params.actors_names; ctx._source.writers_names = params.writers_names;",
                "lang": "painless",
                "params": {
                    "director": director,
                    "actors_names": actors_names,
                    "writers_names": writers_names,
                }
         }
    }
    es_obj = Elasticsearch(hosts=[{"host": ES_HOST}], retry_on_timeout=True)
    es_obj.update_by_query(body=query, index='movies')


post_save.connect(person_film_work_signal, PersonFilmWork)
pre_delete.connect(person_film_work_signal, PersonFilmWork)
