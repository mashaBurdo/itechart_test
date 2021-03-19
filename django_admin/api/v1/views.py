import math
import uuid

from django.contrib.postgres.aggregates import ArrayAgg
from django.core.paginator import EmptyPage, PageNotAnInteger, Paginator
from django.db.models import Exists, F, OuterRef, Q, Subquery, UUIDField
from django.http import JsonResponse
from django.views.generic.list import BaseListView

from movies.models import (FilmWork, Genre, GenreFilmWork, Person,
                           PersonFilmWork)


class Movies(BaseListView):
    model = FilmWork
    http_method_names = ["get"]
    paginate_by = 50

    def get_queryset(self):
        film_works = FilmWork.objects.values(
            "id", "title", "description", "rating", "type"
        ).annotate(creation_date=F("created_at"))
        for film in film_works:
            genres_data = GenreFilmWork.objects.filter(film_work_id=film["id"]).values('genre__name')
            persons_data = PersonFilmWork.objects.filter(film_work_id=film["id"]).values('person__name', 'role')
            film['genres'] = [genre['genre__name'] for genre in genres_data]
            film['actors'] = [person['person__name'] for person in persons_data if person['role'] == 'Actor']
            film['directors'] = [person['person__name'] for person in persons_data if person['role'] == 'Director']
            film['writers'] = [person['person__name'] for person in persons_data if person['role'] == 'Writer']
        return film_works

    def get_context_data(self, *, object_list=None, **kwargs):
        context = super(Movies, self).get_context_data(**kwargs)
        paginator = Paginator(self.get_queryset(), self.paginate_by)
        page = context['page_obj'].number

        context = {
            # "page": page,
            "count": paginator.count,
            "total_pages": paginator.num_pages,
            'prev': paginator.page(page).previous_page_number() if paginator.page(page).has_previous() else 'null',
            "next":  paginator.page(page).next_page_number() if paginator.page(page).has_next() else 'null',
            "results": list(paginator.page(page))
        }
        return context

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MovieByID(BaseListView):
    model = FilmWork
    http_method_names = ["get"]

    def get_queryset(self):
        film_uuid = self.kwargs.get('movie_uuid')
        film = {}
        try:
            film = FilmWork.objects.filter(id=film_uuid).values(
                "id", "title", "description", "rating", "type"
            ).annotate(creation_date=F("created_at")).first()
        except Exception as e:
            print('LOOOOO', e)
            return {'e': str(e)}
        if not film:
            return {}
        genres_data = GenreFilmWork.objects.filter(film_work_id=film["id"]).values('genre__name')
        persons_data = PersonFilmWork.objects.filter(film_work_id=film["id"]).values('person__name', 'role')
        film['genres'] = [genre['genre__name'] for genre in genres_data]
        film['actors'] = [person['person__name'] for person in persons_data if person['role'] == 'Actor']
        film['directors'] = [person['person__name'] for person in persons_data if person['role'] == 'Director']
        film['writers'] = [person['person__name'] for person in persons_data if person['role'] == 'Writer']
        return film

    def get_context_data(self, *, object_list=None, **kwargs):
        context = self.get_queryset()
        return context

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)
