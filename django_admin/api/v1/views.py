from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q
from django.http import JsonResponse
from django.views.generic.list import BaseListView
from django.db.models import F, UUIDField
from django.db.models import OuterRef, Subquery
import math
import uuid
from movies.models import FilmWork, Person, PersonFilmWork, Genre, GenreFilmWork
from django.db.models import Exists, OuterRef
from django.core.paginator import Paginator
from django.core.paginator import EmptyPage
from django.core.paginator import PageNotAnInteger



class Movies(BaseListView):
    model = FilmWork
    http_method_names = ["get"]
    paginate_by = 50

    def get_queryset(self):
        page = int(self.request.GET.get('page'))
        pag_by = self.paginate_by
        if not page:
            page = 1
        if page == 'last':
            count = FilmWork.objects.all().count()
            page = math.ceil(count/pag_by)
        film_works = FilmWork.objects.values(
            "id", "title", "description", "rating", "type"
        )[pag_by*(page-1):pag_by*page].annotate(creation_date=F("created_at"))
        cnt = 1
        for film in film_works:
            genres_data = GenreFilmWork.objects.filter(film_work_id=film["id"]).values('genre__name')
            persons_data = PersonFilmWork.objects.filter(film_work_id=film["id"]).values('person__name', 'role')
            film['genres'] = [genre['genre__name'] for genre in genres_data]
            film['actors'] = [person['person__name'] for person in persons_data if person['role'] == 'Actor']
            film['directors'] = [person['person__name'] for person in persons_data if person['role'] == 'Director']
            film['writers'] = [person['person__name'] for person in persons_data if person['role'] == 'Writer']
            # cnt += 1
        return film_works

    def get_context_data(self, *, object_list=None, **kwargs):
        page = int(self.request.GET.get('page'))
        pag_by = self.paginate_by
        count = FilmWork.objects.all().count()
        total = math.ceil(count/pag_by)
        context = {
            # "page": page,
            # "pag_by": pag_by,
            "count": count,
            "total_pages": total,
            "prev": page - 1 if page - 1 > 0 else "null",
            "next":  page + 1 if page + 1 <= total else "null",
            "results": list(self.get_queryset())
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
