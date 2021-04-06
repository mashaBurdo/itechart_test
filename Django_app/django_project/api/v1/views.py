from django.contrib.postgres.aggregates.general import ArrayAgg
from django.core.paginator import Paginator
from django.db.models import F, Q, Case, Value, When, CharField
from django.http import JsonResponse
from django.views.generic.list import BaseListView
from movies.models import FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork


class LolKek(BaseListView):
    def get_queryset(self):
        return Genre.objects.all()

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse({"LoL": "Kek"})


class Movies(BaseListView):
    model = FilmWork
    http_method_names = ["get"]
    paginate_by = 50

    def get_queryset(self):
        film_works = FilmWork.objects.values(
            "id", "title", "description", "rating"
        ).annotate(
            creation_date=F("created_at"),
            genres=ArrayAgg("genres__name", distinct=True),
            actors=ArrayAgg(
                "persons__name",
                distinct=True,
                filter=Q(persons__personfilmwork__role="Actor"),
            ),
            writers=ArrayAgg(
                "persons__name",
                distinct=True,
                filter=Q(persons__personfilmwork__role="Writer"),
            ),
            directors=ArrayAgg(
                "persons__name",
                distinct=True,
                filter=Q(persons__personfilmwork__role="Director"),
            ),
            type=Case(
                When(type="null", then=Value("")),
                default=Value(""),
                output_field=CharField(),
            ),
        )
        return film_works

    def get_context_data(self, object_list=None, **kwargs):
        default_context = super(Movies, self).get_context_data(**kwargs)
        page = default_context["page_obj"].number
        paginator = Paginator(self.get_queryset(), self.paginate_by)
        prev_page = (
            paginator.page(page).previous_page_number()
            if paginator.page(page).has_previous()
            else None
        )
        next_page = (
            paginator.page(page).next_page_number()
            if paginator.page(page).has_next()
            else None
        )

        context = {
            "count": paginator.count,
            "total_pages": paginator.num_pages,
            "prev": prev_page,
            "next": next_page,
            "results": list(paginator.page(page)),
        }
        return context

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MovieByID(BaseListView):
    model = FilmWork
    http_method_names = ["get"]

    def get_queryset(self):
        film_uuid = self.kwargs.get("movie_uuid")
        film = {}
        try:
            film = (
                FilmWork.objects.values("id", "title", "description", "rating")
                .annotate(
                    creation_date=F("created_at"),
                    genres=ArrayAgg("genres__name", distinct=True),
                    actors=ArrayAgg(
                        "persons__name",
                        distinct=True,
                        filter=Q(persons__personfilmwork__role="Actor"),
                    ),
                    writers=ArrayAgg(
                        "persons__name",
                        distinct=True,
                        filter=Q(persons__personfilmwork__role="Writer"),
                    ),
                    directors=ArrayAgg(
                        "persons__name",
                        distinct=True,
                        filter=Q(persons__personfilmwork__role="Director"),
                    ),
                    type=Case(
                        When(type="null", then=Value("")),
                        default=Value(""),
                        output_field=CharField(),
                    ),
                )
                .get(id=film_uuid)
            )
        except Exception as e:
            return {"e": str(e)}
        if not film:
            return {}
        return film

    def get_context_data(self, object_list=None, **kwargs):
        context = self.get_queryset()
        return context

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)
