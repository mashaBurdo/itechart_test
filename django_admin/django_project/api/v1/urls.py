from api.v1 import views
from django.urls import path

urlpatterns = [
    path("lol/", views.LolKek.as_view()),
    path("movies/", views.Movies.as_view()),
    path("movies/<uuid:movie_uuid>/", views.MovieByID.as_view()),
]
