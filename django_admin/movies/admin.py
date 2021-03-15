from django.contrib import admin
from movies.models import FilmWork, FilmWorkType, Genre, Person, PersonRole

admin.site.register(FilmWork)
admin.site.register(FilmWorkType)
admin.site.register(Genre)
admin.site.register(Person)
admin.site.register(PersonRole)
