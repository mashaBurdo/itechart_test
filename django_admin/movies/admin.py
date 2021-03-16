from django.contrib import admin
from movies.models import FilmWork, Genre, Person, PersonFilmWork, GenreFilmWork


class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmWork
    extra = 0

    def get_queryset(self, request):
        qs = super(PersonFilmWorkInline, self).get_queryset(request)
        return qs.select_related('person')
#
#
class GenreFilmWorkInline(admin.TabularInline):
    model = GenreFilmWork
    extra = 0
#
#
@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    list_display = ('title', 'type', 'created_at', 'rating')
    list_filter = ('type',)
    search_fields = ('title', 'description', 'id')
    fields = (
        'title', 'type', 'description', 'certificate',
        'file_path', 'rating', 'created_at', 'updated_at', 'id',)
    readonly_fields = ('created_at', 'updated_at', 'id',)
    inlines = [GenreFilmWorkInline]
#
#
@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ('name',)
    fields = ('name', 'created_at', 'updated_at', 'id',)
    readonly_fields = ('created_at', 'updated_at', 'id',)
    inlines = [PersonFilmWorkInline]
#
#
@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', 'description')
    search_fields = ('name', 'description')
    fields = ('name', 'description', 'created_at', 'updated_at', 'id',)
    readonly_fields = ('created_at', 'updated_at', 'id',)

#
@admin.register(PersonFilmWork)
class PersonRoleAdmin(admin.ModelAdmin):
    list_display = ('person', 'film_work', 'role')
    list_filter = ('role',)
    search_fields = ('person__name', 'film_work__title')
    fields = ('person', 'film_work', 'role', 'created_at', 'id',)
    readonly_fields = ('created_at', 'id',)
#
#
@admin.register(GenreFilmWork)
class GenreFilmWorkAdmin(admin.ModelAdmin):
    list_display = ('genre', 'film_work',)
    list_filter = ('genre',)
    search_fields = ('genre__name', 'film_work__title',)
    fields = ('genre', 'film_work', 'created_at', 'id',)
    readonly_fields = ('created_at', 'id',)

