from django.contrib import admin
from movies.models import FilmWork, Genre, Person, PersonFilmWork, GenreFilmWork

admin.site.register(FilmWork)
admin.site.register(Person)
admin.site.register(PersonFilmWork)
admin.site.register(Genre)
admin.site.register(GenreFilmWork)

# class PersonRoleInline(admin.TabularInline):
#     model = PersonRole
#     extra = 0
#
#
# @admin.register(FilmWork)
# class FilmWorkAdmin(admin.ModelAdmin):
#     list_display = ('title', 'type', 'creation_date', 'rating')
#     list_filter = ('type',)
#     search_fields = ('title', 'description', 'id')
#     fields = (
#         'title', 'type', 'description', 'creation_date', 'certificate',
#         'file_path', 'rating', 'genres')
#     inlines = [PersonRoleInline]
#
#
# @admin.register(Person)
# class PersonAdmin(admin.ModelAdmin):
#     list_display = ('surname', 'name')
#     search_fields = ('surname', 'name')
#     fields = ('surname', 'name')
#     inlines = [PersonRoleInline]
#
#
# @admin.register(PersonRole)
# class PersonRoleAdmin(admin.ModelAdmin):
#     list_display = ('person', 'film_work', 'role')
#     list_filter = ('role',)
#     search_fields = ('person', 'film_work')
#
#
# class FilmWorkGenreInline(admin.TabularInline):
#     model = FilmWork.genres.through
#     extra = 0
#
#
# @admin.register(Genre)
# class GenreAdmin(admin.ModelAdmin):
#     list_display = ('name', 'description')
#     search_fields = ('name', 'description')
#     inlines = [FilmWorkGenreInline]
#
#
# class FilmWorkInline(admin.TabularInline):
#     model = FilmWork
#     fields = ('title', 'genres')
#     extra = 0
#
#
# @admin.register(FilmWorkType)
# class FilmWorkTypeAdmin(admin.ModelAdmin):
#     list_filter = ('name',)
#     inlines = [FilmWorkInline]
#
