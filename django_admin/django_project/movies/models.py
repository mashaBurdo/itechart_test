import uuid

from django.contrib.auth.models import User
from django.core.validators import MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _
from model_utils.fields import AutoCreatedField, AutoLastModifiedField
from model_utils.models import TimeStampedModel


class Genre(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(_("name"), max_length=255)
    description = models.TextField(_("description"), null=True, blank=True)
    created_at = AutoCreatedField(_("created"))
    updated_at = AutoLastModifiedField(_("modified"))

    class Meta:
        managed = False
        db_table = "genre"
        verbose_name = _("genre")

    def __str__(self):
        return self.name


class Person(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(_("name"), max_length=255)
    created_at = AutoCreatedField(_("created"))
    updated_at = AutoLastModifiedField(_("modified"))

    class Meta:
        managed = False
        db_table = "person"
        verbose_name = _("person")
        indexes = [models.Index(fields=["name"])]

    def __str__(self):
        return self.name


class Type(models.TextChoices):
    SERIAL = "Serial", _("Serial")
    FILM = "Film", _("Film")


class FilmWork(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    title = models.CharField(_("title"), max_length=255)
    description = models.TextField(_("description"), null=True, blank=True)
    created_at = AutoCreatedField(_("created"))
    updated_at = AutoLastModifiedField(_("modified"))
    certificate = models.CharField(
        _("certificate"), max_length=255, null=True, blank=True
    )
    file_path = models.FileField(
        _("file path"), upload_to="film_works/", null=True, blank=True
    )
    rating = models.FloatField(
        _("rating"), validators=[MinValueValidator(0)], null=True, blank=True
    )
    type = models.TextField(_("type"), choices=Type.choices)
    genres = models.ManyToManyField(Genre, through='GenreFilmWork')
    persons = models.ManyToManyField(Person, through='PersonFilmWork')

    class Meta:
        # managed = False
        db_table = "film_work"
        verbose_name = _("film work")
        indexes = [models.Index(fields=["title"])]

    def __str__(self):
        return self.title


class GenreFilmWork(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    film_work = models.ForeignKey(FilmWork, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)
    created_at = AutoCreatedField(_("created"))

    class Meta:
        managed = False
        db_table = "genre_film_work"
        verbose_name = _("genre film work")
        indexes = [models.Index(fields=["genre", "film_work"])]

    def __str__(self):
        return str(self.genre)


class Role(models.TextChoices):
    ACTOR = "Actor", _("Actor")
    WRITER = "Writer", _("Writer")
    DIRECTOR = "Director", _("Director")


class PersonFilmWork(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    film_work = models.ForeignKey(FilmWork, on_delete=models.CASCADE)
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    role = models.TextField(_("role"), choices=Role.choices)
    created_at = AutoCreatedField(_("created"))

    class Meta:
        managed = False
        db_table = "person_film_work"
        verbose_name = _("person film work")
        indexes = [models.Index(fields=["person", "film_work"])]

    def __str__(self):
        return str(self.person)
