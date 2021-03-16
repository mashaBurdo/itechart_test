from django.core.validators import MinValueValidator
from django.contrib.auth.models import User
from django.db import models
from django.utils.translation import gettext_lazy as _
from model_utils.models import TimeStampedModel


class Genre(TimeStampedModel):
    name = models.CharField(_("name"), max_length=255)
    description = models.TextField(_("description"), blank=True, null=True)

    class Meta:
        verbose_name = _("genre")

    def __str__(self):
        return self.name


class FilmWorkType(TimeStampedModel):
    name = models.CharField(_("name"), max_length=255)

    class Meta:
        verbose_name = _("film work type")

    def __str__(self):
        return self.name


class FilmWork(TimeStampedModel):
    type = models.ForeignKey(
        FilmWorkType,
        verbose_name=_("film work type"),
        related_name="films",
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
    )
    title = models.CharField(_("title"), max_length=255)
    description = models.TextField(_("description"), blank=True, null=True)
    creation_date = models.DateField(_("creation date"), blank=True, null=True)
    certificate = models.TextField(_("certificate"), blank=True, null=True)
    file_path = models.FileField(
        _("file path"), upload_to="film_works/", blank=True, null=True
    )
    rating = models.FloatField(
        _("rating"), validators=[MinValueValidator(0)], blank=True, null=True
    )
    genres = models.ManyToManyField(Genre)

    class Meta:
        verbose_name = _("film work")

    def __str__(self):
        return self.title


class Person(TimeStampedModel):
    name = models.CharField(_("name"), max_length=255)
    surname = models.CharField(_("surname"), max_length=255)
    films = models.ManyToManyField(FilmWork, through="PersonRole")

    class Meta:
        verbose_name = _("person")

    def __str__(self):
        return self.name + " " + self.surname


class Role(models.TextChoices):
    ACTOR = "actor", _("actor")
    WRITER = "writer", _("writer")
    DIRECTOR = "director", _("director")


class PersonRole(models.Model):
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    film_work = models.ForeignKey(FilmWork, on_delete=models.CASCADE)
    role = models.TextField(_("role"), choices=Role.choices)

    def __str__(self):
        return self.person.__str__()
