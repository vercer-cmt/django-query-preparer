# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

from django.db import models

from dqp.manager import PreparedStatementManager


# Models used by the tests
class Species(models.Model):
    name = models.CharField(max_length=50)

    objects = models.Manager()
    prepare = PreparedStatementManager()


class Animal(models.Model):
    name = models.CharField(max_length=50)

    species = models.ForeignKey(Species, on_delete=models.CASCADE)

    objects = models.Manager()
    prepare = PreparedStatementManager()


class Items(models.Model):
    description = models.CharField(max_length=50)
    animal = models.ForeignKey(Animal, on_delete=models.CASCADE)

    objects = models.Manager()
    prepare = PreparedStatementManager()
