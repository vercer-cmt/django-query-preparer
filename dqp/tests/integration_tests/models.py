from django.db import models

from dqp.manager import PreparedStatementManager

# Fake models used only in the tests

class Species(models.Model):
    name = models.CharField(max_length=50)

    objects = models.Manager()
    prepare = PreparedStatementManager()

class Animal(models.Model):
    name = models.CharField(max_length=50)

    species = models.ForeignKey(Species, on_delete=models.CASCADE)
