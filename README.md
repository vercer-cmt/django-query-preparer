# Django Query Preparer

Django Query Preparer (`dqp`) can prepare your queries in the database on application start so they don't need to be parsed and compiled at execution time! This is a postgres specific implementation which targets python 3.7+ and Django 2.2+.

## Usage

### Raw SQL

#### Quickstart

Preparing and executing raw SQL statements is straight forward:

```python
from dqp import execute_stmt, prepare_sql

@prepare_sql
def myquery():
    return "select count(*) from django_migrations;"

execute_stmt(myquery())  # <- note that we call myquery() which gives us the name of the prepared statement.
```

The steps to preparing and executing an SQL query on the fly are:
1. Write a function that takes no arguments and returns a string which is the query to prepare.
2. Decorate this function with the `@prepare_sql` decorator. Note that `@prepare_sql` evaluates the function and prepares the SQL in the database immediately.
3. Call `execute_stmt` and pass the function as the prepared statement name. The decorator changes the output of the function in your scope to return the name of the prepared statement.

You can use placeholder parameters as normal too:

```python
from dqp import execute_stmt, prepare_sql

@prepare_sql
def myquery():
    return "select count(*) from django_migrations where id < %s;"

execute_stmt(myquery(), [10])
```

or

```python
from dqp import execute_stmt, prepare_sql

@prepare_sql
def myquery():
    return "select count(*) from django_migrations where id < %(migration_id)s;"

execute_stmt(myquery(), {"migration_id": 10})
```

#### In Django

It's almost as easy to use prepared SQL statements in a Django app:

1. Add `"dqp.apps.DQPConfig"` to your list of `INSTALLED_APPS`.
2. Again, write a function that takes no arguments and returns one string which is the query to prepare.
3. Decorate this function with the `@register_prepared_sql` decorator.  The decorated functions will be evaluated and prepared in the database when the app receives the `on_ready` signal.
4. Call `execute_stmt` and pass the function as the prepared statement name.

e.g:

```python
from django.http import JsonResponse

from dqp import execute_stmt, register_prepared_sql

@register_prepared_sql
def myquery():
    return "select count(*) from django_migrations;"

def view_migrations(request):
    # migrations will be a list of dicts
    migrations = execute_stmt(myquery())
    return JsonResponse(migrations)
```

### Django ORM queries

#### Basic Usage

You can also prepare queries built using the Django ORM methods.

1. Add `"dqp.apps.DQPConfig"` to your list of `INSTALLED_APPS`.
2. For the model that you want to prepare the query against, add the `PreparedStatementManager`:

```python
from django.db import models

from dqp.manager import PreparedStatementManager

class MyModel(models.Model):
  name = models.CharField(max_length=50, blank=True, null=True)
  alias = models.CharField(max_length=50, blank=True, null=True)

  objects = models.Manager()
  prepare = PreparedStatementManager()  # <-- Here is the PreparedStatementManager!
```

3. Write a function that takes no arguments and returns a query set using the `PreparedStatementManager`. Decorate it with the `register_prepared_qs` decorator:

```python
from dqp import register_prepared_qs

@register_prepared_qs
def get_all_from_my_model():
  return MyModel.prepare.all()
```

4. Call `execute_stmt()`. It will return a `PreparedStatementQuerySet`:

```python
from dqp import execute_stmt
qs = execute_stmt(get_all_from_my_model())

from dqp.queryset import PreparedStatementQuerySet
isinstance(qs, PreparedStatementQuerySet)  # -> True
```

You can use a placeholder when preparing query sets with input arguments:

```python
from dqp import register_prepared_qs, Placeholder

@register_prepared_qs
def get_my_model_lt():
  return MyModel.prepare.filter(id__lt=Placeholder("id"))

execute_stmt(get_my_model_lt(), id=4)
```

You have to name your placeholders and then use those names as keyword arguments to `execute_stmt`. This is because Django can re-order the filters so there's no guarantee that the order in which you specify the filters in the ORM functions will be the order they appear in the executed SQL.

If you want to use lists as inputs you can use `ListPlaceholder`:
```python
from dqp import register_prepared_qs, ListPlaceholder

@register_prepared_qs
def get_my_model_in():
  return MyModel.prepare.filter(id__in=ListPlaceholder("ids"))

execute_stmt(get_my_model_in(), ids=[4, 5])
```

Each placeholder must have a unique name within the same query. You can mix and match constant and passed in parameters:

```python
from dqp import register_prepared_qs, ListPlaceholder

@register_prepared_qs
def get_active_in_range():
  return MyModel.prepare.filter(id__in=ListPlaceholder("ids"), active=True)

execute_stmt(get_active_in_range(), ids=range(10))
```



### Limitations

The following query set methods are not supported when preparing a query nor on a `PreparedStatementQuerySet`:
- `aggregate()`
- `in_bulk()`
- `create()`
- `bulk_create()`
- `bulk_update()`
- `get_or_create()`
- `update_or_create()`
- `delete()`
- `update()`
- `exists()`
- `explain()`

### Prefetch Related
You cannot use `prefetch_related` when preparing a query:

```python
@register_prepared_qs
def get_my_model_lt():
  return MyModel.prepare.filter(id__lt=Placeholder).prefetch_related('related_field')

> PreparedQueryNotSupported: Cannot use prefetch_related when preparing queysets. Add the prefetch related to the returned queryset on statement execution
```

As the error says, you must use the prefetch related AFTER executing the prepared statement:

```python
@register_prepared_qs
def get_my_model_lt():
  return MyModel.prepare.filter(id__lt=Placeholder)

qs = execute_stmt(get_my_model_lt(), [4])
qs = qs.prefetch_related('related_field')
```

The query for the `prefetch_related` will not be a prepared statement. If you need the related data to be prepared and executed as prepared statement then you'll have to write two separate queries and map one to the other manually.

### Differences with ordinary Django usage:
In normal Django usage `get()`, `count()`, `first()`, `last()`, `latest()` and `earliest()` all execute immediately. This doesn't happen for prepared queries: they do not execute immediately. But they will still produce what you expect:

```python
# Normal django usage
MyModel.objects.count()
# > 12

# Prepared statement usage
@prepare_qs
def my_model_count():
  return MyModel.prepare.count()
# > no result - not executed yet

execute_stmt(my_model_count())
# > 12
```

A query prepared using `get()`, `first()`, `last()`, `latest()` and `earliest()` follows the Django behaviour when executed and will return a model instance, not a query set, or it will raise the same exceptions if zero or more than one rows match the filter.

```python
@prepare_qs
def my_model_get():
  return MyModel.prepare.get(id=1)
# > no result - not executed yet

qs = execute_stmt(my_model_get())
isinstance(qs, MyModel)
# > True
```

You can use  `count()`, `first()` and `last()` on a `PreparedStatementQuerySet` but you can't use `get()`, `filter()`, `latest()` or `earliest()`. You'll need to use python's built-in `filter` and `sorted` methods to further filter or order the results of an executed prepared statement.

## Configuration

As noted, add `"dqp.apps.DQPConfig"` to your list of `INSTALLED_APPS`.

You can control whether the registered queries are prepared when the app is ready by setting `DQP_PREPARE_ON_APP_START`. It defaults to `True`. If you do not want queries to be prepared on ap start (perhaps because you haven't run the migrations to create the schema in your database yet or because you're running tests) then set this to `False`.  If `DQP_DB_PREPARE_ON_APP_START` is `False` then you must remember to manually prepare your statements (by calling `PreparedStatementController().prepare_sql_stmt()` or `PreparedStatementController().prepare_qs_stmt()`).

## Using `dqp` in tests

### Unittest

Prepared queries are stored per database session in postgres. But database sessions are restarted between each test by Django. To make sure that all you prepared statements are re-prepared before every test add the `PrepStmtTestMixin` to your test class:

```python
from django.test import TestCase
from dqp.testing import PrepStmtTestMixin

class MyTests(TestCase, PrepStmtTestMixin):
    @classmethod
    def setUp(cls):
      # If you have a setUp method in your test class then you'll need to call super().setUp()
      # to make sure the setUp method of PrepStmtTestMixin is called.
      super().setUp()

    def test_stuff(self):
      # ...
      pass
```

As re-preparing the statements does take a small amount of time you should only use the `PrepStmtTestMixin` in the tests that use prepared queries.

### Pytest

You can use the `prepare_all` function to prepare statements at the start of any test that requires it:

```python
import pytest
from dqp.testing import prepare_all

@pytest.mark.django_db(transaction=True)
def test_stuff():
    prepare_all()
    # testing code...
```
