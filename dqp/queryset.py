# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

from collections import namedtuple

from django.db.models.query import QuerySet

from dqp.constants import Placeholder
from dqp.exceptions import (
    PreparedQueryNotSupported,
    CannotAlterPreparedStatementQuerySet,
    PreparedStatementNotYetExecuted,
)
from dqp.query import PreparedSQLQuery, PreparedStmtQuery


class PreparedQuerySetBase(QuerySet):
    # Methods that need to be implemented by inheriting classes
    def _fetch_all(self):
        raise NotImplementedError

    def count(self):
        raise NotImplementedError

    def get(self, *args, **kwargs):
        raise NotImplementedError

    def first(self):
        raise NotImplementedError

    def last(self):
        raise NotImplementedError

    # Methods that are not available for prepared querysets

    NotSupportedMessage = (
        "Cannot use {} with prepared querysets. Please use an ordinary ORM queryset or use SQL for the prepared query."
    )

    def aggregate(self, *args, **kwargs):
        raise PreparedQueryNotSupported(self.NotSupportedMessage.format("aggregate"))

    def in_bulk(self, *args, **kwargs):
        raise PreparedQueryNotSupported(self.NotSupportedMessage.format("in_bulk"))

    def create(self, *args, **kwargs):
        raise PreparedQueryNotSupported(self.NotSupportedMessage.format("create"))

    def bulk_create(self, *args, **kwargs):
        raise PreparedQueryNotSupported(self.NotSupportedMessage.format("bulk_create"))

    def bulk_update(self, *args, **kwargs):
        raise PreparedQueryNotSupported(self.NotSupportedMessage.format("bulk_update"))

    def get_or_create(self, *args, **kwargs):
        raise PreparedQueryNotSupported(self.NotSupportedMessage.format("get_or_create"))

    def update_or_create(self, *args, **kwargs):
        raise PreparedQueryNotSupported(self.NotSupportedMessage.format("update_or_create"))

    def delete(self):
        raise PreparedQueryNotSupported(self.NotSupportedMessage.format("delete"))

    def update(self, *args, **kwargs):
        raise PreparedQueryNotSupported(self.NotSupportedMessage.format("update"))

    def exists(self):
        raise PreparedQueryNotSupported(self.NotSupportedMessage.format("exists"))

    def explain(self, *args, **kwargs):
        raise PreparedQueryNotSupported(self.NotSupportedMessage.format("explain"))


class PreparedQuerySqlBuilder(PreparedQuerySetBase):
    """
    A queryset class that can be used in the same way as a normal (non-evaluated) QuerySet. It is used to generate
    the SQL which is then prepared in the database for later execution.
    """

    def __init__(self, model=None, query=None, using=None, hints=None):
        query = query or PreparedSQLQuery(model)
        super().__init__(model, query, using, hints)
        self.is_get = False
        self.is_count_qry = False

    def __repr__(self):
        sql, params = self.sql_with_params()
        params = tuple([("%s" if p == Placeholder else p) for p in params])
        return sql % params

    def _fetch_all(self):
        """ _fetch_all does nothing: the PreparedQuerySqlBuilder only builds the sql, it cannot execute it. """
        self._result_cache = ()

    def sql_with_params(self):
        return self.query.sql_with_params()

    def prefetch_related(self, *lookups):
        raise PreparedQueryNotSupported(
            "Cannot use prefetch_related when preparing queysets. "
            "Add the prefetch related to the returned queryset on statement execution"
        )

    def values_list(self, *args, **kwargs):
        raise PreparedQueryNotSupported(
            "Cannot use values_list when preparing queysets. "
            "Add the values_list to the returned queryset on statement execution"
        )

    def count(self):
        clone = self._chain()
        clone.query.count()
        clone.is_count_qry = True
        return clone

    def get(self, *args, **kwargs):
        clone = self.filter(*args, **kwargs)
        clone.is_get = True
        return clone

    def first(self):
        clone = self.order_by("id")[:1]
        clone.is_get = True
        return clone

    def last(self):
        clone = self.order_by("-id")[:1]
        clone.is_get = True
        return clone


class PreparedStatementQuerySet(PreparedQuerySetBase):
    """
    A queryset class to return the results of an executed prepares statement.
    """

    def __init__(self, model, query, using=None, hints=None):
        if not isinstance(query, PreparedStmtQuery):
            raise ValueError("Can only use a PreparedStatementQuerySet with a PreparedStmtQuery")
        super().__init__(model, query, using, hints)

    def execute(self):
        self._result_cache = None
        self._fetch_all()

    def _fetch_all(self):
        if self._result_cache is None:
            self._result_cache = list(self._iterable_class(self))

    def prefetch_related(self, *arg, **kwargs):
        """
        Adding a prefetch_related to this class causes an immediate db lookup and returns a new instance of the queryset.
        """
        clone = super().prefetch_related(*arg, **kwargs)
        clone._result_cache = self._result_cache
        clone._prefetch_related_objects()
        return clone

    def count(self):
        if self._result_cache is None:
            raise PreparedStatementNotYetExecuted("You must call `execute()` on the PreparedStatementQuerySet first.")
        return len(self._result_cache)

    def first(self):
        if self._result_cache is None:
            raise PreparedStatementNotYetExecuted("You must call `execute()` on the PreparedStatementQuerySet first.")
        if len(self._result_cache) > 0:
            return self._result_cache[0]
        else:
            return None

    def last(self):
        if self._result_cache is None:
            raise PreparedStatementNotYetExecuted("You must call `execute()` on the PreparedStatementQuerySet first.")
        if len(self._result_cache) > 0:
            return self._result_cache[-1]
        else:
            return None

    def values_list(self, *fields, flat=False, named=False):
        if self._result_cache is None:
            raise PreparedStatementNotYetExecuted("You must call `execute()` on the PreparedStatementQuerySet first.")

        if flat and named:
            raise TypeError("'flat' and 'named' can't be used together.")

        if flat is True and len(fields) > 1:
            raise TypeError("'flat' is not valid when values_list is called with more than one field.")

        clone = self._chain()

        if flat is True:
            clone._result_cache = [getattr(i, fields[0]) for i in self._result_cache]
        elif named is True:
            Row = namedtuple('Row', fields)
            clone._result_cache = [Row(*[getattr(i, j) for j in fields]) for i in  self._result_cache]
        else:
            clone._result_cache = [tuple(getattr(i, j) for j in fields) for i in self._result_cache]
        return clone

    def filter(self, *args, **kwargs):
        raise CannotAlterPreparedStatementQuerySet(
            "Please use python built-in function `filter` on the query set instead"
        )

    def get(self, *args, **kwargs):
        raise CannotAlterPreparedStatementQuerySet(
            "Please use python built-in function `filter` on the query set instead"
        )

    def earliest(self, *args, **kwargs):
        raise CannotAlterPreparedStatementQuerySet(
            "Please use python built-in function `sorted` on the query set instead"
        )

    def latest(self, *args, **kwargs):
        raise CannotAlterPreparedStatementQuerySet(
            "Please use python built-in function `sorted` on the query set instead"
        )
