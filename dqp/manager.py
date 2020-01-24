# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

from django.db.models.manager import BaseManager

from dqp.queryset import PreparedQuerySqlBuilder


class PreparedStatementManager(BaseManager.from_queryset(PreparedQuerySqlBuilder)):
    _queryset_class = PreparedQuerySqlBuilder
