# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

from django.db.models.expressions import Col
from django.db.models.sql.compiler import SQLCompiler

from dataclasses import dataclass


@dataclass
class CompiledSQLData:
    """
    Holds information about the compiled SQL that is determined when preparing the SQL but needs to be known on execution.
    """

    col_count = 0
    has_extra_select = False
    select = None
    klass_info = None
    annotation_col_map = None


class NoExecutionSQLCompiler(SQLCompiler):
    """
    The NoExecutionSQLCompiler is mostly the same as the basic django SQLCompiler but it has special cases for preparing
    a count() query and it cannot execute SQL quries; it is only used to build the sql.
    """

    def __init__(self, query, connection, using, is_count_qry=False):
        super().__init__(query, connection, using)
        self.is_count_qry = is_count_qry

    def get_select(self):
        """ Special case for if we are preparing a count() query """
        if self.is_count_qry is True:
            col = (Col("__count", None), ("COUNT(*)", []), None)
            klass_info = {"model": self.query.model, "select_fields": ["__count"]}
            annotations = dict()
            return (col,), klass_info, annotations
        else:
            return super().get_select()

    def get_extra_select(self, *args, **kawrgs):
        """ Special case for if we are preparing a count() query """
        if self.is_count_qry is True:
            return tuple()
        else:
            return super().get_extra_select(*args, **kawrgs)

    def execute_sql(self, *args, **kwargs):
        raise NotImplementedError


class PreparedStmtCompiler(SQLCompiler):
    """
    The PreparedStmtCompiler executes prepared statements. The executions statement and the input parameters are
    supplied by the PreparedStmtQuery object.
    """

    def __init__(self, query, connection, using, compiled_sql_data):
        # circular import
        from dqp.query import PreparedStmtQuery

        if not isinstance(query, PreparedStmtQuery):
            raise ValueError("Can only use a PreparedStmtCompiler with a PreparedStmtQuery")
        super().__init__(query, connection, using)
        self.col_count = compiled_sql_data.col_count
        self.has_extra_select = compiled_sql_data.col_count
        self.select = compiled_sql_data.select
        self.klass_info = compiled_sql_data.klass_info
        self.annotation_col_map = compiled_sql_data.annotation_col_map

    def as_sql(self, *args, **kwargs):
        return self.query.exec_stmt, self.query.params
