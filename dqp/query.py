# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

from dataclasses import dataclass

from django.db import DEFAULT_DB_ALIAS, connections
from django.db.models.sql.query import Query

from dqp.constants import Placeholder, ListPlaceholder
from dqp.sql_compiler import CompiledSQLData, NoExecutionSQLCompiler, PreparedStmtCompiler


class PreparedSQLQuery(Query):
    """
    A PreparedSQLQuery object is used to create an sql statement from a queryset but cannot execute it.
    """

    compiler = "NoExecutionSQLCompiler"

    def __init__(self, *args, **kawrgs):
        super().__init__(*args, **kawrgs)
        self.compiled_sql_data = CompiledSQLData()
        self.is_count_qry = False
        self.placeholder_names = set()

    def count(self):
        self.is_count_qry = True

    def get_compiler(self, using=None, connection=None):
        """
        A PreparedSQLQuery always uses the NoExecutionSQLCompiler.
        """
        if using is None and connection is None:
            raise ValueError("Need either using or connection")
        if using:
            connection = connections[using]
        return NoExecutionSQLCompiler(self, connection, using, is_count_qry=self.is_count_qry)

    def build_lookup(self, lookups, lhs, rhs):
        if isinstance(rhs, Placeholder) or isinstance(rhs, ListPlaceholder):
            if rhs.name in self.placeholder_names:
                raise NameError(
                    "Repeated placeholder name: {}. All placeholders in a query must have unique names.".format(
                        rhs.name
                    )
                )
            self.placeholder_names.add(rhs.name)
            # Hack - if the rhs is a placeholder then we just want to return the placeholder when the
            # value is prepared against the lhs field type.
            _f = lhs.output_field.get_prep_value
            lhs.output_field.get_prep_value = lambda x: x
            if hasattr(lhs.output_field, "get_path_info"):
                _ff = lhs.output_field.get_path_info()[-1].target_fields[-1].get_prep_value
                lhs.output_field.get_path_info()[-1].target_fields[-1].get_prep_value = lambda x: x

        lookup = super().build_lookup(lookups, lhs, rhs)

        if isinstance(rhs, Placeholder) or isinstance(rhs, ListPlaceholder):
            # Restore the get_prep_value functions to their original for the next use (which may not be in a prepared qry)
            lhs.output_field.get_prep_value = _f
            if hasattr(lhs.output_field, "get_path_info"):
                lhs.output_field.get_path_info()[-1].target_fields[-1].get_prep_value = _ff

        return lookup

    def sql_with_params(self):
        """
        Return the query as an SQL string and the parameters that will be
        substituted into the query.
        """
        compiler = self.get_compiler(using=DEFAULT_DB_ALIAS)
        sql, params = compiler.as_sql()

        # These are required to be passed into the PreparedStmtCompiler that will execute the prepared statement.
        self.compiled_sql_data.col_count = compiler.col_count
        self.compiled_sql_data.has_extra_select = compiler.col_count
        self.compiled_sql_data.select = compiler.select
        self.compiled_sql_data.klass_info = compiler.klass_info
        self.compiled_sql_data.annotation_col_map = compiler.annotation_col_map

        return sql, params


class PreparedStmtQuery(Query):
    """
    A PreparedStmtQuery uses the PreparedStmtCompiler to execute a prepared statement in the database.
    """

    compiler = "PreparedStmtCompiler"

    def __init__(self, model, exec_stmt, params, compiled_sql_data):
        super().__init__(model)
        self.exec_stmt = exec_stmt
        # django-cachalot doesn't like params to be None so use an empty iterable instead
        self.params = params or ()
        self.compiled_sql_data = compiled_sql_data

    def sql_with_params(self):
        """
        Return the query as an SQL string and the parameters that will be
        substituted into the query.
        """
        return (self.exec_stmt, self.params)

    def get_compiler(self, using=None, connection=None):
        """
        A PreparedStmtQuery always uses the PreparedStmtCompiler.
        """
        if using is None and connection is None:
            raise ValueError("Need either using or connection")
        if using:
            connection = connections[using]
        return PreparedStmtCompiler(self, connection, using, self.compiled_sql_data)
