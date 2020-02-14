# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

import re
import logging

from django.db import transaction
from django.db import connection, OperationalError
from psycopg2.errors import InvalidSqlStatementName, ProgrammingError

from dqp.constants import Placeholder, ListPlaceholder, FailureBehaviour
from dqp.query import PreparedStmtQuery
from dqp.queryset import PreparedQuerySqlBuilder, PreparedStatementQuerySet


logger = logging.getLogger(__name__)


NAMED_PLACEHOLDER_REGEX = re.compile("(%\([\w-]+\)s)")
PLACEHOLDER_REGEX = re.compile("(%s)")
IN_REGEX = re.compile("(IN|in|In|iN)[\s]*\(%s\)")


def dictfetchall(cursor):
    """Return all rows from a cursor as a dict"""
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


class PreparedStatement:
    def __init__(self, name, sql):
        self.name = name
        self.input_sql = sql
        self.num_params = 0
        self.named_placeholders = None
        self.sql = ""

    def prepare(self, on_fail=FailureBehaviour.ERROR):
        self._prepare_input_sql()
        self._create_exec_stmt()

        try:
            with connection.cursor() as cursor:
                cursor.execute("""PREPARE {} AS {}""".format(self.name, self.sql))
        except Exception as e:
            if on_fail == FailureBehaviour.WARN:
                logger.warning("Could not prepare query: {}".format(e))
            else:
                raise

    def _prepare_input_sql(self):
        """
        One of the nice features of psycopg2 is the ability to name parameters in the SQL and then supply a dict, e.g:

        > SQL = "select count(*) from django_migrations where id = %(migration_id)s;"
        > cursor.execute(SQL, {"migration_id": 1})

        Or even just use placeholders:

        > SQL = "select count(*) from django_migrations where id = %s;"
        > cursor.execute(SQL, [1])

        Unfortunately, when a statement is prepared in postgresql it must use numbered arguments:

        > SQL = "prepare my_stmt as select count(*) from django_migrations where id = $1;"
        > cursor.execute(SQL)
        > cursor.execute("execute my_stmt(%(migration_id)s)", {"migration_id": 1})

        or:

        > SQL = "prepare my_stmt as select count(*) from django_migrations where id = $1;"
        > cursor.execute(SQL)
        > cursor.execute("execute my_stmt(%s)", [1])

        Because we want the calling code to be able to generate SQL using the placeholders we do the replacement here.

        N.B. Mixing named and un-named placeholders is not allowed. Use one or the other.
        """
        sql = self.input_sql.lower().strip()
        if sql[-1] == ";":
            sql = sql[:-1]
        sql_as_list = sql.split()

        counter = 0
        unnamed_params_only = False

        # First look for unnamed placeholders
        for i, token in enumerate(sql_as_list):
            match = PLACEHOLDER_REGEX.search(token)
            if match is not None and len(match.groups()) == 1:
                counter += 1
                sql_as_list[i] = PLACEHOLDER_REGEX.sub("${}".format(counter), token)

        if counter > 0:
            # If we have found un-named placeholders then we don't allow any named placeholders.
            unnamed_params_only = True

        # Now check for named placeholders
        named_placeholders = []
        for i, token in enumerate(sql_as_list):
            match = NAMED_PLACEHOLDER_REGEX.search(token)
            if match is not None and len(match.groups()) == 1:
                if unnamed_params_only is True:
                    raise ProgrammingError("Cannot match named and un-named placeholder values in a prepared statement")
                placeholder = match.groups()[0]
                named_placeholders.append(placeholder)
                counter += 1
                sql_as_list[i] = NAMED_PLACEHOLDER_REGEX.sub("${}".format(counter), token)

        if len(named_placeholders) > 0:
            self.named_placeholders = named_placeholders

        self.num_params = counter
        sql = " ".join(sql_as_list)
        sql += ";"
        self.sql = sql

    def _create_exec_stmt(self):
        if self.named_placeholders is not None:
            self.execute_stmt = "".join(["EXECUTE ", self.name, "(", ", ".join(self.named_placeholders), ")"])
        elif self.num_params > 0:
            self.execute_stmt = "".join(["EXECUTE ", self.name, "(", ", ".join(self.num_params * ["%s"]), ")"])
        else:
            self.execute_stmt = "EXECUTE {}".format(self.name)

    def execute(self, qry_args=None):
        try:
            # We try the execution in its own transaction. This is because if an SQL statement fails while django is
            # in a transaction it won't run any more SQL statements until the current transaction has been rolled back.
            # Putting just this line in a transaction allows us to rollback the inner transaction without affecting a
            # user's transaction outside this code.
            with transaction.atomic():
                return self._execute(qry_args)
        except OperationalError as e:
            # Check to see if the error was caused by InvalidSqlStatementName which means that we didn't prepare this
            # statement before attemptiong to execute it. This could be caused by the database session reconnecting.
            if e.__context__ is not None and e.__context__.__class__ == InvalidSqlStatementName:
                with transaction.atomic():
                    if not self._check_stmt_is_prepared():
                        # Let's try to re-prepare the statement
                        self.prepare()
                        # and check again to make sure it worked!
                        if not self._check_stmt_is_prepared():
                            raise RuntimeError("Statement {} will not be prepared!".format(self.name))
                        # ok let's execute the statement. If it fails this time we just let it error out.
                        return self._execute(qry_args)
                    else:
                        # The statement is prepared so the error must be something else. Raise it
                        raise
            else:
                raise

    def deallocate(self):
        if self._check_stmt_is_prepared():
            try:
                with connection.cursor() as cursor:
                    cursor.execute("""DEALLOCATE {}""".format(self.name))
            except Exception:
                pass

    def _check_stmt_is_prepared(self):
        with connection.cursor() as cursor:
            cursor.execute("""select count(*) from pg_prepared_statements where name = %s;""", [self.name])
            (count,) = cursor.fetchone()
        return count == 1

    def _execute(self, args):
        with connection.cursor() as cursor:
            cursor.execute(self.execute_stmt, args)
            return dictfetchall(cursor)


class PreparedORMStatement(PreparedStatement):
    """
    A wrapper around the PreparedStatement class which takes a django query set and handles preperation and execution of it.
    """

    def __init__(self, name, qs):
        if not isinstance(qs, PreparedQuerySqlBuilder):
            raise ValueError(
                "A prepared ORM statement requires the queryset to be a PreparedQuerySqlBuilder built using the PreparedStatementManager"
            )
        sql, self.params = qs.sql_with_params()
        super().__init__(name, self._modify_sql(sql))
        self.model = qs.model
        self.compiled_sql_data = qs.query.compiled_sql_data
        self.is_get_qry = qs.is_get
        self.is_count_qry = qs.is_count_qry
        self.params_required = any((isinstance(x, Placeholder) or isinstance(x, ListPlaceholder)) for x in self.params)

    @staticmethod
    def _modify_sql(sql):
        # Change from `field IN (%s)` to use postgres specific `field = ANY(%s)` so we can supply any number of args at
        # execution time. I believe this is safe as django never produces sql with named placeholders.
        return IN_REGEX.sub("= ANY(%s)", sql)

    def execute(self, *args, **kwargs):
        if kwargs == {}:
            if self.params_required is True:
                raise ValueError("Not enough parameters supplied to execute prepared statement")
            elif len(self.params) > 0:
                return super().execute(self.params)
            else:
                return super().execute()

        # Merge the given keyword arguments with any constant arguments returned by django sql compiler
        qry_params = []
        for p in self.params:
            if isinstance(p, Placeholder) or isinstance(p, ListPlaceholder):
                # It's a standard placeholder value so replace with the passed in parameter value
                if p.name not in kwargs:
                    raise ValueError("Missing parameter {} is required to execute prepared statement".format(p.name))
                qry_params.append(kwargs[p.name])
                del kwargs[p.name]
            elif isinstance(p, str) and "dqp.placeholder." in p:
                # It's a `LIKE` placeholder parameter and has form of either "%param", "%param%" or "param%"
                parts = [i if len(i) > 0 else "%" for i in p.split("%")]
                for i, part in enumerate(parts):
                    if part == "%":
                        continue

                    arg_name = part.lstrip("dqp.placeholder.")
                    if arg_name not in kwargs:
                        raise ValueError(
                            "Missing parameter {} is required to execute prepared statement".format(arg_name)
                        )
                    # replace the placehollder name with the actual supplied parameter value
                    parts[i] = kwargs[arg_name]
                    del kwargs[arg_name]
                qry_params.append("".join(parts))
            else:
                # It's a parameter that is constant.
                qry_params.append(p)

        if len(kwargs.keys()) > 0:
            raise ValueError("Unknown parameters supplied for prepared statement: {}".format(" , ".join(kwargs.keys())))
        return super().execute(qry_params)

    def _execute(self, qry_args):
        if self.is_count_qry is True:
            return self._execute_count_qry(qry_args)

        query = PreparedStmtQuery(self.model, self.execute_stmt, qry_args, self.compiled_sql_data)
        qs = PreparedStatementQuerySet(model=self.model, query=query)
        qs.execute()
        if self.is_get_qry is True:
            # Mimic django behaviour: return the object or raise errors for a get query rather than returning a queryset
            num = len(qs)
            if num == 1:
                return qs[0]
            if not num:
                raise self.model.DoesNotExist("%s matching query does not exist." % self.model._meta.object_name)
            raise self.model.MultipleObjectsReturned(
                "get() returned more than one %s -- it returned %s!" % (self.model._meta.object_name, num)
            )
        else:
            return qs

    def _execute_count_qry(self, args):
        with connection.cursor() as cursor:
            cursor.execute(self.execute_stmt, args)
            return cursor.fetchone()[0]
