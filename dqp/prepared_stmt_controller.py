# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

from dqp.constants import FailureBehaviour
from dqp.exceptions import StatementAlreadyPreparedException, StatementNotPreparedException, StatementNotRegistered
from dqp.prepared_stmt import PreparedStatement, PreparedORMStatement


class PreparedStatementController:

    __singleton_instance = None

    prepared_statements = {}
    sql_generating_functions = {}
    qs_generating_functions = {}

    def __new__(cls):
        if cls.__singleton_instance is None:
            cls.__singleton_instance = object.__new__(cls)
        return cls.__singleton_instance

    def destroy(self):
        """
        Destroy this instance of the singleton (used in tests)
        """
        self.deallocate_all()
        self.sql_generating_functions = {}
        self.qs_generating_functions = {}
        self.__singleton_instance = None
        del self

    def register_sql(self, stmt_name, func):
        """
        Register a function which generates some SQL to be prepared in the database. The reason we register the function
        and not it's output is so that the functions are only evaulated when actually prepared so in effect they are
        lazily evaluated.
        """
        self.sql_generating_functions[stmt_name] = func

    def register_qs(self, stmt_name, func):
        """
        Register a function which generates a queryset from which SQL can be generated to be prepared in the database.
        """
        self.qs_generating_functions[stmt_name] = func

    def prepare_all(self, force=True, on_fail=FailureBehaviour.ERROR):
        """
        Prepare the SQL output of all registered functions in the database.
        """
        for stmt_name in self.sql_generating_functions.keys():
            self.prepare_sql_stmt(stmt_name, force, on_fail)

        for stmt_name in self.qs_generating_functions.keys():
            self.prepare_qs_stmt(stmt_name, force, on_fail)

    def prepare_sql_stmt(self, stmt_name, force, on_fail=FailureBehaviour.ERROR):
        """
        Prepares an SQL statement in the db. If force is True and the statement is already prepared then it is deallocated
        and re-prepared, otherwise an error is thrown if a re-preparation is attempted.
        """
        if stmt_name in self.prepared_statements.keys():
            if not force:
                raise StatementAlreadyPreparedException(f"Statement {stmt_name} has already been prepared")

            self.prepared_statements[stmt_name].deallocate()
            del self.prepared_statements[stmt_name]

        if stmt_name not in self.sql_generating_functions:
            raise StatementNotRegistered("Statement {} has not been registered before preparation".format(stmt_name))

        stmt_sql = self.sql_generating_functions[stmt_name]()
        self.prepared_statements[stmt_name] = PreparedStatement(stmt_name, stmt_sql)
        self.prepared_statements[stmt_name].prepare(on_fail)

    def prepare_qs_stmt(self, stmt_name, force, on_fail=FailureBehaviour.ERROR):
        """
        Prepares an queryset statement in the db. If force is True and the statement is already prepared then it is
        deallocated and re-prepared, otherwise an error is thrown if a re-preparation is attempted.
        """
        if stmt_name in self.prepared_statements.keys():
            if not force:
                raise StatementAlreadyPreparedException(f"Statement {stmt_name} has already been prepared")

            self.prepared_statements[stmt_name].deallocate()
            del self.prepared_statements[stmt_name]

        if stmt_name not in self.qs_generating_functions:
            raise StatementNotRegistered("Statement {} has not been registered before preparation".format(stmt_name))

        stmt_qs = self.qs_generating_functions[stmt_name]()
        self.prepared_statements[stmt_name] = PreparedORMStatement(stmt_name, stmt_qs)
        self.prepared_statements[stmt_name].prepare(on_fail)

    def execute(self, stmt_name, *args, **kwargs):
        if stmt_name not in self.prepared_statements.keys():
            raise StatementNotPreparedException(f"Statement {stmt_name} has not been prepared prior to execution")

        return self.prepared_statements[stmt_name].execute(*args, **kwargs)

    def deallocate_all(self):
        """
        Deallocate all prepared statements and remove them from the prepared_statements map.
        If the statements are not prepared in the database then it doesn't fail, it just silently moves on.
        """
        for prep_stmt in self.prepared_statements.values():
            prep_stmt.deallocate()
        self.prepared_statements = {}


def execute_stmt(stmt_name, *args, **kwargs):
    return PreparedStatementController().execute(stmt_name, *args, **kwargs)
