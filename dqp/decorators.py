# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

import hashlib

from dqp.prepared_stmt_controller import PreparedStatementController


def _make_stmt_name(func):
    """
    Create the prepared query name which is a string up-to 63 characters made up of
    - an underscore
    - the md5 hash of the <module.function> name
    - an underscore
    - up-to 29 characters of the function name
    """
    m = hashlib.md5()
    func_name = func.__name__
    fully_qualified_name =f"{func.__module__}.{func.__name__}"
    m.update(bytes(fully_qualified_name, encoding="utf-8"))

    stmt_name = "_"
    stmt_name += m.hexdigest()
    stmt_name += "_" + func_name[:29]
    return stmt_name


def register_prepared_sql(func):
    """
    Register an SQL statement to be prepared in the database. The function supplied must return an SQL query which will
    be prepared in the database. The function return will be overwritten to supply the name of prepared statement so it
    can easily be called:

    @register_prepared_sql
    def count_trades():
        return "select count(*) from trades_parent_trade;"

    # prepare the query (handled automatically by lib in django)

    rows = execute_stmt(count_trades())

    """
    stmt_name = _make_stmt_name(func)
    PreparedStatementController().register_sql(stmt_name, func)
    return lambda: stmt_name


def register_prepared_qs(func):
    """
    Register an ORM statement to be prepared in the database. The function supplied must return a django queryset query
    which will be prepared in the database. The function return will be overwritten to supply the name of prepared
     statement so it can easily be called:

    @register_prepared_qs
    def get_trades():
        return Parent_trades.prepare_objects.filter(id__in=PLACEHOLDER_LIST)

    # prepare the query (handled automatically by lib in django)

    rows = execute_stmt(get_trades(), [1, 2, 3])

    """
    stmt_name = _make_stmt_name(func)
    PreparedStatementController().register_qs(stmt_name, func)
    return lambda: stmt_name


def prepare_sql(func):
    """
    Prepare an SQL statement in the database. The function supplied must return an SQL query which will be prepared in
    the database. The function return will be overwritten to supply the name of prepared statement so it can easily
    be called:

    @prepare_sql
    def count_trades():
        return "select count(*) from trades_parent_trade;"

    rows = execute_stmt(count_trades())

    """
    stmt_name = _make_stmt_name(func)
    PreparedStatementController().register_sql(stmt_name, func)
    PreparedStatementController().prepare_sql_stmt(stmt_name, force=False)
    return lambda: stmt_name


def prepare_qs(func):
    """
    Prepare an ORM statement in the database. The function supplied must return a django queryset query which will be
    prepared in the database. The function return will be overwritten to supply the name of prepared statement so it
    can easily be called:

    @prepare_qs
    def get_trades():
        return Parent_trades.objects.filter(id__in=PLACEHOLDER_LIST)

    rows = execute_stmt(get_trades(), [1, 2, 3])

    """
    stmt_name = _make_stmt_name(func)
    PreparedStatementController().register_qs(stmt_name, func)
    PreparedStatementController().prepare_qs_stmt(stmt_name, force=False)
    return lambda: stmt_name
