# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

from django.db import connection
from django.test import TestCase

from dqp.decorators import register_prepared_sql, prepare_sql, register_prepared_qs, prepare_qs
from dqp.prepared_stmt import PreparedStatement, PreparedORMStatement
from dqp.prepared_stmt_controller import PreparedStatementController

from test_app.models import Species


class TestDecorators(TestCase):
    def test_register_prepared_sql(self):
        """
        Given a function has been decorated with the register_prepared_sql decorator
        When  the function is brought into scope
        Then  the function will be registered with the PreparedStatementController
        And   its name will be name of the decorated function
        And   a prepared statement object will not be created
        And   the function will not be executed
        And   the SQL will not be prepared in the database
        """

        @register_prepared_sql
        def some_sql():
            # By raising an error we can ensure that the function isn't evaluated in the course of the test
            raise RuntimeError

        self.assertEqual(some_sql(), "test_app.integration_tests.test_decorators.some_sql")
        self.assertTrue(some_sql() in PreparedStatementController.sql_generating_functions)
        self.assertFalse(some_sql() in PreparedStatementController.qs_generating_functions)
        self.assertFalse(some_sql() in PreparedStatementController.prepared_statements)

        with connection.cursor() as cursor:
            cursor.execute(
                "select count(*) from pg_prepared_statements where name = 'test_app__integration_tests__test_decorators__some_sql';"
            )
            (count,) = cursor.fetchone()
        self.assertEqual(count, 0)

    def test_prepare_sql(self):
        """
        Given a function has been decorated with the prepare_sql decorator
        When  the function is brought into scope
        Then  the function will be registered with the PreparedStatementController
        And   a prepared statement object will be created
        And   its name will be name of the decorated function
        And   the function will be executed to return the SQL
        And   the SQL will be prepared in the database
        """

        @prepare_sql
        def select_now():
            return "select now();"

        self.assertEqual(select_now(), "test_app.integration_tests.test_decorators.select_now")
        self.assertTrue(select_now() in PreparedStatementController.sql_generating_functions)
        self.assertFalse(select_now() in PreparedStatementController.qs_generating_functions)
        self.assertTrue(select_now() in PreparedStatementController.prepared_statements)
        self.assertTrue(isinstance(PreparedStatementController.prepared_statements[select_now()], PreparedStatement))
        self.assertEqual(PreparedStatementController.prepared_statements[select_now()].sql, "select now();")

        with connection.cursor() as cursor:
            cursor.execute(
                "select count(*) from pg_prepared_statements where name = 'test_app__integration_tests__test_decorators__select_now';"
            )
            (count,) = cursor.fetchone()
        self.assertEqual(count, 1)

    def test_register_prepared_qs(self):
        """
        Given a function has been decorated with the register_prepared_qs decorator
        When  the function is brought into scope
        Then  the function will be registered with the PreparedStatementController
        And   its name will be name of the decorated function
        And   a prepared statement object will not be created
        And   the function will not be executed
        And   the SQL will not be prepared in the database
        """

        @register_prepared_qs
        def an_orm_query():
            # By raising an error we can ensure that the function isn't evaluated in the course of the test
            raise RuntimeError

        self.assertEqual(an_orm_query(), "test_app.integration_tests.test_decorators.an_orm_query")
        self.assertFalse(an_orm_query() in PreparedStatementController.sql_generating_functions)
        self.assertTrue(an_orm_query() in PreparedStatementController.qs_generating_functions)
        self.assertFalse(an_orm_query() in PreparedStatementController.prepared_statements)

        with connection.cursor() as cursor:
            cursor.execute(
                "select count(*) from pg_prepared_statements where name = 'test_app__integration_tests__test_decorators__an_orm_query';"
            )
            (count,) = cursor.fetchone()
        self.assertEqual(count, 0)

    def test_prepare_qs(self):
        """
        Given a function has been decorated with the prepare_qs decorator
        When  the function is brought into scope
        Then  the function will be registered with the PreparedStatementController
        And   a prepared ORM statement object will be created
        And   its name will be name of the decorated function
        And   the function will be executed to return the SQL from the ORM query
        And   the SQL will be prepared in the database
        """

        @prepare_qs
        def all_species():
            return Species.prepare.all()

        self.assertEqual(all_species(), "test_app.integration_tests.test_decorators.all_species")
        self.assertFalse(all_species() in PreparedStatementController.sql_generating_functions)
        self.assertTrue(all_species() in PreparedStatementController.qs_generating_functions)
        self.assertTrue(all_species() in PreparedStatementController.prepared_statements)
        self.assertTrue(
            isinstance(PreparedStatementController.prepared_statements[all_species()], PreparedORMStatement)
        )
        self.assertEqual(
            PreparedStatementController.prepared_statements[all_species()].sql,
            """select "test_app_species"."id", "test_app_species"."name" from "test_app_species";""",
        )

        with connection.cursor() as cursor:
            cursor.execute(
                "select count(*) from pg_prepared_statements where name = 'test_app__integration_tests__test_decorators__all_species';"
            )
            (count,) = cursor.fetchone()
        self.assertEqual(count, 1)
