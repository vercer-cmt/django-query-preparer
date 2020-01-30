# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

from django.db import connection
from django.test import TransactionTestCase
from psycopg2.errors import ProgrammingError

from dqp.prepared_stmt import dictfetchall, PreparedStatement


class TestPreparedStatement(TransactionTestCase):

    test_schema = """
    drop table if exists my_table;
    create table my_table (
        id   serial,
        name text
    );
    """

    test_data = """
    insert into my_table(name) values ('dan');
    insert into my_table(name) values ('ben');
    insert into my_table(name) values ('gareth');
    insert into my_table(name) values ('marcin');
    """

    @classmethod
    def setUp(cls):
        """
        Create some schema and data in the test database which is used in the following tests.
        """
        with connection.cursor() as cursor:
            cursor.execute(cls.test_schema)
            cursor.execute(cls.test_data)

    @classmethod
    def tearDown(cls):
        """
        Clean up after this test suite
        """
        with connection.cursor() as cursor:
            cursor.execute("drop table if exists my_table;")

    def test_prepare_statement(self):
        """
        Given a PreparedStatement object is instantiated with an sql query
        When  prepare() is called
        Then  the query should be pre-processed
        And   the execution statement should be created
        And   the query should be prepared in the database
        """
        my_qry = "select * from my_table;"
        ps = PreparedStatement("my_stmt", my_qry)

        self.assertFalse(ps._check_stmt_is_prepared())

        ps.prepare()

        with connection.cursor() as cursor:
            cursor.execute("select * from pg_prepared_statements where name = 'my_stmt';")
            results = dictfetchall(cursor)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["name"], "my_stmt")
        self.assertEqual(results[0]["statement"].lower(), "prepare my_stmt as select * from my_table;")

        self.assertEqual(ps.sql, "select * from my_table;")
        self.assertEqual(ps.execute_stmt.lower(), "execute my_stmt")

        self.assertTrue(ps._check_stmt_is_prepared())

    def test_execute_statement_no_params(self):
        """
        Given a PreparedStatement object is instantiated with an sql query
        And   the statement has been prepared
        When  execute() is called
        Then  the results of the query should be returned
        """
        my_qry = "select * from my_table;"
        ps = PreparedStatement("my_stmt", my_qry)
        ps.prepare()
        self.assertTrue(ps._check_stmt_is_prepared())

        results = ps.execute()
        expected_results = [
            {"id": 1, "name": "dan"},
            {"id": 2, "name": "ben"},
            {"id": 3, "name": "gareth"},
            {"id": 4, "name": "marcin"},
        ]
        self.assertEqual(results, expected_results)

    def test_execute_statement_with_params(self):
        """
        Given a PreparedStatement object is instantiated with an sql query
        And   the statement has been prepared
        When  execute() is called with some parameters
        Then  the results of the query should be returned
        """
        my_qry = "select * from my_table where name like %(person_name)s"
        ps = PreparedStatement("my_stmt", my_qry)
        ps.prepare()
        self.assertTrue(ps._check_stmt_is_prepared())

        results = ps.execute({"person_name": "dan"})
        expected_results = [{"id": 1, "name": "dan"}]
        self.assertEqual(results, expected_results)

        # Try a name that doesn't exist in the table
        results = ps.execute({"person_name": "jamie"})
        self.assertEqual(results, [])

    def test_deallocate(self):
        """
        Given a PreparedStatement object exists
        And   the statement has been prepared
        When  deallocate() is called
        Then  the statement should deleted from the database
        ---
        Given a PreparedStatement object exists
        And   the statement has not been prepared
        When  deallocate() is called
        Then  no action should be taken and no error thrown
        """
        my_qry = "select * from my_table;"
        ps = PreparedStatement("my_stmt", my_qry)
        ps.prepare()
        self.assertTrue(ps._check_stmt_is_prepared())

        ps.deallocate()
        self.assertFalse(ps._check_stmt_is_prepared())

        # check no error is thrown on trying to deallocate a non-prepared query
        ps.deallocate()
        self.assertFalse(ps._check_stmt_is_prepared())

    def test_prep_on_execution(self):
        """
        Given a PreparedStatement object exists
        And   the statement has been prepared but then deallocated
        When  execute() is called
        Then  the statement should be re-prepared and then executed.

        This case can happen if the database session for the django app changes - the PreparedStatement object will still
        exist but the prepared statement will not exist in the db as prepared statements are per database session.
        """

        my_qry = "select * from my_table;"
        ps = PreparedStatement("my_stmt", my_qry)
        ps.prepare()
        self.assertTrue(ps._check_stmt_is_prepared())

        ps.deallocate()
        self.assertFalse(ps._check_stmt_is_prepared())

        results = ps.execute()
        expected_results = [
            {"id": 1, "name": "dan"},
            {"id": 2, "name": "ben"},
            {"id": 3, "name": "gareth"},
            {"id": 4, "name": "marcin"},
        ]
        self.assertEqual(results, expected_results)
        self.assertTrue(ps._check_stmt_is_prepared())

