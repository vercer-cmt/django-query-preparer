# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

from unittest.mock import patch

from django.test import TestCase

from dqp.exceptions import StatementNotPreparedException, StatementAlreadyPreparedException, StatementNotRegistered
from dqp.prepared_stmt_controller import PreparedStatementController
from dqp.prepared_stmt import PreparedStatement, PreparedORMStatement

from test_app.models import Species


class TestPreparedStatementController(TestCase):
    def setUp(self):
        PreparedStatementController().destroy()

    def tearDown(self):
        PreparedStatementController().destroy()

    def test_is_singleton(self):
        """
        Given a PreparedStatementController is instantiated
        When  another PreparedStatementController is instantiated
        Then  they should both point to the same singleton instance
        """
        psc1 = PreparedStatementController()
        psc2 = PreparedStatementController()
        self.assertTrue(psc1 is psc2)

    def test_register_sql(self):
        """
        Given a function that returns an SQL string
        When  register_sql is called with that function as one of the arguments
        Then  that function should be added to the sql_generating_functions dict
        """

        def gen_sql():
            pass

        psc = PreparedStatementController()
        psc.register_sql("gen_sql", gen_sql)
        self.assertTrue("gen_sql" in psc.sql_generating_functions)
        self.assertFalse("gen_sql" in psc.qs_generating_functions)
        self.assertTrue(psc.sql_generating_functions["gen_sql"] is gen_sql)

    def test_register_qs(self):
        """
        Given a function that returns an ORM query
        When  register_qs is called with that function as one of the arguments
        Then  that function should be added to the qs_generating_functions dict
        """

        def gen_qs():
            pass

        psc = PreparedStatementController()
        psc.register_qs("gen_qs", gen_qs)
        self.assertFalse("gen_qs" in psc.sql_generating_functions)
        self.assertTrue("gen_qs" in psc.qs_generating_functions)
        self.assertTrue(psc.qs_generating_functions["gen_qs"] is gen_qs)

    def test_prepare_sql_stmt(self):
        """
        Given a function that generates SQL has been registered with the PreparedStatementController
        When  prepare_sql_stmt is called
        Then  a PreparedStatement object should be created
        And   the PreparedStatement should be added to the prepared_statements dict
        And   the prepare method of the PreparedStatement will be called
        """
        psc = PreparedStatementController()
        psc.register_sql("gen_sql", lambda: None)

        with patch.object(PreparedStatement, "prepare", return_value=None) as mock_prepare:
            psc.prepare_sql_stmt("gen_sql", force=False)
            self.assertTrue("gen_sql" in psc.prepared_statements)
            self.assertTrue(isinstance(psc.prepared_statements["gen_sql"], PreparedStatement))
        mock_prepare.assert_called_once()

    def test_prepare_sql_stmt_force(self):
        """
        Given a SQL statement has already been prepared in the database
        When  prepare_sql_stmt is called for the same function
        And   force is False
        Then  a StatementAlreadyPreparedException error will be raised
        ---
        Given a SQL statement has already been prepared in the database
        When  prepare_sql_stmt is called for the same function
        And   force is True
        Then  the existing statement will be deallocated
        And   the statement will be re-prepared
        """
        psc = PreparedStatementController()
        psc.register_sql("gen_sql", lambda: None)

        with patch.object(PreparedStatement, "prepare", return_value=None):
            psc.prepare_sql_stmt("gen_sql", force=False)
            self.assertTrue("gen_sql" in psc.prepared_statements)

        with self.assertRaises(StatementAlreadyPreparedException):
            psc.prepare_sql_stmt("gen_sql", force=False)

        with patch.object(PreparedStatement, "prepare", return_value=None) as mock_prepare:
            with patch.object(PreparedStatement, "deallocate", return_value=None) as mock_deallocate:
                psc.prepare_sql_stmt("gen_sql", force=True)
        mock_deallocate.assert_called_once()
        mock_prepare.assert_called_once()

    def test_prepare_sql_stmt_unregistered(self):
        """
        Given a function that generates SQL has not been registered with the PreparedStatementController
        When  prepare_sql_stmt is called for that function
        Then  a StatementNotRegistered error will be raised
        """
        psc = PreparedStatementController()
        with self.assertRaises(StatementNotRegistered):
            psc.prepare_sql_stmt("unregistered_sql", force=False)

    def test_prepare_qs_stmt(self):
        """
        Given a function that generates an ORM query has been registered with the PreparedStatementController
        When  prepare_qs_stmt is called
        Then  a PreparedORMStatement object should be created
        And   the PreparedORMStatement should be added to the prepared_statements dict
        And   the SQL will be preapred in the database
        """
        psc = PreparedStatementController()
        psc.register_qs("gen_qs", lambda: Species.prepare.all())

        with patch.object(PreparedORMStatement, "prepare", return_value=None) as mock_prepare:
            psc.prepare_qs_stmt("gen_qs", force=False)
            self.assertTrue("gen_qs" in psc.prepared_statements)
            self.assertTrue(isinstance(psc.prepared_statements["gen_qs"], PreparedORMStatement))
        mock_prepare.assert_called_once()

    def test_prepare_qs_stmt_force(self):
        """
        Given an ORM statement has already been prepared in the database
        When  prepare_qs_stmt is called for the same function
        And   force is False
        Then  a StatementAlreadyPreparedException error will be raised
        ---
        Given an ORM statement has already been prepared in the database
        When  prepare_qs_stmt is called for the same function
        And   force is True
        Then  the existing statement will be deallocated
        And   the statement will be re-prepared
        """
        psc = PreparedStatementController()
        psc.register_qs("gen_qs", lambda: Species.prepare.all())

        with patch.object(PreparedORMStatement, "prepare", return_value=None):
            psc.prepare_qs_stmt("gen_qs", force=False)
            self.assertTrue("gen_qs" in psc.prepared_statements)

        with self.assertRaises(StatementAlreadyPreparedException):
            psc.prepare_qs_stmt("gen_qs", force=False)

        with patch.object(PreparedORMStatement, "prepare", return_value=None) as mock_prepare:
            with patch.object(PreparedORMStatement, "deallocate", return_value=None) as mock_deallocate:
                psc.prepare_qs_stmt("gen_qs", force=True)
        mock_deallocate.assert_called_once()
        mock_prepare.assert_called_once()

    def test_prepare_qs_stmt_unregistered(self):
        """
        Given a function that generates an ORM query has not been registered with the PreparedStatementController
        When  prepare_sql_stmt is called for that function
        Then  a StatementNotRegistered error will be raised
        """
        psc = PreparedStatementController()
        with self.assertRaises(StatementNotRegistered):
            psc.prepare_qs_stmt("unregistered_qs", force=False)

    def test_prepare_all(self):
        """
        Given a set of sql and qs generating functions that have been registered with the PreparedStatementController
        When  prepare_all() is called
        Then  prepare_sql_stmt and prepare_qs_stmt should be called for each registered function as appropriate
        """
        psc = PreparedStatementController()
        psc.register_sql("gen_sql1", lambda: None)
        psc.register_sql("gen_sql2", lambda: None)
        psc.register_sql("gen_sql3", lambda: None)
        psc.register_qs("gen_qs1", lambda: Species.prepare.all())
        psc.register_qs("gen_qs2", lambda: Species.prepare.all())

        with patch.object(PreparedORMStatement, "prepare", return_value=None) as mock_orm_prepare:
            with patch.object(PreparedStatement, "prepare", return_value=None) as mock_sql_prepare:
                psc.prepare_all()
        self.assertEqual(mock_sql_prepare.call_count, 3)
        self.assertEqual(mock_orm_prepare.call_count, 2)

    def test_execute_prepared_stmt(self):
        """
        Given a statement has been prepared in the database
        When  execute() is called for that statement
        Then  then the execute method of the PreparedStatement object will be called
        """
        psc = PreparedStatementController()
        psc.register_sql("gen_sql", lambda: None)

        with patch.object(PreparedStatement, "prepare", return_value=None):
            psc.prepare_sql_stmt("gen_sql", force=False)
            self.assertTrue("gen_sql" in psc.prepared_statements)

        with patch.object(PreparedStatement, "execute", return_value=None) as mock_execute:
            psc.execute("gen_sql")
        mock_execute.assert_called_once()

    def test_execute_unprepared_stmt(self):
        """
        Given a statement has not been prepared in the database
        When  execute() is called for that statement
        Then  a StatementNotPreparedException error will be raised
        """
        psc = PreparedStatementController()
        self.assertFalse("gen_sql" in psc.prepared_statements)

        with self.assertRaises(StatementNotPreparedException):
            psc.execute("gen_qs", force=False)

    def test_deallocate_all(self):
        """
        Given there are prepared statements
        When  deallocate_all() is called
        Then  the deallocate method will be called on every PreparedStatement object
        And   the prepared_statements dict will be empty
        """
        psc = PreparedStatementController()
        psc.register_sql("gen_sql", lambda: None)
        psc.register_sql("gen_sql2", lambda: None)
        psc.register_sql("gen_sql3", lambda: None)

        with patch.object(PreparedStatement, "prepare", return_value=None):
            psc.prepare_sql_stmt("gen_sql", force=False)
            psc.prepare_sql_stmt("gen_sql2", force=False)
            psc.prepare_sql_stmt("gen_sql3", force=False)

        with patch.object(PreparedStatement, "deallocate", return_value=None) as mock_deallocate:
            psc.deallocate_all()
        self.assertEqual(mock_deallocate.call_count, 3)

        self.assertEqual(psc.prepared_statements, {})
