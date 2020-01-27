from django.test import TestCase
from psycopg2.errors import ProgrammingError

from dqp.prepared_stmt import PreparedStatement, PreparedORMStatement

class TestPreparedStatement(TestCase):

    def test_pg_name_1(self):
        """
        Given a statement name which contains no dots
        When a PreparedStatement object is instantiated with this name
        Then the `pg_name` parameter will be the `name` parameter
        """
        ps = PreparedStatement("my_qry", "")
        self.assertEqual(ps.pg_name, "my_qry")

    def test_pg_name_2(self):
        """
        Given a statement name which contains dots
        When a PreparedStatement object is instantiated with this name
        Then the `pg_name` parameter will be the `name` parameter with the dots replaced by double-underscores
        """
        ps = PreparedStatement("my_module.my_file.my_qry", "")
        self.assertEqual(ps.pg_name, "my_module__my_file__my_qry")

    def test_prepare_input_sql_1(self):
        """
        Given an sql query string with no parameter placeholders
        When  _prepare_input_sql is called
        Then  the `sql` set on a PreparedStatement object should be equal to the given sql query string
        And   the `num_params` set on a PreparedStatement object should be equal to 0
        And   the `named_placeholders` set on a PreparedStatement object should be equal to None
        """
        input_sql = "select count(*) from my_records;"

        ps = PreparedStatement("", input_sql)
        ps._prepare_input_sql()

        self.assertEqual(ps.sql, input_sql)
        self.assertEqual(ps.num_params, 0)
        self.assertEqual(ps.named_placeholders, None)

    def test_prepare_input_sql_2(self):
        """
        Given an sql query string with unnamed parameter placeholders
        When  _prepare_input_sql is called
        Then  the `sql` set on a PreparedStatement object should be equal to the given sql query string but with
              all instances of %s replaced by $X where X is the given parameter number in order
        And   the `num_params` set on a PreparedStatement object should be equal to the number of %s in the input string
        And   the `named_placeholders` set on a PreparedStatement object should be equal to None
        """
        input_sql = "select id, record_name from my_records where id < %s and time_created > %s;"
        expected_sql = "select id, record_name from my_records where id < $1 and time_created > $2;"

        ps = PreparedStatement("", input_sql)
        ps._prepare_input_sql()

        self.assertEqual(ps.sql, expected_sql)
        self.assertEqual(ps.num_params, 2)
        self.assertEqual(ps.named_placeholders, None)


    def test_prepare_input_sql_3(self):
        """
        Given an sql query string with named parameter placeholders
        When  _prepare_input_sql is called
        Then  the `sql` set on a PreparedStatement object should be equal to the given sql query string but with
              all instances of %(Y)s replaced by $X where X is the given parameter number in order
        And   the `num_params` set on a PreparedStatement object should be equal to the number of %(Y)s in the input string
        And   the `named_placeholders` set on a PreparedStatement object should be a list of the parameter names in the
              order in which they appear in the input string
        """
        input_sql = (
            "select r.id, r.record_name, j.info " +
            "from my_records r, joining_table j " +
            "where r.id < %(record_id)s and r.time_created > %(time_created)s and j.record_id = %(record_id)s;"
        )

        expected_sql = (
            "select r.id, r.record_name, j.info " +
            "from my_records r, joining_table j " +
            "where r.id < $1 and r.time_created > $2 and j.record_id = $3;"
        )
        expected_params = ["%(record_id)s", "%(time_created)s", "%(record_id)s"]

        ps = PreparedStatement("", input_sql)
        ps._prepare_input_sql()

        self.assertEqual(ps.sql, expected_sql)
        self.assertEqual(ps.num_params, 3)
        self.assertEqual(ps.named_placeholders, expected_params)

    def test_prepare_input_sql_4(self):
        """
        Given an sql query string with noth named parameter placeholders and unnamed parameter placeholders
        When  _prepare_input_sql is called
        Then  a ProgrammingError exception should be raised
        """
        input_sql = "select 1 from a_table where id = %s and id <> %(named_id)s;"
        ps = PreparedStatement("", input_sql)
        with self.assertRaises(ProgrammingError) as context:
            ps._prepare_input_sql()

    def test_create_exec_stmt_1(self):
        """
        Given a statement name and no parameters
        When  _create_exec_stmt is called
        Then  the `execute_stmt` should be "execute <statement name>"
        """
        ps = PreparedStatement("my_qry", "")
        self.assertEqual(ps.num_params, 0)
        self.assertEqual(ps.named_placeholders, None)
        ps._create_exec_stmt()
        self.assertEqual(ps.execute_stmt.lower(), "execute my_qry")

    def test_create_exec_stmt_2(self):
        """
        Given a statement name and a number of parameters but no named parameters
        When  _create_exec_stmt is called
        Then  the `execute_stmt` should be "execute <statement name>((%s)+)" with one %s for each parameter
        """
        ps = PreparedStatement("my_qry", "")
        ps.num_params = 5
        self.assertEqual(ps.named_placeholders, None)
        ps._create_exec_stmt()
        self.assertEqual(ps.execute_stmt.lower(), "execute my_qry(%s, %s, %s, %s, %s)")

    def test_create_exec_stmt_2(self):
        """
        Given a statement name and a number of named parameters
        When  _create_exec_stmt is called
        Then  the `execute_stmt` should be "execute <statement name>((%(<param name>)s)+)" with one %s for each parameter
        """
        ps = PreparedStatement("my_qry", "")
        ps.num_params = 3
        ps.named_placeholders = ["%(record_id)s", "%(time_created)s", "%(time_created)s"]
        ps._create_exec_stmt()
        self.assertEqual(ps.execute_stmt.lower(), "execute my_qry(%(record_id)s, %(time_created)s, %(time_created)s)")


class TestPreparedORMStatement(TestCase):

    def test_modify_sql_1(self):
        """
        Given an sql query string with no IN terms
        When  _modify_sql is called
        Then  the return will be the same string as the input value
        """
        input_sql = "select * from my_table where id = %s"
        modified_sql = PreparedORMStatement._modify_sql(input_sql)

        self.assertEqual(modified_sql, input_sql)

    def test_modify_sql_2(self):
        """
        Given an sql query string with one or more IN terms
        When  _modify_sql is called
        Then  the return will be the same string as the input value but with all IN terms replaced by ANY terms
        """
        input_sql = "select * from my_table where id IN(%s) and time_created > %s and foo in (%s)"
        expected_sql = "select * from my_table where id = ANY(%s) and time_created > %s and foo = ANY(%s)"
        modified_sql = PreparedORMStatement._modify_sql(input_sql)

        self.assertEqual(modified_sql, expected_sql)
