# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

from dqp.prepared_stmt_controller import PreparedStatementController


class PrepStmtTestMixin:
    """
    Re-prepare all prepared queries between each test
    """

    @classmethod
    def setUp(cls):
        PreparedStatementController().deallocate_all()
        PreparedStatementController().prepare_all(force=True)
