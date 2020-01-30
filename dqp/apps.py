# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

from django.apps import AppConfig

from dqp.constants import FailureBehaviour

class DQPConfig(AppConfig):
    name = "dqp"

    def ready(self):
        from dqp.prepared_stmt_controller import PreparedStatementController

        PreparedStatementController().prepare_all(on_fail=FailureBehaviour.WARN)
