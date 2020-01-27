# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

from django.apps import AppConfig
from django.conf import settings


class DQPConfig(AppConfig):
    name = "dqp"

    def ready(self):
        if getattr(settings, "DQP_PREPARE_ON_APP_START", True) is True:
            from dqp.prepared_stmt_controller import PreparedStatementController

            PreparedStatementController().prepare_all()
