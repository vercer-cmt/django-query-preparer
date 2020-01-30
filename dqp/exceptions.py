# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt


class StatementAlreadyPreparedException(Exception):
    pass


class StatementNotPreparedException(Exception):
    pass


class PreparedQueryNotSupported(Exception):
    pass


class CannotAlterPreparedStatementQuerySet(Exception):
    pass


class PreparedStatementNotYetExecuted(Exception):
    pass


class StatementNotRegistered(Exception):
    pass
