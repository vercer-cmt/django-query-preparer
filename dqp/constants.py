# Copyright (c) 2020, Vercer Ltd. Rights set out in LICENCE.txt

from collections import UserList
from enum import Enum


class Placeholder:
    def __init__(self, name):
        if "%" in name:
            raise ValueError("Placeholders cannot contain the % symbol")
        self.name = name

    def __repr__(self):
        return "dqp.placeholder.{}".format(self.name)


class ListPlaceholder(UserList):
    def __init__(self, name):
        self.name = name
        self.data = [Placeholder(name)]


class FailureBehaviour(Enum):
    ERROR = "error"
    WARN  = "warn"
