#!/usr/bin/env python3

from enum import Enum
import pickle
from gprebalance_modules.planner import *

class RebalanceStep:
    class Status(Enum):
        APPROVE_REQUIERED = 1
        PLANNED = 2
        IN_PROGRESS = 3
        ERROR = 4
        ROLLBACK_PLANNED = 5
        ROLLED_BACK = 6
        CANCELLED = 7
        DONE = 8

    def __init__(self, id: int, move: LogicalMove):
        self.id = id
        self.move = move
        self.status = self.Status.PLANNED

    def __str__(self):
        return (
            f"id: {self.getId()}, status: {self.getStatus()}"
        )

    def getId(self):
        return self.id

    def getStatus(self):
        return self.status

    def getMove(self):
        return self.move

    def setStatus(self, status: Status):
        self.status = status

    def serializeStep(self) -> bytes:
        return pickle.dumps(self)

class RebalanceStepMoveMirror(RebalanceStep):
    def __init__(self, id: int, move: LogicalMove):
        super().__init__(id, move)

    def __str__(self):
        return (
            f"RebalanceStepMoveMirror - {super().__str__()}:\n"
            f"{str(self.move)}"
        )

class RebalanceStepSwitchoverToMirror(RebalanceStep):
    def __init__(self, id: int, move: LogicalMove):
        super().__init__(id, move)
        self.status = self.Status.APPROVE_REQUIERED

    def __str__(self):
        return (
            f"RebalanceStepSwitchoverToMirror - {super().__str__()}"
        )

class RebalanceStepSwitchoverToPrimary(RebalanceStep):
    def __init__(self, id: int, move: LogicalMove):
        super().__init__(id, move)
        self.status = self.Status.APPROVE_REQUIERED

    def __str__(self):
        return (
            f"RebalanceStepSwitchoverToPrimary - {super().__str__()}"
        )

def deserializeStep(input: bytes) -> RebalanceStep:
    return pickle.loads(input)
