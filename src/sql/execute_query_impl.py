import abc

from oracledb import Cursor

class ExecuteQueryImpl(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def execute(self, cursor: Cursor, params):
        pass
