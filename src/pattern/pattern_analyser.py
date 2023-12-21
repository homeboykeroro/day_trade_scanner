from abc import ABC, abstractmethod

class PatternAnalyser(ABC):
    @abstractmethod
    def analyse(self) -> None:
        return NotImplemented