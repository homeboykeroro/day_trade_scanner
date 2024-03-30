from enum import Enum

class ScatterSymbol(str, Enum):
    POP = 'D'
    DIP = 'o'
    BUY = '>'
    SELL = '<'