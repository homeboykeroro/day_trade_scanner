from enum import Enum

class ScatterSymbol(str, Enum):
    POP = 'D'
    DIP = 'o'
    SUPPORT = 'v'
    NEW_HIGH_TEST = '^'
    BUY = '>'
    SELL = '<'