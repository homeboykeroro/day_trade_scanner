from enum import Enum

class Pattern(str, Enum):
    INITIAL_POP_UP = 'INITIAL_POP_UP',
    NEW_FIVE_MINUTE_CANDLE_HIGH = 'NEW_FIVE_MINUTE_CANDLE_HIGH'