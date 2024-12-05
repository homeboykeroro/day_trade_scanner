from enum import Enum

class Instrument(str, Enum):
    STOCKS = 'STK'
    OPTIONS = 'OPT'
    FUTURES = 'FUT'
    US_INDICE = 'IND.US'