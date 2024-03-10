from enum import Enum

class Broker(str, Enum):
    FIRSTRADE = 'Firstrade'
    IB = 'Interactive Brokers'
    