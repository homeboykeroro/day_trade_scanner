from enum import Enum

class DiscordChannel(str, Enum):
    DAY_TRADE_FLOOR = 'DAY_TRADE_FLOOR'
    SWING_TRADE_FLOOR = 'SWING_TRADE_FLOOR'
    CHATBOT_LOG = 'CHATBOT_LOG'