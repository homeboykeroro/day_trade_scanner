from module.discord_chatbot import DiscordChatBot
from datasource.ib_connector import IBConnector

from utils.logger import Logger

logger = Logger()
ib_connector = IBConnector()

# DIA 73128548
# SPY 756733
# QQQ 320227571
# USD/HKD 12345777
# HKD/JPY 15016090
def main():
    one_minute_candle_df = ib_connector.fetch_intra_day_minute_candle(contract_list=[dict(con_id=12345777, symbol='USD')])
    logger.log_debug_msg()

if __name__ == '__main__':
    main()