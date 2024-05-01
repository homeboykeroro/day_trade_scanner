from module.discord_chatbot_client import DiscordChatBotClient
from module.stock_screener import StockScreener
#from module.pl_report_generator import PLReportGenerator

from utils.logger import Logger

logger = Logger()

discord_client = DiscordChatBotClient()
stock_screener = StockScreener(discord_client)
#pl_report_generator = PLReportGenerator(discord_client)

def main():  
    discord_client.run_chatbot()
    stock_screener.scan()

if __name__ == '__main__':
    main()
