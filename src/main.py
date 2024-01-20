from module.discord_chatbot_client import DiscordChatBotClient
from module.stock_screener import StockScreener

from utils.logger import Logger

logger = Logger()

def main():  
    discord_client = DiscordChatBotClient()
    bot_thread = discord_client.run_chatbot()

    while True:
        if discord_client.is_chatbot_ready:
            logger.log_debug_msg('Chatbot is ready', with_std_out=True)
            break
     
    stock_screener = StockScreener(discord_client)
    stock_screener.run_screener()
   
    bot_thread.join()
 
if __name__ == '__main__':
    main()
