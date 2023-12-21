from module.discord_chatbot_client import DiscordChatBotClient
from module.scanner import Scanner
from utils.logger import Logger

idle_msg_logged = False
connected_msg_logged = False
logger = Logger()

ticker_to_previous_day_data_dict = {}
ticker_to_contract_dict = {}

REFRESH_INTERVAL = 20

def main():  
    discord_client = DiscordChatBotClient()
    bot_thread = discord_client.run_chatbot()

    while True:
        if discord_client.is_chatbot_ready:
            break
      
    scanner = Scanner(discord_client)
    scanner.run_scanner()
        
    bot_thread.join()

if __name__ == '__main__':
    main()
