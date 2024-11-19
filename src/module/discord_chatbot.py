import os
import asyncio
from discord.ext import commands
import discord

from model.discord.discord_message import DiscordMessage
from model.discord.view.redirect_button import RedirectButton

from utils.logger import Logger

from constant.discord.discord_message_channel import DiscordMessageChannel

# Text to Speech
TEXT_TO_SPEECH_CHANNEL_ID = int(os.environ['DISCORD_TEXT_TO_SPEECH_CHANNEL_ID'])
CHATBOT_ERROR_LOG_CHANNEL_ID = int(os.environ['DISCORD_CHATBOT_ERROR_LOG_CHANNEL_ID'])

# Screener
SMALL_CAP_TOP_GAINER_SCREENER_LIST_CHANNEL_ID = int(os.environ['DISCORD_SMALL_CAP_TOP_GAINER_SCREENER_LIST_CHANNEL_ID'])
YESTERDAY_TOP_GAINER_SCREENER_LIST_CHANNEL_ID = int(os.environ['DISCORD_YESTERDAY_TOP_GAINER_SCREENER_LIST_CHANNEL_ID'])

# Scanner
SMALL_CAP_INITIAL_POP_CHANNEL_ID = int(os.environ['DISCORD_SMALL_CAP_INITIAL_POP_CHANNEL_ID'])
YESTERDAY_TOP_GAINER_BULLISH_DAILY_CANDLE_CHANNEL_ID = int(os.environ['DISCORD_YESTERDAY_TOP_GAINER_BULLISH_DAILY_CANDLE_CHANNEL_ID'])
IPO_LIST_CHANNEL_ID = int(os.environ['DISCORD_IPO_LIST_CHANNEL_ID'])

# Log
SERP_API_ACCOUNT_INFO_LOG_CHANNEL_ID = int(os.environ['DISCORD_SERP_API_ACCOUNT_INFO_LOG_CHANNEL_ID'])
SERP_API_SEARCH_QUERY_LOG_CHANNEL_ID = int(os.environ['DISCORD_SERP_API_SEARCH_QUERY_LOG_CHANNEL_ID'])
SERP_API_SEARCH_RESULT_LOG_CHANNEL_ID = int(os.environ['DISCORD_SERP_API_SEARCH_RESULT_LOG_CHANNEL_ID'])

# "debugpy.debugJustMyCode": false
logger = Logger()

intents = discord.Intents.default()
intents.reactions = True
intents.members = True
intents.guilds = True
intents.message_content = True

bot = commands.Bot(command_prefix='!', intents=intents)

class DiscordChatBot():
    def __init__(self):
        self.__is_chatbot_ready = False
    
    @property
    def is_chatbot_ready(self):
        return self.__is_chatbot_ready
    
    @is_chatbot_ready.setter
    def is_chatbot_ready(self, is_chatbot_ready):
        self.__is_chatbot_ready = is_chatbot_ready
           
    @bot.event
    async def on_ready(self):
        self.__is_chatbot_ready = True

    # https://discordpy.readthedocs.io/en/stable/ext/commands/commands.html#parameters
    async def send_message_to_channel(self,
                                      message: DiscordMessage, 
                                      channel_type: DiscordMessageChannel, 
                                      with_text_to_speech: bool = False): 
        channel_id = None
        
        if channel_type == DiscordMessageChannel.TEXT_TO_SPEECH:
            channel_id = TEXT_TO_SPEECH_CHANNEL_ID
        elif channel_type == DiscordMessageChannel.CHATBOT_ERROR_LOG:
            channel_id = CHATBOT_ERROR_LOG_CHANNEL_ID
        elif channel_type == DiscordMessageChannel.SMALL_CAP_INITIAL_POP:
            channel_id = SMALL_CAP_TOP_GAINER_SCREENER_LIST_CHANNEL_ID
        elif channel_type == DiscordMessageChannel.YESTERDAY_TOP_GAINER_BULLISH_DAILY_CANDLE:
            channel_id = YESTERDAY_TOP_GAINER_BULLISH_DAILY_CANDLE_CHANNEL_ID
        elif channel_type == DiscordMessageChannel.IPO_LIST:
            channel_id = IPO_LIST_CHANNEL_ID
        elif channel_type == DiscordMessageChannel.SERP_API_ACCOUNT_INFO_LOG:
            channel_id = SERP_API_ACCOUNT_INFO_LOG_CHANNEL_ID
        elif channel_type == DiscordMessageChannel.SERP_API_SEARCH_QUERY_LOG:
            channel_id = SERP_API_SEARCH_QUERY_LOG_CHANNEL_ID
        elif channel_type == DiscordMessageChannel.SERP_API_SEARCH_RESULT_LOG:
            channel_id = SERP_API_SEARCH_RESULT_LOG_CHANNEL_ID
        elif channel_type == DiscordMessageChannel.YESTERDAY_TOP_GAINER_SCREENER_LIST:
            channel_id = YESTERDAY_TOP_GAINER_SCREENER_LIST_CHANNEL_ID
        
        if not channel_id:
            raise Exception(f"Channel ID not found for: {channel_type}")
        
        channel = bot.get_channel(channel_id) 
        
        if channel: 
            msg_param = dict(content=message.content, 
                             embed=message.embed, 
                             view=message.view, 
                             files=message.files, 
                             tts=with_text_to_speech)
            
            if message.jump_url:
                view=RedirectButton(ticker=message.ticker, jump_url=message.jump_url)
                msg_param.update(dict(view=view))
            
            await channel.send(**msg_param)
    
    async def add_message_list_to_task(self, message_list: list, channel_type: DiscordMessageChannel, with_text_to_speech: bool = False):
        tasks = [self.send_message_to_channel(message=message, channel_type=channel_type, with_text_to_speech=with_text_to_speech) for message in message_list] 
        result_message_list = await asyncio.gather(*tasks)
        
        return result_message_list
    
    def send_message_by_list_with_response(self, message_list: list, channel_type: DiscordMessageChannel):
        try:
            asyncio.run(self.add_message_list_to_task(message_list=message_list, channel_type=channel_type))
        except Exception as e:
            logger.log_error_msg(f'Send message by list with response failed, {e}', with_std_out = True)
    
    def run_chatbot(self, chatbot_token: str):
        # such high level api asyncio should handle event loop itself
        asyncio.run(bot.start(chatbot_token))
        
        while True:
            if self.__is_chatbot_ready:
                logger.log_debug_msg('Chatbot is ready', with_std_out=True)
                break