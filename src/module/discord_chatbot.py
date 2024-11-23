import os
import asyncio
import threading
from discord.ext import commands
import discord

from model.discord.discord_message import DiscordMessage
from model.discord.view.redirect_button import RedirectButton

from utils.logger import Logger

from constant.discord.discord_message_channel import DiscordMessageChannel

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Text to Speech
TEXT_TO_SPEECH_CHANNEL_ID = int(os.environ['DISCORD_TEXT_TO_SPEECH_CHANNEL_ID'])
CHATBOT_ERROR_LOG_CHANNEL_ID = int(os.environ['DISCORD_CHATBOT_ERROR_LOG_CHANNEL_ID'])

# Screener
SMALL_CAP_TOP_GAINER_SCREENER_LIST_CHANNEL_ID = int(os.environ['DISCORD_SMALL_CAP_TOP_GAINER_SCREENER_LIST_CHANNEL_ID'])
YESTERDAY_TOP_GAINER_SCREENER_LIST_CHANNEL_ID = int(os.environ['DISCORD_YESTERDAY_TOP_GAINER_SCREENER_LIST_CHANNEL_ID'])

# Scanner
SMALL_CAP_INITIAL_POP_CHANNEL_ID = int(os.environ['DISCORD_SMALL_CAP_INITIAL_POP_CHANNEL_ID'])
YESTERDAY_TOP_GAINER_BULLISH_DAILY_CANDLE_CHANNEL_ID = int(os.environ['DISCORD_YESTERDAY_TOP_GAINER_BULLISH_DAILY_CANDLE_CHANNEL_ID'])
SMALL_CAP_INTRA_DAY_BREAKOUT_CHANNEL_ID = int(os.environ['DISCORD_SMALL_CAP_INTRA_DAY_BREAKOUT_CHANNEL_ID'])
IPO_LIST_CHANNEL_ID = int(os.environ['DISCORD_IPO_LIST_CHANNEL_ID'])

# Log
YESTERDAY_TOP_GAINER_SCRAPER_HISTORY_LOG_CHANNEL_ID = int(os.environ['DISCORD_YESTERDAY_TOP_GAINER_SCRAPER_HISTORY_LOG_CHANNEL_ID'])
SERP_API_ACCOUNT_INFO_LOG_CHANNEL_ID = int(os.environ['DISCORD_SERP_API_ACCOUNT_INFO_LOG_CHANNEL_ID'])
SERP_API_SEARCH_QUERY_LOG_CHANNEL_ID = int(os.environ['DISCORD_SERP_API_SEARCH_QUERY_LOG_CHANNEL_ID'])
SERP_API_SEARCH_RESULT_LOG_CHANNEL_ID = int(os.environ['DISCORD_SERP_API_SEARCH_RESULT_LOG_CHANNEL_ID'])

logger = Logger()

# https://hackmd.io/@kangjw/Discordpy%E6%A9%9F%E5%99%A8%E4%BA%BA%E5%BE%9E0%E5%88%B01%E8%B6%85%E8%A9%B3%E7%B4%B0%E6%95%99%E5%AD%B8
intents = discord.Intents.default()
intents.reactions = True
intents.members = True
intents.guilds = True
intents.message_content = True

class DiscordChatBot(discord.Client):
    def __init__(self, token: str, *args, **kwargs):
        self.__is_chatbot_ready = False
        self.__token = token
        
        loop = asyncio.new_event_loop()
        super().__init__(*args, **kwargs, intents=intents, loop=loop)

    @property
    def is_chatbot_ready(self):
        return self.__is_chatbot_ready
    
    @is_chatbot_ready.setter
    def is_chatbot_ready(self, is_chatbot_ready):
        self.__is_chatbot_ready = is_chatbot_ready
           
    async def on_ready(self):
        logger.log_debug_msg(f'Chatbot is ready', with_std_out=True)
        self.__is_chatbot_ready = True
    
    async def on_message(self, message):
        if message.author == self.user:
            return
        
        await message.channel.send(message.content)

    def get_discord_channel(self, channel_type: DiscordMessageChannel):
        channel_id = None
        
        if channel_type == DiscordMessageChannel.TEXT_TO_SPEECH:
            channel_id = TEXT_TO_SPEECH_CHANNEL_ID
        elif channel_type == DiscordMessageChannel.CHATBOT_ERROR_LOG:
            channel_id = CHATBOT_ERROR_LOG_CHANNEL_ID
        elif channel_type == DiscordMessageChannel.SMALL_CAP_INITIAL_POP:
            channel_id = SMALL_CAP_TOP_GAINER_SCREENER_LIST_CHANNEL_ID
        elif channel_type == DiscordMessageChannel.YESTERDAY_TOP_GAINER_BULLISH_DAILY_CANDLE:
            channel_id = YESTERDAY_TOP_GAINER_BULLISH_DAILY_CANDLE_CHANNEL_ID
        elif channel_type == DiscordMessageChannel.SMALL_CAP_INTRA_DAY_BREAKOUT:
            channel_id = SMALL_CAP_INTRA_DAY_BREAKOUT_CHANNEL_ID
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
        
        channel = self.get_channel(channel_id)
        return channel

    async def send_message_to_channel(self,
                                      message: DiscordMessage, 
                                      channel_type: DiscordMessageChannel, 
                                      with_text_to_speech: bool = False): 
        channel = self.get_discord_channel(channel_type)
        
        try:
            msg_param = dict(content=message.content, 
                             embed=message.embed, 
                             view=message.view, 
                             files=message.files, 
                             tts=with_text_to_speech)
            
            if message.jump_url:
                view=RedirectButton(ticker=message.ticker, jump_url=message.jump_url)
                msg_param.update(dict(view=view))
            
            result = await channel.send(**msg_param)
            return result
        except Exception as e:
            logger.log_error_msg(f'Failed to send message to channel {channel_type.value}, {e}')
            return dict(exception=e)
    
    async def add_message_list_to_task(self, message_list: list, channel_type: DiscordMessageChannel, with_text_to_speech: bool = False):
        tasks = [self.send_message_to_channel(message=message, channel_type=channel_type, with_text_to_speech=with_text_to_speech) for message in message_list] 
        result_message_list = await asyncio.gather(*tasks, return_exceptions=True, loop=self.loop)
        
        return result_message_list
    
    def send_message(self, message: DiscordMessage, channel_type: DiscordMessageChannel, with_text_to_speech: bool = False):
        # response does not matter when using this method
        try:
            channel = self.get_discord_channel(channel_type)
            loop = self.loop
            msg_param = dict(content=message.content, 
                             embed=message.embed, 
                             view=message.view, 
                             files=message.files, 
                             tts=with_text_to_speech)

            loop.create_task(channel.send(**msg_param))
        except Exception as e:
            logger.log_error_msg(f'Add message to discord event loop error, {e}')
    
    def send_message_by_list_with_response(self, message_list: list, channel_type: DiscordMessageChannel, with_text_to_speech: bool = False):
        try:
            loop = self.loop
            response = asyncio.run_coroutine_threadsafe(self.add_message_list_to_task(message_list=message_list, channel_type=channel_type, with_text_to_speech=with_text_to_speech), loop)
            try: 
                result_list = response.result() 
                return result_list
            except Exception as ex: 
                print("Get future response error, {ex}")
        except Exception as e:
            logger.log_error_msg(f'Send message by list with response failed, {e}', with_std_out = True)
    
    def run_chatbot(self):
        self.run(self.__token)

        while True:
            if self.__is_chatbot_ready:
                logger.log_debug_msg('Chatbot starts in run_chatbot', with_std_out=True)
                break
        