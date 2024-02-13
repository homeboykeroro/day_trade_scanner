import os
import time
import asyncio
import threading
import discord

from model.discord.discord_message import DiscordMessage
from model.discord.view.redirect_button import RedirectButton

from utils.logger import Logger

from constant.discord.discord_channel import DiscordChannel

CHATBOT_TOKEN = os.environ['DISCORD_CHATBOT_TOKEN']
INITIAL_POP_CHANNEL_ID = int(os.environ['DISCORD_INITIAL_POP_CHANNEL_ID'])
INITIAL_DIP_CHANNEL_ID = int(os.environ['DISCORD_INITIAL_DIP_CHANNEL_ID'])
TOP_GAINER_SCANNER_LIST_CHANNEL_ID = int(os.environ['DISCORD_TOP_GAINER_SCANNER_LIST_CHANNEL_ID'])
TOP_LOSER_SCANNER_LIST_CHANNEL_ID = int(os.environ['DISCORD_TOP_LOSER_SCANNER_LIST_CHANNEL_ID'])
YESTERDAY_TOP_GAINER_SCANNER_LIST_CHANNEL_ID = int(os.environ['DISCORD_YESTERDAY_TOP_GAINER_SCANNER_LIST_CHANNEL_ID'])
TEXT_TO_SPEECH_CHANNEL_ID = int(os.environ['DISCORD_TEXT_TO_SPEECH_CHANNEL_ID']) #bug fix
CHATBOT_LOG_CHANNEL_ID = int(os.environ['DISCORD_CHATBOT_LOG_CHANNEL_ID'])
CHATBOT_ERROR_LOG_CHANNEL_ID = int(os.environ['DISCORD_CHATBOT_ERROR_LOG_CHANNEL_ID'])
DEVELOPMENT_TEST_CHANNEL_ID = int(os.environ['DISCORD_DEVELOPMENT_TEST_CHANNEL_ID'])

logger = Logger()

intents = discord.Intents.default()
intents.reactions = True
intents.members = True
intents.guilds = True
intents.message_content = True

class DiscordChatBotClient(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, intents=intents)
        self.__is_chatbot_ready = False
        self.__initial_pop_channel = None
        self.__initial_dip_channel = None
        self.__chatbot_log_channel = None
        
    @property
    def is_chatbot_ready(self):
        return self.__is_chatbot_ready
    
    @is_chatbot_ready.setter
    def is_chatbot_ready(self, is_chatbot_ready):
        self.__is_chatbot_ready = is_chatbot_ready

    async def on_ready(self):
        self.__is_chatbot_ready = True
        self.__initial_pop_channel = self.get_channel(INITIAL_POP_CHANNEL_ID)
        self.__initial_dip_channel = self.get_channel(INITIAL_DIP_CHANNEL_ID)
        self.__top_gainer_scanner_list = self.get_channel(TOP_GAINER_SCANNER_LIST_CHANNEL_ID)
        self.__top_loser_scanner_list = self.get_channel(TOP_LOSER_SCANNER_LIST_CHANNEL_ID)
        self.__yesterday_top_scanner_list = self.get_channel(YESTERDAY_TOP_GAINER_SCANNER_LIST_CHANNEL_ID)
        self.__text_to_speech_channel = self.get_channel(TEXT_TO_SPEECH_CHANNEL_ID)
        self.__chatbot_log_channel = self.get_channel(CHATBOT_LOG_CHANNEL_ID)
        self.__chatbot_error_log_channel = self.get_channel(CHATBOT_ERROR_LOG_CHANNEL_ID)
        self.__development_test_channel = self.get_channel(DEVELOPMENT_TEST_CHANNEL_ID)
        logger.log_debug_msg(f'Logged on as {self.user} in Discord', with_std_out=True)

    async def on_message(self, message):
        if message.author == self.user:
            return
    
        await message.channel.send(message.content)
    
    def send_message(self, message: DiscordMessage, channel_type: DiscordChannel, with_text_to_speech: bool = False):
        loop = self.loop
        channel = self.__get_channel(channel_type)

        try:
            msg_param = dict(content=message.content, embed=message.embed, view=message.view, files=message.files, tts=with_text_to_speech)
            
            if message.jump_url:
                view=RedirectButton(ticker=message.ticker, jump_url=message.jump_url)
                msg_param.update(dict(view=view))
            
            loop.create_task(channel.send(**msg_param))
        except Exception as e:
            logger.log_error_msg(f'Failed to send message to channel, {e}', with_std_out = True)
            
    def send_message_by_list(self, message_list: list, channel_type: DiscordChannel, with_text_to_speech: bool = False, delay: float = None):
        try:
            for message in message_list:
                self.send_message(message=message, channel_type=channel_type, with_text_to_speech=with_text_to_speech)
                
                if delay:
                    time.sleep(delay)
                
        except Exception as e:
            logger.log_error_msg(f'Failed to send message by list, {e}', with_std_out = True)
        
    def send_message_by_list_with_response(self, message_list: list, channel_type: DiscordChannel, with_text_to_speech: bool = False):
        try:
            result_message_list = asyncio.run(self.add_send_message_task(message_list, channel_type, with_text_to_speech))
            return result_message_list
        except Exception as e:
            logger.log_error_msg(f'Send message by list with response failed, {e}', with_std_out = True)
        
    async def add_send_message_task(self, message_list: list, channel_type: DiscordChannel, with_text_to_speech: bool = False):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        tasks = []
        
        for message in message_list:
            msg_param = dict(content=message.content, embed=message.embed, view=message.view, files=message.files, tts=with_text_to_speech)
            
            if message.jump_url:
                view=RedirectButton(ticker=message.ticker, jump_url=message.jump_url)
                msg_param.update(dict(view=view))
            
            task = loop.create_task(self.send_message_to_channel_coro(msg_param,
                                                                      channel_type=channel_type))
            tasks.append(task)

        result_message_list = await asyncio.gather(*tasks, return_exceptions=True)
        return result_message_list
    
    async def send_message_to_channel_coro(self, msg_param: dict, channel_type: DiscordChannel = None):
        loop = self.loop
        channel = self.__get_channel(channel_type)
        
        task = loop.create_task(channel.send(**msg_param))
            
        while True:
            if task.done():
                break
            else:
                await asyncio.sleep(0.1)
            
        return task.result()

    def __get_channel(self, channel_type: DiscordChannel):
        if channel_type == DiscordChannel.INITIAL_POP:
            channel = self.__initial_pop_channel    
        elif channel_type == DiscordChannel.INITIAL_DIP:
            channel = self.__initial_dip_channel
        elif channel_type == DiscordChannel.TOP_GAINER_SCANNER_LIST:
            channel = self.__top_gainer_scanner_list
        elif channel_type == DiscordChannel.TOP_LOSER_SCANNER_LIST:
            channel = self.__top_loser_scanner_list
        elif channel_type == DiscordChannel.YESTERDAY_TOP_GAINER_SCANNER_LIST:
            channel = self.__yesterday_top_scanner_list
        elif channel_type == DiscordChannel.TEXT_TO_SPEECH:
            channel = self.__text_to_speech_channel
        elif channel_type == DiscordChannel.CHATBOT_LOG:
            channel = self.__chatbot_log_channel
        elif channel_type == DiscordChannel.CHATBOT_ERROR_LOG:
            channel = self.__chatbot_error_log_channel
        elif channel_type == DiscordChannel.DEVELOPMENT_TEST:
            channel = self.__development_test_channel
        else:
            raise Exception('No Discord channel is specified')
            
        return channel

    def run_chatbot(self) -> threading.Thread:
        bot_thread = threading.Thread(target=self.run, name="discord_chatbot_thread", args=(CHATBOT_TOKEN,))
        bot_thread.start()
        return bot_thread