import os
import traceback
import threading
import discord

from utils.logger import Logger

from constant.discord.discord_channel import DiscordChannel

CHATBOT_TOKEN = os.environ['DISCORD_CHATBOT_TOKEN']
DAY_TRADE_FLOOR_CHANNEL_ID = int(os.environ['DISCORD_DAY_TRADE_FLOOR_CHANNEL_ID'])
SWING_TRADE_FLOOR_CHANNEL_ID = int(os.environ['DISCORD_SWING_TRADE_FLOOR_CHANNEL_ID'])
CHATBOT_LOG_CHANNEL_ID = int(os.environ['DISCORD_CHATBOT_LOG_CHANNEL_ID'])
DISCORD_MESSAGE_READ_OUT_CHANNEL_ID = int(os.environ['DISCORD_MESSAGE_READ_OUT_CHANNEL_ID'])

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
        self.__day_trade_floor_channel = None
        self.__swing_trade_floor_channel = None
        self.__chatbot_log_channel = None
        
    @property
    def is_chatbot_ready(self):
        return self.__is_chatbot_ready
    
    @is_chatbot_ready.setter
    def is_chatbot_ready(self, is_chatbot_ready):
        self.__is_chatbot_ready = is_chatbot_ready

    async def on_ready(self):
        self.__is_chatbot_ready = True
        self.__day_trade_floor_channel = self.get_channel(DAY_TRADE_FLOOR_CHANNEL_ID)
        self.__swing_trade_floor_channel = self.get_channel(SWING_TRADE_FLOOR_CHANNEL_ID)
        self.__chatbot_log_channel = self.get_channel(CHATBOT_LOG_CHANNEL_ID)
        logger.log_debug_msg(f'Logged on as {self.user} in Discord', with_std_out=True)

    async def on_message(self, message):
        if message.author == self.user:
            return
    
        await message.channel.send(message.content)
            
    def send_messages_to_channel(self, channel: DiscordChannel, message: str, file_dir: str = None, with_text_to_speech: bool = False):
        if self.__is_chatbot_ready:   
            
            if channel == DiscordChannel.DAY_TRADE_FLOOR:
                channel = self.__day_trade_floor_channel    
            elif channel == DiscordChannel.SWING_TRADE_FLOOR:
                channel = self.__swing_trade_floor_channel
            elif channel == DiscordChannel.CHATBOT_LOG:
                channel = self.__chatbot_log_channel
            else:
                raise Exception('No Discord channel is specified')
            
            if not message:
                raise Exception('No message is sent')
            
            if file_dir:
                if not os.path.isfile(file_dir):
                    raise Exception('Attached file doesn\'t exist')
            
            if channel:
                loop = self.loop
                
                try:
                    if file_dir:
                        loop.create_task(channel.send(message, file=discord.File(file_dir), tts=with_text_to_speech))
                    else:
                        loop.create_task(channel.send(message, tts=with_text_to_speech))
                except Exception as e:
                    logger.log_error_msg(f'Chatbot fatal error, {e}', with_std_out = True)
                    logger.log_error_msg(traceback.format_exc())
            else:
                logger.log_debug_msg('Channel not found. Cannot send message.', with_std_out=True)
        else:
            logger.log_debug_msg('Chatbot is not ready yet', with_std_out=True)

    def run_chatbot(self) -> threading.Thread:
        bot_thread = threading.Thread(target=self.run, name="discord_chatbot_thread", args=(CHATBOT_TOKEN,))
        bot_thread.start()
        return bot_thread