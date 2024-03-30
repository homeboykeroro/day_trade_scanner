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

# Text to Speech
TEXT_TO_SPEECH_CHANNEL_ID = int(os.environ['DISCORD_TEXT_TO_SPEECH_CHANNEL_ID'])

# For Pattern Analysis
INITIAL_POP_CHANNEL_ID = int(os.environ['DISCORD_INITIAL_POP_CHANNEL_ID'])
INITIAL_DIP_CHANNEL_ID = int(os.environ['DISCORD_INITIAL_DIP_CHANNEL_ID'])
YESTERDAY_BULLISH_DAILY_CANDLE_CHANNEL_ID = int(os.environ['DISCORD_YESTERDAY_BULLISH_DAILY_CANDLE_CHANNEL_ID'])

# Scanner List Log
TOP_GAINER_SCANNER_LIST_CHANNEL_ID = int(os.environ['DISCORD_TOP_GAINER_SCANNER_LIST_CHANNEL_ID'])
TOP_LOSER_SCANNER_LIST_CHANNEL_ID = int(os.environ['DISCORD_TOP_LOSER_SCANNER_LIST_CHANNEL_ID'])
YESTERDAY_TOP_GAINER_SCANNER_LIST_CHANNEL_ID = int(os.environ['DISCORD_YESTERDAY_TOP_GAINER_SCANNER_LIST_CHANNEL_ID'])

# Error Log
CHATBOT_LOG_CHANNEL_ID = int(os.environ['DISCORD_CHATBOT_LOG_CHANNEL_ID'])
CHATBOT_ERROR_LOG_CHANNEL_ID = int(os.environ['DISCORD_CHATBOT_ERROR_LOG_CHANNEL_ID'])

# NAV
NAV_CHANNEL_ID = int(os.environ['DISCORD_NAV_CHANNEL_ID'])

# Banking
CITIBANK_INTERESTS_CHANNEL_ID = int(os.environ['DISCORD_CITIBANK_INTERESTS_CHANNEL_ID'])
HSBC_INTERESTS_CHANNEL_ID = int(os.environ['DISCORD_HSBC_INTERESTS_CHANNEL_ID'])
CITIBANK_NAV_CHANNEL_ID = int(os.environ['DISCORD_CITIBANK_NAV_CHANNEL_ID'])
HSBC_NAV_CHANNEL_ID = int(os.environ['DISCORD_HSBC_NAV_CHANNEL_ID'])

# Firstrade Interests
FIRSTRADE_INTERESTS_CHANNEL_ID = int(os.environ['DISCORD_FIRSTRADE_INTERESTS_CHANNEL_ID'])

# Interactive Brokers Interests
IB_INTERESTS_CHANNEL_ID = int(os.environ['DISCORD_IB_INTERESTS_CHANNEL_ID'])

# Trade Entry and Exit
DAY_TRADE_ENTRY_AND_EXIT_CHANNEL_ID = int(os.environ['DISCORD_DAY_TRADE_ENTRY_AND_EXIT_CHANNEL_ID'])
SWING_TRADE_ENTRY_AND_EXIT_CHANNEL_ID = int(os.environ['DISCORD_SWING_TRADE_ENTRY_AND_EXIT_CHANNEL_ID'])

# Trade History
FIRSTRADE_DAY_TRADE_SUMMARY_CHANNEL_ID = int(os.environ['DISCORD_FIRSTRADE_DAY_TRADE_SUMMARY_CHANNEL_ID'])
FIRSTRADE_SWING_TRADE_SUMMARY_CHANNEL_ID = int(os.environ['DISCORD_FIRSTRADE_SWING_TRADE_SUMMARY_CHANNEL_ID'])
IB_DAY_TRADE_SUMMARY_CHANNEL_ID = int(os.environ['DISCORD_IB_DAY_TRADE_SUMMARY_CHANNEL_ID'])
IB_SWING_TRADE_SUMMARY_CHANNEL_ID = int(os.environ['DISCORD_IB_SWING_TRADE_SUMMARY_CHANNEL_ID'])

# Aggregate Result
DAILY_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_DAILY_PROFIT_AND_LOSS_CHANNEL_ID'])
WEEKLY_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_WEEKLY_PROFIT_AND_LOSS_CHANNEL_ID'])
MONTH_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_MONTH_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID'])
YEAR_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_YEAR_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID'])
MONTHLY_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_MONTHLY_PROFIT_AND_LOSS_CHANNEL_ID'])
YEARLY_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_YEARLY_PROFIT_AND_LOSS_CHANNEL_ID'])

# Firstrade Profit and Loss
FIRSTRADE_DAILY_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_FIRSTRADE_DAILY_PROFIT_AND_LOSS_CHANNEL_ID'])
FIRSTRADE_WEEKLY_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_FIRSTRADE_WEEKLY_PROFIT_AND_LOSS_CHANNEL_ID'])
FIRSTRADE_MONTH_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_FIRSTRADE_MONTH_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID'])
FIRSTRADE_YEAR_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_FIRSTRADE_YEAR_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID'])
FIRSTRADE_MONTHLY_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_FIRSTRADE_MONTHLY_PROFIT_AND_LOSS_CHANNEL_ID'])
FIRSTRADE_YEARLY_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_FIRSTRADE_YEARLY_PROFIT_AND_LOSS_CHANNEL_ID'])

# Interactive Brokers Profit and Loss
IB_DAILY_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_IB_DAILY_PROFIT_AND_LOSS_CHANNEL_ID'])
IB_WEEKLY_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_IB_WEEKLY_PROFIT_AND_LOSS_CHANNEL_ID'])
IB_MONTH_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_IB_MONTH_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID'])
IB_YEAR_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_IB_YEAR_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID'])
IB_MONTHLY_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_IB_MONTHLY_PROFIT_AND_LOSS_CHANNEL_ID'])
IB_YEARLY_PROFIT_AND_LOSS_CHANNEL_ID = int(os.environ['DISCORD_IB_YEARLY_PROFIT_AND_LOSS_CHANNEL_ID'])

# Attachment
DISCORD_ATTACHMENT_UPLOAD_CHANNEL_ID = int(os.environ['DISCORD_ATTACHMENT_UPLOAD_CHANNEL_ID'])

# Testing Channel
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
        
    @property
    def is_chatbot_ready(self):
        return self.__is_chatbot_ready
    
    @is_chatbot_ready.setter
    def is_chatbot_ready(self, is_chatbot_ready):
        self.__is_chatbot_ready = is_chatbot_ready

    async def on_ready(self):
        self.__is_chatbot_ready = True
        
        self.__text_to_speech_channel = self.get_channel(TEXT_TO_SPEECH_CHANNEL_ID)
        
        self.__initial_pop_channel = self.get_channel(INITIAL_POP_CHANNEL_ID)
        self.__initial_dip_channel = self.get_channel(INITIAL_DIP_CHANNEL_ID)
        self.__yesterday_bullish_DAILY_candle_channel = self.get_channel(YESTERDAY_BULLISH_DAILY_CANDLE_CHANNEL_ID)
        
        self.__top_gainer_scanner_list_channel = self.get_channel(TOP_GAINER_SCANNER_LIST_CHANNEL_ID)
        self.__top_loser_scanner_list_channel = self.get_channel(TOP_LOSER_SCANNER_LIST_CHANNEL_ID)
        self.__yesterday_top_scanner_list_channel = self.get_channel(YESTERDAY_TOP_GAINER_SCANNER_LIST_CHANNEL_ID)
        
        self.__chatbot_log_channel = self.get_channel(CHATBOT_LOG_CHANNEL_ID)
        self.__chatbot_error_log_channel = self.get_channel(CHATBOT_ERROR_LOG_CHANNEL_ID)
        
        self.__ib_interests_channel = self.get_channel(IB_INTERESTS_CHANNEL_ID)
        self.__firstrade_interests_channel = self.get_channel(FIRSTRADE_INTERESTS_CHANNEL_ID)
        
        self.__day_trade_entry_and_exit_channel = self.get_channel(DAY_TRADE_ENTRY_AND_EXIT_CHANNEL_ID)
        self.__swing_trade_entry_and_exit_channel = self.get_channel(SWING_TRADE_ENTRY_AND_EXIT_CHANNEL_ID)
        
        self.__firstrade_day_trade_summary_channel = self.get_channel(FIRSTRADE_DAY_TRADE_SUMMARY_CHANNEL_ID)
        self.__firstrade_swing_trade_summary_channel = self.get_channel(FIRSTRADE_SWING_TRADE_SUMMARY_CHANNEL_ID)
        self.__ib_day_trade_summary_channel = self.get_channel(IB_DAY_TRADE_SUMMARY_CHANNEL_ID)
        self.__ib_swing_trade_summary_channel = self.get_channel(IB_SWING_TRADE_SUMMARY_CHANNEL_ID)
        
        self.__nav = self.get_channel(NAV_CHANNEL_ID)
        self.__citibank_nav = self.get_channel(CITIBANK_NAV_CHANNEL_ID)
        self.__hsbc_nav = self.get_channel(HSBC_NAV_CHANNEL_ID)
        
        self.__citibank_interests_channel = self.get_channel(CITIBANK_INTERESTS_CHANNEL_ID)
        self.__hsbc_interests_channel = self.get_channel(HSBC_INTERESTS_CHANNEL_ID)
        
        self.__daily_profit_and_loss_channel = self.get_channel(DAILY_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__weekly_profit_and_loss_channel = self.get_channel(WEEKLY_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__month_to_date_profit_and_loss_channel = self.get_channel(MONTH_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__year_to_date_profit_and_loss_channel = self.get_channel(YEAR_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__monthly_profit_and_loss_channel = self.get_channel(MONTHLY_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__yearly_profit_and_loss_channel = self.get_channel(YEARLY_PROFIT_AND_LOSS_CHANNEL_ID)
        
        self.__firstrade_daily_profit_and_loss_channel = self.get_channel(FIRSTRADE_DAILY_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__firstrade_weekly_profit_and_loss_channel = self.get_channel(FIRSTRADE_WEEKLY_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__firstrade_month_to_date_profit_and_loss_channel = self.get_channel(FIRSTRADE_MONTH_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__firstrade_year_to_date_profit_and_loss_channel = self.get_channel(FIRSTRADE_YEAR_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__firstrade_monthly_profit_and_loss_channel = self.get_channel(FIRSTRADE_MONTHLY_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__firstrade_yearly_profit_and_loss_channel = self.get_channel(FIRSTRADE_YEARLY_PROFIT_AND_LOSS_CHANNEL_ID)
        
        self.__ib_daily_profit_and_loss_channel = self.get_channel(IB_DAILY_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__ib_weekly_profit_and_loss_channel = self.get_channel(IB_WEEKLY_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__ib_month_to_date_profit_and_loss_channel = self.get_channel(IB_MONTH_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__ib_year_to_date_profit_and_loss_channel = self.get_channel(IB_YEAR_TO_DATE_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__ib_monthly_profit_and_loss_channel = self.get_channel(IB_MONTHLY_PROFIT_AND_LOSS_CHANNEL_ID)
        self.__ib_yearly_profit_and_loss_channel = self.get_channel(IB_YEARLY_PROFIT_AND_LOSS_CHANNEL_ID)
        
        self.__attachment_upload_channel = self.get_channel(DISCORD_ATTACHMENT_UPLOAD_CHANNEL_ID)
        
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
        if channel_type == DiscordChannel.TEXT_TO_SPEECH:
            channel = self.__text_to_speech_channel
        elif channel_type == DiscordChannel.INITIAL_POP:
            channel = self.__initial_pop_channel    
        elif channel_type == DiscordChannel.INITIAL_DIP:
            channel = self.__initial_dip_channel
        elif channel_type == DiscordChannel.YESTERDAY_BULLISH_DAILY_CANDLE:
            channel = self.__yesterday_bullish_DAILY_candle_channel
        elif channel_type == DiscordChannel.TOP_GAINER_SCANNER_LIST:
            channel = self.__top_gainer_scanner_list_channel
        elif channel_type == DiscordChannel.TOP_LOSER_SCANNER_LIST:
            channel = self.__top_loser_scanner_list_channel
        elif channel_type == DiscordChannel.YESTERDAY_TOP_GAINER_SCANNER_LIST:
            channel = self.__yesterday_top_scanner_list_channel
        elif channel_type == DiscordChannel.CHATBOT_LOG:
            channel = self.__chatbot_log_channel
        elif channel_type == DiscordChannel.CHATBOT_ERROR_LOG:
            channel = self.__chatbot_error_log_channel
        elif channel_type == DiscordChannel.FIRSTRADE_INTERESTS:
            channel = self.__firstrade_interests_channel
        elif channel_type == DiscordChannel.IB_INTERESTS:
            channel = self.__ib_interests_channel
        elif channel_type == DiscordChannel.DAY_TRADE_ENTRY_AND_EXIT:
            channel = self.__day_trade_entry_and_exit_channel
        elif channel_type == DiscordChannel.SWING_TRADE_ENTRY_AND_EXIT:
            channel = self.__swing_trade_entry_and_exit_channel
        elif channel_type == DiscordChannel.FIRSTRADE_DAY_TRADE_SUMMARY:
            channel = self.__firstrade_day_trade_summary_channel
        elif channel_type == DiscordChannel.FIRSTRADE_SWING_TRADE_SUMMARY:
            channel = self.__firstrade_swing_trade_summary_channel
        elif channel_type == DiscordChannel.IB_DAY_TRADE_SUMMARY:
            channel = self.__ib_day_trade_summary_channel
        elif channel_type == DiscordChannel.IB_SWING_TRADE_SUMMARY:
            channel = self.__ib_swing_trade_summary_channel
        elif channel_type == DiscordChannel.FIRSTRADE_DAILY_PROFIT_AND_LOSS:
            channel = self.__firstrade_daily_profit_and_loss_channel
        elif channel_type == DiscordChannel.FIRSTRADE_WEEKY_PROFIT_AND_LOSS:
            channel = self.__firstrade_weekly_profit_and_loss_channel
        elif channel_type == DiscordChannel.FIRSTRADE_MONTH_TO_DATE_PROFIT_AND_LOSS:
            channel = self.__firstrade_month_to_date_profit_and_loss_channel
        elif channel_type == DiscordChannel.FIRSTRADE_YEAR_TO_DATE_PROFIT_AND_LOSS:
            channel = self.__firstrade_year_to_date_profit_and_loss_channel
        elif channel_type == DiscordChannel.FIRSTRADE_MONTHLY_PROFIT_AND_LOSS:
            channel = self.__firstrade_monthly_profit_and_loss_channel
        elif channel_type == DiscordChannel.FIRSTRADE_YEARLY_PROFIT_AND_LOSS:
            channel = self.__firstrade_yearly_profit_and_loss_channel
        elif channel_type == DiscordChannel.IB_DAILY_PROFIT_AND_LOSS:
            channel = self.__ib_daily_profit_and_loss_channel
        elif channel_type == DiscordChannel.IB_WEEKY_PROFIT_AND_LOSS:
            channel = self.__ib_weekly_profit_and_loss_channel
        elif channel_type == DiscordChannel.IB_MONTH_TO_DATE_PROFIT_AND_LOSS:
            channel = self.__ib_month_to_date_profit_and_loss_channel
        elif channel_type == DiscordChannel.IB_YEAR_TO_DATE_PROFIT_AND_LOSS:
            channel = self.__ib_year_to_date_profit_and_loss_channel
        elif channel_type == DiscordChannel.IB_MONTHLY_PROFIT_AND_LOSS:
            channel = self.__ib_monthly_profit_and_loss_channel
        elif channel_type == DiscordChannel.IB_YEARLY_PROFIT_AND_LOSS:
            channel = self.__ib_yearly_profit_and_loss_channel
        elif channel_type == DiscordChannel.NAV:    
            channel = self.__nav
        elif channel_type == DiscordChannel.CITIBANK_NAV:
            channel = self.__citibank_nav
        elif channel_type == DiscordChannel.HSBC_NAV:
            channel = self.__hsbc_nav 
        elif channel_type == DiscordChannel.CITIBANK_INTERESTS:
            channel = self.__citibank_interests_channel
        elif channel_type == DiscordChannel.HSBC_INTERESTS:
            channel = self.__hsbc_interests_channel
        elif channel_type == DiscordChannel.DAILY_PROFIT_AND_LOSS:
            channel = self.__daily_profit_and_loss_channel
        elif channel_type == DiscordChannel.WEEKLY_PROFIT_AND_LOSS:
            channel = self.__weekly_profit_and_loss_channel
        elif channel_type == DiscordChannel.MONTH_TO_DATE_PROFIT_AND_LOSS:
            channel = self.__month_to_date_profit_and_loss_channel
        elif channel_type == DiscordChannel.YEAR_TO_DATE_PROFIT_AND_LOSS:
            channel = self.__year_to_date_profit_and_loss_channel
        elif channel_type == DiscordChannel.MONTHLY_PROFIT_AND_LOSS:
            channel = self.__monthly_profit_and_loss_channel
        elif channel_type == DiscordChannel.YEARLY_PROFIT_AND_LOSS:
            channel = self.__yearly_profit_and_loss_channel
        elif channel_type == DiscordChannel.ATTACHMENT_UPLOAD:
            channel = self.__attachment_upload_channel
        elif channel_type == DiscordChannel.DEVELOPMENT_TEST:
            channel = self.__development_test_channel
        else:
            raise Exception('No Discord channel is specified')
            
        return channel

    def run_chatbot(self) -> threading.Thread:
        bot_thread = threading.Thread(target=self.run, name="discord_chatbot_thread", args=(CHATBOT_TOKEN,))
        bot_thread.start()
        return bot_thread