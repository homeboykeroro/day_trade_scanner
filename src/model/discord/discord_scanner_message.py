import datetime
import discord

from constant.pattern import Pattern
from constant.candle.bar_size import BarSize

class DiscordScannerMessage:
    def __init__(self, embed = None, 
                       display_message: discord.File = None, read_out_message: str = None, 
                       candle_chart_list: list = None, 
                       ticker: str = None, 
                       hit_scanner_datetime: datetime = None, 
                       pattern: Pattern = None, 
                       bar_size: BarSize = None):
        self.__embed = embed
        self.__candle_chart_list = candle_chart_list
        self.__display_message = display_message
        self.__read_out_message = read_out_message
        self.__ticker = ticker
        self.__hit_scanner_datetime = hit_scanner_datetime
        self.__pattern = pattern
        self.__bar_size = bar_size
        
    def __members(self):
        return (self.__embed, self.__candle_chart_list, self.__display_message, self.__read_out_message, self.__ticker, self.__hit_scanner_datetime, self.__pattern, self.__bar_size)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DiscordScannerMessage):
            return self.__members() == other.__members()

    def __hash__(self) -> int:
        return hash(self.__members())

    @property
    def embed(self):
        return self.__embed
    
    @embed.setter
    def embed(self, embed):
        self.__embed = embed
    
    @property
    def candle_chart_list(self):
        return self.__candle_chart_list
    
    @candle_chart_list.setter
    def candle_chart_list(self, candle_chart_list):
        self.__candle_chart_list = candle_chart_list
            
    @property
    def display_message(self):
        return self.__display_message
    
    @display_message.setter
    def display_message(self, display_message):
        self.__display_message = display_message
        
    @property
    def read_out_message(self):
        return self.__read_out_message
    
    @read_out_message.setter
    def read_out_message(self, read_out_message):
        self.__read_out_message = read_out_message
        
    @property
    def ticker(self):
        return self.__ticker
    
    @ticker.setter
    def ticker(self, ticker):
        self.__ticker = ticker
        
    @property
    def hit_scanner_datetime(self):
        return self.__hit_scanner_datetime
    
    @hit_scanner_datetime.setter
    def hit_scanner_datetime(self, hit_scanner_datetime):
        self.__hit_scanner_datetime = hit_scanner_datetime
        
    @property
    def pattern(self):
        return self.__pattern
    
    @pattern.setter
    def pattern(self, pattern):
        self.__pattern = pattern
    
    @property
    def bar_size(self):
        return self.__bar_size
    
    @bar_size.setter
    def bar_size(self, bar_size):
        self.__bar_size = bar_size