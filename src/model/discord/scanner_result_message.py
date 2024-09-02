import datetime
import os
import discord

from model.ib.contract_info import ContractInfo
from model.financial_data import FinancialData
from model.offering_news import OfferingNews
from model.discord.discord_message import DiscordMessage

from constant.pattern import Pattern
from constant.candle.bar_size import BarSize

class ScannerResultMessage(DiscordMessage):
    def __init__(self, title,
                       readout_msg,
                       close, 
                       yesterday_close,
                       volume: int = None, total_volume: int = None,
                       contract_info: ContractInfo = None,
                       financial_data: FinancialData = None,
                       offering_news: OfferingNews = None,
                       chart_dir: str = None, 
                       ticker: str = None,
                       hit_scanner_datetime: datetime = None, 
                       pattern: Pattern = None, 
                       bar_size: BarSize = None):
        super().__init__(ticker=ticker)
        embed = discord.Embed(title=f'{title}')
        embed.add_field(name = 'Close:', value= f'${close}', inline = True)
        embed.add_field(name = 'Yesterday Close:', value = f'${yesterday_close}', inline = True)
        embed.add_field(name = chr(173), value = chr(173), inline = True)
        
        if (not volume) and total_volume:
            embed.add_field(name = 'Total Volume:', value = f'{"{:,}".format(int(total_volume))}', inline = True)
            embed.add_field(name = chr(173), value = chr(173), inline = True)
            embed.add_field(name = chr(173), value = chr(173), inline = True)
        else:
            embed.add_field(name = 'Volume:', value = f'{"{:,}".format(int(volume))}', inline = True)
            embed.add_field(name = 'Total Volume:', value = f'{"{:,}".format(int(total_volume))}', inline = True)
            embed.add_field(name = chr(173), value = chr(173), inline = True)
        
        candle_chart_list = []
        
        if chart_dir:
            embed.set_image(url=f"attachment://{os.path.basename(chart_dir)}")
            candle_chart_list.append(discord.File(chart_dir, filename=os.path.basename(chart_dir)))
        
        if contract_info:
            contract_info.add_contract_info_to_embed_msg(embed)

        if financial_data:
            financial_data.add_financials_to_embed_msg(embed)
        
        if offering_news:
            offering_news.add_offering_news_to_embed_msg(embed)
        
        self.__ticker = ticker
        self.embed = embed
        self.__readout_msg = readout_msg
        self.files = candle_chart_list
        self.__hit_scanner_datetime = hit_scanner_datetime
        self.__pattern = pattern
        self.__bar_size = bar_size
        
    def __members(self):
        return (self.__readout_msg, self.__ticker, self.__hit_scanner_datetime, self.__pattern, self.__bar_size)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ScannerResultMessage):
            return self.__members() == other.__members()

    def __hash__(self) -> int:
        return hash(self.__members())
    
    @property
    def readout_msg(self):
        return self.__readout_msg
    
    @readout_msg.setter
    def readout_msg(self, readout_msg):
        self.__readout_msg = readout_msg
        
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
        