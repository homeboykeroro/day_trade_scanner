
import discord
from model.discord.discord_message import DiscordMessage
from model.ib.contract_info import ContractInfo

from constant.broker import Broker

class EntryAndExit(DiscordMessage):
    def __init__(self,
                 ticker: str,
                 realised_pl_list: list, realised_pl_percent_list: list,
                 buy_quantity_list: list, sell_quantity_list: list,
                 buy_datetime_list: list, sell_datetime_list: list,
                 contract_info: ContractInfo,
                 trading_platform: Broker):
        super().__init__(ticker=ticker)
        self.__trading_platform = trading_platform
        
        total_realised_pl = 0
        for realised_pl in realised_pl_list:
            total_realised_pl += realised_pl
        self.__total_realised_pl = round(realised_pl)
        
        self.__realised_pl_list = realised_pl_list
        self.__realised_pl_percent_list = realised_pl_percent_list
        self.__buy_quantity_list = buy_quantity_list
        self.__sell_quantity_list = sell_quantity_list 
        self.__buy_datetime_list = buy_datetime_list
        self.__sell_datetime_list = sell_datetime_list
        
        day_trade_record_date = self.__sell_datetime_list[0]
        
        total_bought_quantity = 0
        total_sold_quantity = 0
        
        for bought_quantity in buy_quantity_list:
            total_bought_quantity += bought_quantity
            
        for sold_quantity in sell_quantity_list:
            total_sold_quantity += sold_quantity
        
        dt_list = buy_datetime_list + sell_datetime_list
        list.sort(dt_list)
        self.__candle_retrieval_start_datetime = dt_list[0]
        self.__candle_retrieval_end_datetime = dt_list[-1]
        
        realised_pl_display = f'${self.__total_realised_pl}' if self.__total_realised_pl > 0 else f'-${abs(self.__total_realised_pl)}'
        
        embed = discord.Embed(title=f"${self.ticker} Trade Entry and Exit Summary {day_trade_record_date.strftime('%Y-%m-%d')}\n")
        embed.add_field(name = 'Realised P&L:', value=realised_pl_display, inline = True)
        embed.add_field(name = chr(173), value = chr(173), inline=True)
        embed.add_field(name = chr(173), value = chr(173), inline=True)
        
        # embed.add_field(name = 'Total Shares Traded:', value=total_bought_quantity, inline = True)
        # embed.add_field(name = 'Remaining Positions:', value=(total_bought_quantity - total_sold_quantity), inline = True)
        # embed.add_field(name = chr(173), value = chr(173), inline=True)
        
        embed.add_field(name = 'Trading Platform:', value=f'{trading_platform.value}', inline=True)
        embed.add_field(name = chr(173), value = chr(173), inline=True)
        embed.add_field(name = chr(173), value = chr(173), inline=True)
        self.embed = embed
        
        #contract_info.add_contract_info_to_embed_msg(embed)
    
    def __members(self):
        return (self.__candle_retrieval_start_datetime,
                self.__candle_retrieval_end_datetime,
                self.__buy_datetime_list,
                self.__sell_datetime_list,
                self.__chart_dir)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, EntryAndExit):
            return self.__members() == other.__members()

    def __hash__(self) -> int:
        return hash(self.__members())
    
    @property
    def total_realised_pl(self):
        return self.__total_realised_pl
    
    @total_realised_pl.setter
    def total_realised_pl(self, total_realised_pl):
        self.__total_realised_pl = total_realised_pl
    
    @property
    def realised_pl_list(self):
        return self.__realised_pl_list
    
    @realised_pl_list.setter
    def realised_pl_list(self, realised_pl_list):
        self.__realised_pl_list = realised_pl_list
        
    @property
    def realised_pl_percent_list(self):
        return self.__realised_pl_percent_list
    
    @realised_pl_percent_list.setter
    def realised_pl_percent_list(self, realised_pl_percent_list):
        self.__realised_pl_percent_list = realised_pl_percent_list
    
    @property
    def buy_quantity_list(self):
        return self.__buy_quantity_list
    
    @buy_quantity_list.setter
    def buy_quantity_list(self, buy_quantity_list):
        self.__buy_quantity_list = buy_quantity_list
        
    @property
    def sell_quantity_list(self):
        return self.__sell_quantity_list
    
    @sell_quantity_list.setter
    def sell_quantity_list(self, sell_quantity_list):
        self.__sell_quantity_list = sell_quantity_list
    
    @property
    def buy_datetime_list(self):
        return self.__buy_datetime_list
    
    @buy_datetime_list.setter
    def buy_datetime_list(self, buy_datetime_list):
        self.__buy_datetime_list = buy_datetime_list
    
    @property
    def sell_datetime_list(self):
        return self.__sell_datetime_list
    
    @sell_datetime_list.setter
    def sell_datetime_list(self, sell_datetime_list):
        self.__sell_datetime_list = sell_datetime_list
    
    @property
    def trading_platform(self):
        return self.__trading_platform
    
    @trading_platform.setter
    def trading_platform(self, trading_platform):
        self.__trading_platform = trading_platform
    
    @property
    def candle_retrieval_start_datetime(self):
        return self.__candle_retrieval_start_datetime
    
    @candle_retrieval_start_datetime.setter
    def candle_retrieval_start_datetime(self, candle_retrieval_start_datetime):
        self.__candle_retrieval_start_datetime = candle_retrieval_start_datetime
    
    @property
    def candle_retrieval_end_datetime(self):
        return self.__candle_retrieval_end_datetime
    
    @candle_retrieval_end_datetime.setter
    def candle_retrieval_end_datetime(self, candle_retrieval_end_datetime):
        self.__candle_retrieval_end_datetime = candle_retrieval_end_datetime
    
    @property
    def chart_dir(self):
        return self.__chart_dir
    
    @chart_dir.setter
    def chart_dir(self, chart_dir):
        self.__chart_dir = chart_dir