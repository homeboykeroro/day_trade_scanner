import datetime
import discord

from model.discord.discord_message import DiscordMessage
from model.ib.contract_info import ContractInfo

from constant.broker import Broker

class TradeProfitAndLoss(DiscordMessage):
    def __init__(self, ticker: str, 
                       acquired_date: datetime.datetime, sold_date: datetime.datetime,
                       accumulated_shares: int, sell_quantity: int, remaining_positions: int,
                       avg_entry_price: float, avg_exit_price: float,
                       accumulated_cost: float, adjusted_cost: float,
                       market_value: float,
                       realised_pl_percent: float, realised_pl: float, 
                       trading_platform: Broker,
                       contract_info: ContractInfo = None):
        self.__ticker = ticker
        self.__acquired_date = acquired_date
        self.__sold_date = sold_date
        
        self.__realised_pl = realised_pl
        self.__realised_pl_percent = realised_pl_percent
        
        self.__accumulated_cost = accumulated_cost
        self.__adjusted_cost = adjusted_cost
        self.__market_value = market_value
        
        self.__accumulated_shares = accumulated_shares
        self.__sell_quantity = sell_quantity
        self.__remaining_positions = remaining_positions
        
        self.__avg_entry_price = avg_entry_price
        self.__avg_exit_price = avg_exit_price
        
        self.__trading_platform = trading_platform
        
        self.__contract_info = contract_info
        
        super().__init__(ticker=ticker)
        trade_date_display = f"on {acquired_date.strftime('%Y-%m-%d')}" if (acquired_date == sold_date) else f"from {acquired_date.strftime('%Y-%m-%d')} to {sold_date.strftime('%Y-%m-%d')}"
        realised_pl_display = f'${realised_pl}' if realised_pl > 0 else f'-${abs(realised_pl)}'
        
        embed = discord.Embed(title=f'${self.ticker} Trade Summary {trade_date_display}\n')
        embed.add_field(name = 'Realised P&L:', value=realised_pl_display, inline = True)
        embed.add_field(name = 'Realised P&L(%):', value=f'{realised_pl_percent}%', inline = True)
        embed.add_field(name = chr(173), value = chr(173), inline=True)
        
        embed.add_field(name = 'Accumulated Buy Cost:', value=f'${round(accumulated_cost)}', inline = True)
        embed.add_field(name = 'Adjusted Cost:', value=f'${round(adjusted_cost)}', inline = True)
        embed.add_field(name = 'Adjusted Market Value:', value=f'${(market_value)}', inline=True)
        
        embed.add_field(name = 'Total Shares:', value=accumulated_shares, inline = True)
        embed.add_field(name = 'Sell Quantity:', value=sell_quantity, inline = True)
        embed.add_field(name = 'Remaining Positions:', value=remaining_positions, inline = True)
        
        embed.add_field(name = 'Avg Entry Price:', value=f'${avg_entry_price}', inline = True)
        embed.add_field(name = 'Avg Exit Price:', value=f'${avg_exit_price}', inline = True)
        embed.add_field(name = chr(173), value = chr(173), inline=True)
        
        embed.add_field(name = 'Trading Platform:', value=f'{trading_platform.value}', inline=True)
        embed.add_field(name = chr(173), value = chr(173), inline=True)
        embed.add_field(name = chr(173), value = chr(173), inline=True)
        self.embed = embed
        
        #contract_info.add_contract_info_to_embed_msg(embed)

    def __members(self):
        return (self.__ticker, 
                self.__acquired_date, self.__sold_date, 
                self.__realised_pl, self.__realised_pl_percent,
                self.__accumulated_cost, self.__adjusted_cost,
                self.__market_value,
                self.__accumulated_shares, self.__sell_quantity, self.__remaining_positions,
                self.__avg_entry_price, self.__avg_exit_price, 
                self.__contract_info)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, TradeProfitAndLoss):
            return self.__members() == other.__members()

    def __hash__(self) -> int:
        return hash(self.__members())

    @property
    def ticker(self):
        return self.__ticker
    
    @ticker.setter
    def ticker(self, ticker):
        self.__ticker = ticker
    
    @property
    def acquired_date(self):
        return self.__acquired_date
    
    @acquired_date.setter
    def acquired_date(self, acquired_date):
        self.__acquired_date = acquired_date
        
    @property
    def sold_date(self):
        return self.__sold_date
    
    @sold_date.setter
    def sold_date(self, sold_date):
        self.__sold_date = sold_date
    
    @property
    def realised_pl(self):
        return self.__realised_pl
    
    @realised_pl.setter
    def realised_pl(self, realised_pl):
        self.__realised_pl = realised_pl
        
    @property
    def realised_pl_percent(self):
        return self.__realised_pl_percent
    
    @realised_pl_percent.setter
    def realised_pl_percent(self, realised_pl_percent):
        self.__realised_pl_percent = realised_pl_percent    
    
    @property
    def accumulated_cost(self):
        return self.__accumulated_cost
    
    @accumulated_cost.setter
    def accumulated_cost(self, accumulated_cost):
        self.__accumulated_cost = accumulated_cost
        
    @property
    def adjusted_cost(self):
        return self.__adjusted_cost
    
    @adjusted_cost.setter
    def adjusted_cost(self, adjusted_cost):
        self.__adjusted_cost = adjusted_cost
        
    @property
    def market_value(self):
        return self.__market_value
    
    @market_value.setter
    def market_value(self, market_value):
        self.__market_value = market_value
    
    @property
    def total_share(self):
        return self.__total_share
    
    @total_share.setter
    def total_share(self, total_share):
        self.__total_share = total_share
        
    @property
    def sell_quantity(self):
        return self.__sell_quantity
    
    @sell_quantity.setter
    def sell_quantity(self, sell_quantity):
        self.__sell_quantity = sell_quantity

    @property
    def remaining_position(self):
        return self.__remaining_position
    
    @remaining_position.setter
    def remaining_position(self, remaining_position):
        self.__remaining_position = remaining_position
        
    @property
    def avg_entry_price(self):
        return self.__avg_entry_price
    
    @avg_entry_price.setter
    def avg_entry_price(self, avg_entry_price):
        self.__avg_entry_price = avg_entry_price
        
    @property
    def avg_exit_price(self):
        return self.__avg_exit_price
    
    @avg_exit_price.setter
    def avg_exit_price(self, avg_exit_price):
        self.__avg_exit_price = avg_exit_price
    
    @property
    def trading_platform(self):
        return self.__trading_platform
    
    @trading_platform.setter
    def trading_platform(self, trading_platform):
        self.__trading_platform = trading_platform
    
    @property
    def contract_info(self):
        return self.__contract_info
    
    @contract_info.setter
    def contract_info(self, contract_info):
        self.__contract_info = contract_info