from datetime import datetime
import discord

from model.discord.discord_message import DiscordMessage

from constant.broker import Broker

class MonthToDateProfitAndLoss(DiscordMessage):
    def __init__(self, settle_date: datetime = None,
                       realised_pl: float = None,
                       account_value: float = None,
                       trading_platform: Broker = None):
        super().__init__()
        
        self.__settle_date = settle_date
        self.__realised_pl = realised_pl
        self.__account_value = account_value
        self.__trading_platform = trading_platform
        
        realised_pl_display = f'${realised_pl}' if realised_pl > 0 else f'-${abs(realised_pl)}'
        
        embed = discord.Embed(title=f'Month to Date Realised Profit on {settle_date.strftime("%Y-%m-%d")}\n')
        embed.add_field(name = 'Realised:', value=f'{realised_pl_display}', inline=True)
        
        if account_value:
            account_growth_display = round((realised_pl/ (account_value - realised_pl)) * 100, 1)
            embed.add_field(name = 'Account Growth(%):', value=f'{account_growth_display}%', inline=False)
        
        if trading_platform:
            embed.add_field(name = 'Trading Platform:', value=f'{trading_platform.value}', inline=False)
        
        self.embed = embed

    def __members(self):
        return (self.__settle_date, 
                self.__realised_pl, 
                self.__account_value, 
                self.__trading_platform)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, MonthToDateProfitAndLoss):
            return self.__members() == other.__members()

    def __hash__(self) -> int:
        return hash(self.__members())

    @property
    def settle_date(self):
        return self.__settle_date
    
    @settle_date.setter
    def settle_date(self, settle_date):
        self.__settle_date = settle_date

    @property
    def realised_pl(self):
        return self.__realised_pl
    
    @realised_pl.setter
    def realised_pl(self, realised_pl):
        self.__realised_pl = realised_pl
        
    @property
    def account_value(self):
        return self.__account_value
    
    @account_value.setter
    def account_value(self, account_value):
        self.__account_value = account_value
        
    @property
    def trading_platform(self):
        return self.__trading_platform
    
    @trading_platform.setter
    def trading_platform(self, trading_platform):
        self.__trading_platform = trading_platform