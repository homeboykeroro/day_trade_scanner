from datetime import datetime
import discord

from model.discord.discord_message import DiscordMessage

from constant.broker import Broker

class InterestProfit(DiscordMessage):
    def __init__(self, settle_date: datetime,
                       interest_value: float,
                       paid_by: Broker):
        super().__init__()
        
        self.__settle_date = settle_date
        self.__interest_value = interest_value
        self.__paid_by = paid_by
        
        embed = discord.Embed(title=f'Interest Summary on {settle_date.strftime("%Y-%m-%d")}\n')
        embed.add_field(name = 'Interest:', value=f'${interest_value}', inline=True)
        embed.add_field(name = 'Paid by:', value=f'{paid_by.value}', inline=False)
        self.embed = embed

    def __members(self):
        return (self.__settle_date, 
                self.__interest_value, 
                self.__paid_by)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, InterestProfit):
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
    def interest_value(self):
        return self.__interest_value
    
    @interest_value.setter
    def interest_value(self, interest_value):
        self.__interest_value = interest_value
    
    @property
    def paid_by(self):
        return self.__paid_by
    
    @paid_by.setter
    def paid_by(self, paid_by):
        self.__paid_by = paid_by