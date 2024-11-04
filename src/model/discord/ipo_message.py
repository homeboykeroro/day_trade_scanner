import discord

from model.financial_data import FinancialData
from model.discord.discord_message import DiscordMessage

class IPOMessage(DiscordMessage):
    def __init__(self, title,
                       readout_msg,
                       offering_price,
                       offering_shares,
                       offering_amount,
                       description,
                       company_website,
                       ticker: str = None):
        super().__init__(ticker=ticker)
        embed = discord.Embed(title=f'{title}')
        
        embed.add_field(name = 'Ticker:', value= f'{ticker}', inline = True)
        embed.add_field(name = 'Offering Price:', value=f'{"{:,}".format(float(offering_price))}', inline = True)
        embed.add_field(name = 'Number of Shares:', value= f'{"{:,}".format(int(offering_shares))}', inline = True)
        embed.add_field(name = 'Offering Amount:', value= f'{"{:,}".format(int(offering_amount))}', inline = True)
        embed.add_field(name = 'Description:', value = f'{description}', inline = True)
        embed.add_field(name = 'Official Website:', value = f"[{company_website}]({company_website})", inline = True)
        embed.add_field(name = chr(173), value = chr(173), inline = True)

        self.__ticker = ticker
        self.embed = embed
        self.__readout_msg = readout_msg

    def __members(self):
        return (self.__readout_msg, self.__ticker)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, IPOMessage):
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