from abc import ABC
import discord

class DiscordMessage(ABC):
    def __init__(self, ticker: str = None, embed: discord.Embed = None, content: str = None, view: discord.ui.View = None, files: list = None, jump_url: str = None):
        self.__ticker = ticker
        self.__embed = embed
        self.__content = content
        self.__view = view
        self.__files = files
        self.__jump_url = jump_url
        
    def __members(self):
        return (self.__embed, self.__view, self.__jump_url)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DiscordMessage):
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
    def embed(self):
        return self.__embed
    
    @embed.setter
    def embed(self, embed):
        self.__embed = embed
        
    @property
    def content(self):
        return self.__content
    
    @content.setter
    def content(self, content):
        self.__content = content
        
    @property
    def view(self):
        return self.__view
    
    @view.setter
    def view(self, view):
        self.__view = view
    
    @property
    def files(self):
        return self.__files
    
    @files.setter
    def files(self, files):
        self.__files = files
            
    @property
    def jump_url(self):
        return self.__jump_url
    
    @jump_url.setter
    def jump_url(self, jump_url):
        self.__jump_url = jump_url
    