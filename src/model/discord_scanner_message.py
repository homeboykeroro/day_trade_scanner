import datetime

class DiscordScannerMessage:
    def __init__(self, embed, display_message: str, read_out_message: str, ticker: str, hit_scanner_datetime: datetime):
        self.__embed = embed
        self.__display_message = display_message
        self.__read_out_message = read_out_message
        self.__ticker = ticker
        self.__hit_scanner_datetime = hit_scanner_datetime
        
    def __members(self):
        return (self.__display_message, self.__read_out_message)

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
    