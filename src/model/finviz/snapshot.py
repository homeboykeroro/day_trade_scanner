class Snapshot:
    def __init__(self, ticker: str, change_pct: float) -> None:
        self.__ticker = ticker
        self.__change_pct = change_pct
        
    def __members(self):
        return (self.__ticker, self.__change_pct)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Snapshot):
            return self.__members() == other.__members()    
    
    def __str__(self) -> str:
        ticker_display = f'Ticker: {self.__ticker}\n'
        change_pct_display = f'Change Pct: {self.__change_pct}%'
        
        return ticker_display + change_pct_display
        
    @property
    def ticker(self):
        return self.__ticker
        
    @ticker.setter
    def ticker(self, ticker):
        self.__ticker = ticker
        
    @property
    def change_pct(self):
        return self.__change_pct
    
    @change_pct.setter
    def change_pct(self, change_pct):
        self.__change_pct = change_pct