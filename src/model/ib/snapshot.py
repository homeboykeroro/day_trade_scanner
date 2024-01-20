class Snapshot:
    def __init__(self, market_data_availability: str, last: str, previous_close: str):
        self.__market_data_availability = market_data_availability
        self.__last = last
        self.__previous_close = previous_close
        
    def __members(self):
        return (self.__market_data_availability, self.__last, self.__previous_close)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Snapshot):
            return self.__members() == other.__members()

    def __hash__(self) -> int:
        return hash(self.__members())
    
    def __str__(self):
        company_name_display = f'Market Data Availability: {self.__market_data_availability}\n'
        last_price_display = f'Last: {self.__last}\n'
        previous_close_display = f'Previous close: {self.__previous_close}\n'
    
        return company_name_display + last_price_display + previous_close_display

    @property
    def market_data_availability(self):
        return self.__market_data_availability
    
    @market_data_availability.setter
    def market_data_availability(self, market_data_availability):
        self.__market_data_availability = market_data_availability
        
    @property
    def last(self):
        return self.__last
    
    @last.setter
    def last(self, last):
        self.__last = last
        
    @property
    def previous_close(self):
        return self.__previous_close
    
    @previous_close.setter
    def previous_close(self, previous_close):
        self.__previous_close = previous_close
        
  