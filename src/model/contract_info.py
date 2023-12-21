class ContractInfo:
    def __init__(self, con_id, symbol, exchange, company_name, sector, market_cap, shortable, shortable_shares, rebate_rate):
        self.__con_id = con_id
        self.__symbol = symbol
        self.__exchange = exchange
        self.__company_name = company_name
        self.__sector = sector
        self.__market_cap = market_cap
        self.__shortable = shortable
        self.__shortable_shares = shortable_shares
        self.__rebate_rate = rebate_rate
    
    def __members(self):
        return (self.__con_id, self.__symbol, self.__exchange, self.__company_name, self.__sector, self.__market_cap, self.__shortable, self.__shortable_shares, self.__rebate_rate)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ContractInfo):
            return self.__members() == other.__members()

    def __hash__(self) -> int:
        return hash(self.__members())
    
    def __str__(self):
        company_name_display = f'Company: {self.__company_name}\n'
        company_sector_display = f'Sector: {self.__sector}\n'
        market_cap_display = f'Market Cap: {self.__market_cap}\n'
        shortable_display = f"Shortable: {'Yes' if self.__shortable == 'shortable' else 'No'}\n"
        shortable_shares_display = f'Shortable Shares: {self.__shortable_shares if (self.__shortable_shares) else "N/A"}\n'
        rebate_rate_display = f'Rebate Rate: {self.__rebate_rate}'
        
        return company_name_display + company_sector_display + market_cap_display + shortable_display + shortable_shares_display + rebate_rate_display

    @property
    def con_id(self):
        return self.__con_id
    
    @con_id.setter
    def con_id(self, con_id):
        self.__con_id = con_id
        
    @property
    def symbol(self):
        return self.__symbol
    
    @symbol.setter
    def symbol(self, symbol):
        self.__symbol = symbol
        
    @property
    def exchange(self):
        return self.__exchange
    
    @exchange.setter
    def exchange(self, exchange):
        self.__exchange = exchange
        
    @property
    def company_name(self):
        return self.__company_name
    
    @company_name.setter
    def company_name(self, company_name):
        self.__company_name = company_name
        
    @property
    def sector(self):
        return self.__sector
    
    @sector.setter
    def sector(self, sector):
        self.__sector = sector
        
    @property
    def market_cap(self):
        return self.__market_cap
    
    @market_cap.setter
    def market_cap(self, market_cap):
        self.__market_cap = market_cap
        
    @property
    def shortable(self):
        return self.__shortable
    
    @shortable.setter
    def shortable(self, shortable):
        self.__shortable = shortable
        
    @property
    def shortable_shares(self):
        return self.__shortable_shares
    
    @shortable_shares.setter
    def shortable_shares(self, shortable_shares):
        self.__shortable_shares = shortable_shares
        
    @property
    def rebate_rate(self):
        return self.__rebate_rate
    
    @rebate_rate.setter
    def rebate_rate(self, rebate_rate):
        self.__rebate_rate = rebate_rate

    