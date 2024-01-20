class ContractInfo:
    def __init__(self, con_id, symbol, exchange, company_name, sector, market_cap, shortable, shortable_shares, rebate_rate, snapshot):
        self.__con_id = con_id
        self.__symbol = symbol
        self.__exchange = exchange
        self.__company_name = company_name
        self.__sector = sector
        self.__market_cap = market_cap
        self.__numeric_market_cap = self.convert_human_readable_figure_to_num(self.__market_cap)
        self.__shortable = shortable
        self.__shortable_shares = shortable_shares
        self.__rebate_rate = rebate_rate
        self.__snapshot = snapshot
    
    def __members(self):
        return (self.__con_id, self.__symbol, self.__exchange, self.__company_name, self.__sector, self.__market_cap, self.__shortable, self.__shortable_shares, self.__rebate_rate, self.__snapshot)

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
        rebate_rate_display = f'Rebate Rate: {self.__rebate_rate}\n'
        snapshot_display = f'{str(self.__snapshot)}'
        
        return company_name_display + company_sector_display + market_cap_display + shortable_display + shortable_shares_display + rebate_rate_display + snapshot_display

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
    def numeric_market_cap(self):
        return self.__numeric_market_cap
    
    @numeric_market_cap.setter
    def numeric_market_cap(self, numeric_market_cap):
        self.__numeric_market_cap = numeric_market_cap
    
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
        
    @property
    def snapshot(self):
        return self.__snapshot
    
    @snapshot.setter
    def snapshot(self, snapshot):
        self.__snapshot = snapshot
        
    def add_contract_info_to_embed_msg(self, embed):
        embed.add_field(name = 'Company:', value = f'{self.__company_name}', inline = True)
        embed.add_field(name = 'Sector:', value = f'{self.__sector}', inline = True)
        embed.add_field(name = 'Market Cap:', value = f'{self.__market_cap}', inline = True)
        embed.add_field(name = 'Shortable:', value = f"{'Yes' if self.__shortable == 'shortable' else 'No'}", inline = True)
        embed.add_field(name = 'Shortable Shares:', value = f"{self.__shortable_shares if (self.__shortable_shares) else 'N/A'}", inline = True)
        embed.add_field(name = 'Rebate Rate:', value = f'{self.__rebate_rate}', inline = True)

    def convert_human_readable_figure_to_num(self, numeric_str: str):
        if not numeric_str:
            return None
        
        multiplier = 1
        
        if numeric_str.endswith('K'):
            multiplier = 1e4
        if numeric_str.endswith('M'):
            multiplier = 1e6
        elif numeric_str.endswith('B'):
            multiplier = 1e9

        numeric_str = numeric_str[:-1]
        num = float(numeric_str.replace(',', ''))
        
        return num * multiplier