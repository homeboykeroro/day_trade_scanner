from model.pl.trade_profit_and_loss import TradeProfitAndLoss

class EntryAndExit(TradeProfitAndLoss):
    def __init__(self):
        super().__init__()
    
    def __members(self):
        return (self.__candle_retrieval_start_datetime,
                self.__candle_retrieval_end_datetime,
                self.__buy_datetime_list,
                self.__sell_datetime_list,
                self.__chart_dir)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, TradeProfitAndLoss):
            return self.__members() == other.__members()

    def __hash__(self) -> int:
        return hash(self.__members())
    
    @property
    def candle_retrieval_start_datetime(self):
        return self.__candle_retrieval_start_datetime
    
    @candle_retrieval_start_datetime.setter
    def candle_retrieval_start_datetime(self, candle_retrieval_start_datetime):
        self.__candle_retrieval_start_datetime = candle_retrieval_start_datetime
    
    @property
    def candle_retrieval_end_datetime(self):
        return self.__candle_retrieval_end_datetime
    
    @candle_retrieval_end_datetime.setter
    def candle_retrieval_end_datetime(self, candle_retrieval_end_datetime):
        self.__candle_retrieval_end_datetime = candle_retrieval_end_datetime
    
    @property
    def buy_datetime_list(self):
        return self.__buy_datetime_list
    
    @buy_datetime_list.setter
    def buy_datetime_list(self, buy_datetime_list):
        self.__buy_datetime_list = buy_datetime_list
    
    @property
    def sell_datetime_list(self):
        return self.__sell_datetime_list
    
    @sell_datetime_list.setter
    def sell_datetime_list(self, sell_datetime_list):
        self.__sell_datetime_list = sell_datetime_list
    
    @property
    def chart_dir(self):
        return self.__chart_dir
    
    @chart_dir.setter
    def chart_dir(self, chart_dir):
        self.__chart_dir = chart_dir