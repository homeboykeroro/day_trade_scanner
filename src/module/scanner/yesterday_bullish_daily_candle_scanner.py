    def __analyse_yesterday_top_gainer(self, ib_connector: IBConnector, 
                                             discord_client: DiscordChatBotClient):
        logger.log_debug_msg('yesterday top gainer scan starts')

        us_current_datetime = get_current_us_datetime()
        day_offset = 0 if us_current_datetime.time() > datetime.time(16, 0, 0) else -1

        yesterday_top_gainer_retrieval_datetime = get_us_business_day(offset_day=day_offset, 
                                                                      us_date=us_current_datetime)
        yesterday_top_gainer_contract_list = self.__get_previous_day_top_gainers_contracts(ib_connector=ib_connector,
                                                                                           min_pct_change=MIN_YESTERDAY_CLOSE_CHANGE_PCT,
                                                                                           offset=day_offset) 

        if not yesterday_top_gainer_contract_list:
            return

        request_candle_contract_list = [dict(symbol=contract.symbol, con_id=contract.con_id) for contract in yesterday_top_gainer_contract_list]
        previous_day_top_gainers_df = self.__get_daily_candle(ib_connector=ib_connector,
                                                              contract_list=request_candle_contract_list, 
                                                              offset_day=YESTERDAY_BULLISH_DAILY_CANDLE_DAYS,
                                                              outside_rth=False,
                                                              candle_retrieval_end_datetime=yesterday_top_gainer_retrieval_datetime)
        
        with pd.option_context('display.max_rows', None,
                       'display.max_columns', None,
                       'display.precision', 3):
            logger.log_debug_msg('__analyse_yesterday_top_gainer daily df')
            logger.log_debug_msg(previous_day_top_gainers_df)

        yesterday_bullish_daily_candle_analyser = YesterdayBullishDailyCandle(hit_scanner_date=yesterday_top_gainer_retrieval_datetime.date(),
                                                                              yesterday_top_gainer_contract_list=yesterday_top_gainer_contract_list,
                                                                              daily_df=previous_day_top_gainers_df,
                                                                              ticker_to_contract_info_dict=ib_connector.get_ticker_to_contract_dict(), 
                                                                              discord_client=discord_client)
        yesterday_bullish_daily_candle_analyser.analyse()
        
        logger.log_debug_msg('Yesterday top gainer scan completed')
        
  
  
  
  
  
  
    
    
    
    
    
    
    
    def __get_previous_day_top_gainers_contracts(self, ib_connector: IBConnector,
                                                       min_pct_change,
                                                       offset: int = None, 
                                                       retrieval_end_datetime: datetime = None):
        retrieval_start_datetime = get_us_business_day(offset)
        
        if not retrieval_end_datetime:
            retrieval_end_datetime = retrieval_start_datetime
        
        previous_day_top_gainer_list = get_previous_day_top_gainer_list(pct_change=min_pct_change, 
                                                                        start_datetime=retrieval_start_datetime, 
                                                                        end_datetime=retrieval_end_datetime)
        
        if not previous_day_top_gainer_list:
            return []
        
        ticker_list = list(set([top_gainer[0] for top_gainer in previous_day_top_gainer_list]))

        previous_day_top_gainer_contract_list = ib_connector.get_security_by_tickers(ticker_list)
        ib_connector.update_contract_info(previous_day_top_gainer_contract_list)
        ticker_to_contract_dict = ib_connector.get_ticker_to_contract_dict()
        
        contract_dict_list = []
        
        for ticker, contract in ticker_to_contract_dict.items():
            if ticker in ticker_list:
                contract_dict_list.append(contract)
        
        return contract_dict_list
    
    
    
    
    
    