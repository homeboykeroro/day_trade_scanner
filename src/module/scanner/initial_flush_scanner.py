    
    def __analyse_intra_day_top_loser(self, ib_connector: IBConnector,
                                            discord_client: DiscordChatBotClient) -> None:
        logger.log_debug_msg('Intra day top loser scan starts')
        
        contract_list = ib_connector.get_screener_results(MAX_NO_OF_DAY_TRADE_SCANNER_RESULT, IB_TOP_LOSER_FILTER)
        
        logger.log_debug_msg(f'Fetch top loser snapshot')
        ib_connector.update_contract_info(contract_list)
        ticker_to_contract_dict = ib_connector.get_ticker_to_contract_dict()
        snapshot_list = [dict(symbol=contract.symbol, company_name=contract.company_name) for _, contract in ticker_to_contract_dict.items()]
        logger.log_debug_msg(f'Top loser snapshot list: {snapshot_list}, size: {len(snapshot_list)}')
        logger.log_debug_msg(f'Initial dip scanner retrieval completed')
        
        logger.log_debug_msg('Retrieve top loser one minute candle')
        one_minute_candle_df = self.__retrieve_intra_day_minute_candle(ib_connector=ib_connector,
                                                                       contract_list=contract_list, 
                                                                       bar_size=BarSize.ONE_MINUTE)
        logger.log_debug_msg(f'Top loser one minute candle ticker: {one_minute_candle_df.columns.get_level_values(0).unique().tolist()}')
        
        logger.log_debug_msg('Retrieve top loser daily candle')
        daily_df = self.__get_daily_candle(ib_connector=ib_connector,
                                           contract_list=contract_list, 
                                           offset_day=INITIAL_FLUSH_DAILY_CANDLE_DAYS, 
                                           outside_rth=False)
        logger.log_debug_msg(f'Top loser daily candle ticker: {daily_df.columns.get_level_values(0).unique().tolist()}')

        with pd.option_context('display.max_rows', None,
                       'display.max_columns', None,
                       'display.precision', 3):
            logger.log_debug_msg('__analyse_intra_day_top_loser daily df:')
            logger.log_debug_msg(daily_df)

        logger.log_debug_msg(f'Top loser scanner result: {[contract["symbol"] for contract in contract_list]}')
        
        if SHOW_TOP_LOSER_SCANNER_DISCORD_DEBUG_LOG:
            send_msg_start_time = time.time()
            discord_client.send_message(DiscordMessage(content=f'{[contract["symbol"] for contract in contract_list]}'), DiscordChannel.TOP_LOSER_SCANNER_LIST)
            logger.log_debug_msg(f'Send top loser scanner result time: {time.time() - send_msg_start_time}')
        
        initial_flush_analyser = InitialFlush(bar_size=BarSize.ONE_MINUTE,
                                          historical_data_df=one_minute_candle_df, 
                                          daily_df=daily_df, 
                                          ticker_to_contract_info_dict=ib_connector.get_ticker_to_contract_dict(), 
                                          discord_client=discord_client)
        initial_flush_analyser.analyse()
        
        logger.log_debug_msg('Intra-day top loser scan completed')