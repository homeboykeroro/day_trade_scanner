import time
import pandas as pd
import yfinance as yf

from utils.logger import Logger

from requests import Session
# from requests_cache import CacheMixin, SQLiteCache
# from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
# from pyrate_limiter import Duration, RequestRate, Limiter
# class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
#     pass

logger = Logger()

# session = CachedLimiterSession(
#     limiter=Limiter(RequestRate(20, Duration.SECOND*5)),  # max 2 requests per 5 seconds
#     bucket_class=MemoryQueueBucket,
#     backend=SQLiteCache("yfinance.cache"),
# )
# session.headers['User-agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'

def get_financial_data(contract_list: list) -> dict:
    start_time = time.time()
    
    if not contract_list:
        return {}
    
    ticker_list = [contract.get('symbol') for contract in contract_list]
    request_ticker_str = ' '.join(ticker_list)
    result_dict = {}
    
    stocks = yf.Tickers(request_ticker_str)
    
    for ticker in ticker_list:        
        quarterly_cashflow_df = stocks.tickers[ticker].quarterly_cashflow.loc[['Free Cash Flow']] if not stocks.tickers[ticker].quarterly_cashflow.empty and 'Free Cash Flow' in stocks.tickers[ticker].quarterly_cashflow.index else pd.DataFrame()
        
        quarterly_total_debt_df = stocks.tickers[ticker].quarterly_balance_sheet.loc[['Total Debt']] if not stocks.tickers[ticker].quarterly_balance_sheet.empty and 'Total Debt' in stocks.tickers[ticker].quarterly_balance_sheet.index else pd.DataFrame()
        quarterly_total_assest_df = stocks.tickers[ticker].quarterly_balance_sheet.loc[['Total Assets']] if not stocks.tickers[ticker].quarterly_balance_sheet.empty and 'Total Assets' in stocks.tickers[ticker].quarterly_balance_sheet.index else pd.DataFrame()
        quarterly_balance_sheet_df = pd.concat([quarterly_total_debt_df, quarterly_total_assest_df], axis=0)
        
        quarterly_total_revenue_df = stocks.tickers[ticker].quarterly_income_stmt.loc[['Total Revenue']] if not stocks.tickers[ticker].quarterly_income_stmt.empty and 'Total Revenue' in stocks.tickers[ticker].quarterly_income_stmt.index else pd.DataFrame()
        quarterly_total_expense_df = stocks.tickers[ticker].quarterly_income_stmt.loc[['Total Expenses']] if not stocks.tickers[ticker].quarterly_income_stmt.empty and 'Total Expenses' in stocks.tickers[ticker].quarterly_income_stmt.index else pd.DataFrame()
        quarterly_income_stmt_df = pd.concat([quarterly_total_revenue_df, quarterly_total_expense_df], axis=0)
        
        annual_cashflow_df = stocks.tickers[ticker].cashflow.loc[['Free Cash Flow']] if not stocks.tickers[ticker].cashflow.empty and 'Free Cash Flow' in stocks.tickers[ticker].cashflow.index else pd.DataFrame()

        annual_total_revenue_df = stocks.tickers[ticker].balance_sheet.loc[['Total Debt']] if not stocks.tickers[ticker].balance_sheet.empty and 'Total Debt' in stocks.tickers[ticker].balance_sheet.index else pd.DataFrame()
        annual_total_expense_df = stocks.tickers[ticker].balance_sheet.loc[['Total Assets']] if not stocks.tickers[ticker].balance_sheet.empty and 'Total Assets' in stocks.tickers[ticker].balance_sheet.index else pd.DataFrame()
        annual_balance_sheet_df = pd.concat([annual_total_revenue_df, annual_total_expense_df], axis=0)
        
        annual_total_revenue_df = stocks.tickers[ticker].income_stmt.loc[['Total Revenue']] if not stocks.tickers[ticker].income_stmt.empty and 'Total Revenue' in stocks.tickers[ticker].income_stmt.index else pd.DataFrame()
        annual_total_expense_df = stocks.tickers[ticker].income_stmt.loc[['Total Expenses']] if not stocks.tickers[ticker].income_stmt.empty and 'Total Expenses' in stocks.tickers[ticker].income_stmt.index else pd.DataFrame()
        annual_income_stmt_df = pd.concat([annual_total_revenue_df, annual_total_expense_df], axis=0)
        
        stocks.tickers[ticker].get_income_stmt()
        try:
            major_holders_df = stocks.tickers[ticker].major_holders
        except Exception as e:
            major_holders_df = pd.DataFrame()
            logger.log_debug_msg(f'Major holders data not avaliable for {ticker}, {e}', with_std_out=True)

        try:
            institutional_holders_df = stocks.tickers[ticker].institutional_holders
        except Exception as e: 
            institutional_holders_df = pd.DataFrame()
            logger.log_debug_msg(f'Institutional holder data not avaliable for {ticker}, {e}', with_std_out=True)

        result_dict[ticker] = dict(quarterly_cash_flow_df=quarterly_cashflow_df,
                                   quarterly_balance_sheet_df=quarterly_balance_sheet_df,
                                   quarterly_income_stmt_df=quarterly_income_stmt_df,
                                   annual_cashflow_df=annual_cashflow_df,
                                   annual_balance_sheet_df=annual_balance_sheet_df,
                                   annual_income_stmt_df=annual_income_stmt_df,
                                   major_holders_df=major_holders_df,
                                   institutional_holders_df=institutional_holders_df)
    
    logger.log_debug_msg(f'Financial data retrieval time: {time.time() - start_time} seconds', with_std_out=True)
    return result_dict