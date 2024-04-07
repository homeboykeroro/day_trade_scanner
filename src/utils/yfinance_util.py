import time
import pandas as pd
import yfinance as yf

from utils.logger import Logger

from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter
class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

logger = Logger()

# session = CachedLimiterSession(
#     limiter=Limiter(RequestRate(20, Duration.SECOND*5)),  # max 2 requests per 5 seconds
#     bucket_class=MemoryQueueBucket,
#     backend=SQLiteCache("yfinance.cache"),
# )
# session.headers['User-agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0'

def get_financial_data(contract_list: list) -> dict:
    ticker_list = [contract.get('symbol') for contract in contract_list]
    request_ticker_str = ' '.join(ticker_list)
    result_dict = {}
    
    stocks = yf.Tickers(request_ticker_str)
    
    for ticker in ticker_list:        
        quarterly_cashflow_df = stocks.tickers[ticker].quarterly_cashflow.loc[['Free Cash Flow']] if not stocks.tickers[ticker].quarterly_cashflow.empty else pd.DataFrame()
        quarterly_balance_sheet_df = stocks.tickers[ticker].quarterly_balance_sheet.loc[['Total Debt', 'Total Assets']] if not stocks.tickers[ticker].quarterly_balance_sheet.empty else pd.DataFrame()
        quarterly_income_stmt_df = stocks.tickers[ticker].quarterly_income_stmt.loc[['Total Revenue', 'Total Expenses']] if not stocks.tickers[ticker].quarterly_income_stmt.empty else pd.DataFrame()
        
        annual_cashflow_df = stocks.tickers[ticker].cashflow.loc[['Free Cash Flow']] if not stocks.tickers[ticker].cashflow.empty else pd.DataFrame()
        annual_balance_sheet_df = stocks.tickers[ticker].balance_sheet.loc[['Total Debt', 'Total Assets']] if not stocks.tickers[ticker].balance_sheet.empty else pd.DataFrame()
        annual_income_stmt_df = stocks.tickers[ticker].income_stmt.loc[['Total Revenue', 'Total Expenses']] if not stocks.tickers[ticker].income_stmt.empty else pd.DataFrame()
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
    
    return result_dict