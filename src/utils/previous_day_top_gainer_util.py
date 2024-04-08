import datetime
from constant.query.sqlite_query import SqliteQuery

def get_previous_day_top_gainer_list(connector, pct_change: float, start_date: datetime, end_date: datetime) -> bool:
    cursor = connector.cursor
    cursor.execute(SqliteQuery.GET_PREVIOUS_DAY_TOP_GAINER_QUERY.value, (pct_change, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    
    rows = cursor.fetchall()
    return rows

def get_offering_history_list(connector, ticker, scrape_date: datetime):
    cursor = connector.cursor
    cursor.execute(SqliteQuery.GET_OFFERING_HISTORY_QUERY.value, (ticker, scrape_date.strftime('%Y-%m-%d')))
    
    rows = cursor.fetchall()
    return rows

def get_latest_scrape_date(connector, ticker):
    cursor = connector.cursor
    cursor.execute(SqliteQuery.GET_LATEST_TICKER_SCRAPE_DATE_QUERY.value, (ticker,))
    
    result = cursor.fetchone()
    scrape_date = None
    
    if result:
        scrape_date = datetime.strptime('%d-%m-%Y')
    
    return scrape_date
    