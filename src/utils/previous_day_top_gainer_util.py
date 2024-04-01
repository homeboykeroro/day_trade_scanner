import datetime
from constant.query.sqlite_query import SqliteQuery

def get_previous_day_top_gainer_list(connector, pct_change: float, start_date: datetime, end_date: datetime) -> bool:
    cursor = connector.cursor
    cursor.execute(SqliteQuery.GET_PREVIOUS_DAY_TOP_GAINER_QUERY.value, (pct_change, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    
    rows = cursor.fetchall()
    return rows