import datetime

from constant.query.sqlite_query import SqliteQuery

def check_if_message_sent(connector, ticker: str, hit_scanner_datetime: datetime, scan_pattern: str, bar_size: str) -> bool:
    cursor = connector.cursor
    cursor.execute(SqliteQuery.CHECK_MESSAGE_EXIST_QUERY.value, (ticker, hit_scanner_datetime, scan_pattern, bar_size))
    
    row = cursor.fetchone()
    if row is None:
        return False
    else:
        return True

def add_sent_message_record(connector, message_list: list):
    cursor = connector.cursor
    cursor.executemany(SqliteQuery.ADD_MESSAGE_QUERY.value, message_list)
    cursor.connection.commit()
    
def delete_all_sent_message_record(connector) -> int:
    cursor = connector.cursor
    cursor.execute(SqliteQuery.DELETE_ALL_MESSAGE_QUERY.value)
    cursor.connection.commit()
    return cursor.rowcount
