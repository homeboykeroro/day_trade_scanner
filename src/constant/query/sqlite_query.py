from enum import Enum

class SqliteQuery(str, Enum):
    CHECK_MESSAGE_EXIST_QUERY = "SELECT * FROM message WHERE EXISTS (SELECT 1 FROM message WHERE ticker = ? AND hit_scanner_datetime = ? AND scan_pattern = ? AND bar_size = ?)"
    ADD_MESSAGE_QUERY = "INSERT INTO message VALUES(?, ?, ?, ?)"
    DELETE_ALL_MESSAGE_QUERY = "DELETE FROM message"
    
    ADD_SWING_TRADE_SCAN_EXECUTION_RECORD_QUERY = "INSERT INTO swing_trade_scan VALUES (?)"
    CHECK_SWING_TRADE_SCAN_IS_EXECUTED_QUERY = "SELECT * FROM swing_trade_scan WHERE scan_date = ?"