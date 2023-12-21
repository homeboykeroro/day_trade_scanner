from enum import Enum

class SqliteQuery(str, Enum):
    CHECK_MESSAGE_EXIST_QUERY = "SELECT * FROM message WHERE EXISTS (SELECT 1 FROM message WHERE ticker = ? AND hit_scanner_datetime = ? AND scan_pattern = ? AND bar_size = ?)"
    ADD_MESSAGE_QUERY = "INSERT INTO message VALUES(?, ?, ?, ?)"
    DELETE_ALL_MESSAGE_QUERY = "DELETE FROM message"