from enum import Enum

class SqliteQuery(str, Enum):
    CHECK_MESSAGE_EXIST_QUERY = "SELECT * FROM discord_message_record WHERE EXISTS (SELECT 1 FROM discord_message_record WHERE ticker = ? AND hit_scanner_datetime = ? AND scan_pattern = ? AND bar_size = ?)"
    ADD_MESSAGE_QUERY = "INSERT INTO discord_message_record VALUES(?, ?, ?, ?)"
    DELETE_ALL_MESSAGE_QUERY = "DELETE FROM discord_message_record"
    