from enum import Enum

class TinyUrlApiEndpoint(str, Enum):
    HOSTNAME = 'https://api.tinyurl.com'
    SHORTENED_URL_DOMAIN = 'tinyurl.com'
    SHORTEN_URL = '/create'
    