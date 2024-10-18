import os
from enum import Enum

ACCOUNT_ID = os.environ['ACCOUNT_ID']

class ClientPortalApiEndpoint(str, Enum):
    #  Endpoint	                     Method	     Limit
    # /iserver/scanner/run	         POST	     1 req/sec
    # /iserver/marketdata/history	 GET         5 concurrent requests
    # /iserver/marketdata/snapshot	 GET	     10 req/s
    
    # Any endpoint not listed in the table below follows the global restriction of 10 requests per second.
    # The endpoint /iserver/accounts must be called prior to /iserver/marketdata/snapshot.
    # To get available scanner parameter, call /iserver/scanner/params'
    
    ACCOUNT_ID = ACCOUNT_ID
    HOSTNAME = 'https://localhost:5000/v1/api'
    SCANNER_PARAMETER = '/iserver/scanner/params'
    AUTH_STATUS = '/iserver/auth/status'
    RUN_SCANNER = '/iserver/scanner/run'
    MARKET_DATA_HISTORY = '/iserver/marketdata/history'
    ACCOUNT = '/iserver/accounts'
    SECURITY_DEFINITIONS = '/trsrv/secdef'
    SNAPSHOT = '/iserver/marketdata/snapshot'
    SECURITY_STOCKS_BY_SYMBOL = '/trsrv/stocks'
    SSO_VALIDATE = '/sso/validate'
    REAUTHENTICATE = '/iserver/reauthenticate'
    PORTFOLIO_ACCOUNTS = '/portfolio/accounts'
    PORTFOLIO_SUB_ACCOUNTS = '/portfolio/subaccounts'
    TRADES = '/iserver/account/trades'
    
    
    

    