from enum import Enum

class ClientPortalApiEndpoint(str, Enum):
    #  Endpoint	                     Method	   Limit
    # /iserver/scanner/run	         POST	   1 req/sec
    # /iserver/marketdata/history	 GET       5 concurrent requests
    # /iserver/marketdata/snapshot	 GET	   10 req/s
    
    # The endpoint /iserver/accounts must be called prior to /iserver/marketdata/snapshot.
    # To get available scanner parameter, call /iserver/scanner/params'
    
    HOSTNAME = 'https://localhost:5000/v1/api'
    SCANNER_PARAMETER = '/iserver/scanner/params'
    AUTH_STATUS = '/iserver/auth/status'
    RUN_SCANNER = '/iserver/scanner/run'
    MARKET_DATA_HISTORY = '/iserver/marketdata/history'
    ACCOUNT = '/iserver/accounts'
    SECURITY_DEFINITIONS = '/trsrv/secdef'
    SNAPSHOT = '/iserver/marketdata/snapshot'
    
    
    
    

    