# Stock Scanner

 <br />

### Core Dependencies
|Dependency|Description|
|:---------|:----------|
| pandas | Dataframe data analysis and manipulation for stock's OHLCV (Open, High, Low, Close, Volume)|
| numpy | Working with pandas |
| mplfinance | Candlestick chart generation |
| google-search-results | Searching public offerings of specific stock from Google |
| discord\.py | Sending message to discord server |
| python-oracledb | Oracle database connection | 

 <br />

### Pre-Requisite
1. Install Oracle database
2. Create Discord account and channels
3. Create discord chatbot and invite it to your server
4. Create Interactive Brokers trading account, and subscribe market data 
5. Download IB API Gateway from Interactive Brokers official website
6. Execute `create.sql`
7. Edit `config.ini`, change Oracle login credentials and logger file directory

 <br />

### Local Debug Setup

1. Pull `previous_day_top_gainer_scraper` project
2. Access `previous_day_top_gainer_scraper` root directory
3. Run `py -m venv VENV_NAME` to create project virtual environment for `previous_day_top_gainer_scraper`
4. Access `previous_day_top_gainer_scraper` venv directory, then execute `activate`
5. Run `pip install -r requirements.txt` to install dependencies for `previous_day_top_gainer_scraper`
6. Access `stock_scanner` root directory
7. Run `py -m venv VENV_NAME` to create project virtual environment for `stock_scanner`
8. Access `stock_scanner` venv directory, then execute `activate`
9. Run `pip install -r requirements.txt` to install dependencies for `stock_scanner`
10. Execute `pip install -e ../previous_day_top_gainer_scraper`
11. Debug in your IDE

 <br />

### Build Executable File
1. Run `pip install pyinstaller`
2. Run `pyinstaller main.spec` to export this project as the executable file in `dist` folder 

<br />

### Export Dependencies list
1. Run `pip3 freeze > requirements.txt`

<br />

### What It Does
Stock scanner for day trade and swing trade. When specific stock's price action or pattern hit the scanner, the discord chatbot will send message to discord server to notify trader. The scanner scan stocks in U.S stock market for following patterns:

- Stocks that are popping up very quickly (for intra-day momentum trade)
- Yesterday bullish daily candle (for intra-day/ swing bullish pattern continuation trade)
- Previous days' top gainer support (for intra-day/ swing trade, buy at support level looking for bounce)
- Previous days' top gainer continuation (for intra-day/ swing trade continuation trade, looking for strong momentum after breaking new high)
<br />
