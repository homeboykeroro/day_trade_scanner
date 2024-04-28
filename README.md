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
3. Download IB API Gateway from Interactive Brokers official website
4. Execute `create.sql`
5. Edit `config.ini`, change Oracle login credentials and logger file directory

 <br />

### Local Debug Setup

1. Run `py -m venv VENV_NAME` to create project virtual environment
2. Go to venv directory, then execute `activate`
3. Run `pip install -r requirements.txt` to install dependencies
4. Debug in your IDE

 <br />

### Build Executable File
1. Run `pip install pyinstaller`
2. Run `pyinstaller main.py --icon=<icon_path>` to export this project as the executable file in `dist` folder 

<br />

### Export Dependencies list
1. Run `pip3 freeze > requirements.txt`

<br />

### What It Does
Stock scanner for day trade and swing trade

<br />

### Discord Setup
- [Mute Specific Channels Notifications](https://support.discord.com/hc/en-us/articles/209791877-How-do-I-mute-and-disable-notifications-for-specific-channels)
- [Prevent People from Joining Server](https://www.youtube.com/watch?v=j9OFFZw2beY&ab_channel=NoIntroTutorials)

<br />

### Computer Setup
- [Disable laptop boot light/ sound](https://www.asus.com/support/faq/1050213/)
- [Keep applications running with computer's lid closed](https://www.pcmag.com/how-to/how-to-run-your-laptop-with-the-lid-closed)