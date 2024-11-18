import discord
import traceback

from utils.logger import Logger

logger = Logger()

class RedirectButton(discord.ui.View):
    def __init__(self, ticker: str, jump_url: str):
        try:
            super().__init__()
        except Exception as e:
            logger.log_error_msg(f'Failed to initialise redirect button {e}', with_std_out = True)
            logger.log_error_msg(f'{traceback.format_exc()}', with_log_file=False, with_std_out=True)
        
        self.add_item(discord.ui.Button(label=f'View {ticker}', url=jump_url, style=discord.ButtonStyle.primary))