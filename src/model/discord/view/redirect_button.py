import discord

class RedirectButton(discord.ui.View):
    def __init__(self, ticker: str, jump_url: str):
        super().__init__()
        
        self.add_item(discord.ui.Button(label=f'View {ticker}', url=jump_url, style=discord.ButtonStyle.primary))