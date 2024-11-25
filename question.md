### Description
I am creating two python applications (main1.py and main2.py). What I am trying to do is to create a discord bot in each of my python application (one bot per application), and each of my applications would call their own bot to send message to my discord channel (these two applications would send message to the same channel), and also periodically execute their complex function. 


In order to keep executing functions periodically while running my discord bots in my applications, my idea is to start my bots in new threads in each of my application, and use while loop to keep running my complex function in main thread, so that applications can perform the two works concurrently.


main1.py
|------ main thread  (call main1's bot to send message, and periodically execute my complex function in while loop)
|------ main1_chatbot_thread (run discord bot with CHATBOT_TOKEN_1)

main2.py
|------ main thread  (call main2's bot to send message, and periodically execute my complex function in while loop)
|------ main2_chatbot_thread (run discord bot with CHATBOT_TOKEN_2)


### Problem
The problem is that when I run main1.py and main2.py together, my bots keep sending the same message of 'Starts application' multiple times and get stuck (not running code following the `send_message_by_list_with_response` function, so `complex_function` would not get executed), but if I just run main1.py or main2.py individaully my bot works normally. I just have no idea how should I troubleshoot this problem since I don't find any similar issue and example online.


### Code Snippet
Here is my simpified code to demonstrate my idea, main1.py and main2.py code are very similar except for token, thread name and the function that would be called periodically (`complex_function`) 

`main1.py`/ `main2.py`
```
CHATBOT_TOKEN_1 = os.environ['CHATBOT_TOKEN_1']  # for main2.py the value of token would be os.environ['CHATBOT_TOKEN_2']
CHATBOT_THREAD_NAME = 'main1_chatbot_thread' # for main2.py the value of token would be 'main2_chatbot_thread'
main_1_chatbot = DiscordChatBot(CHATBOT_TOKEN_1 )

def create_bot():
    global main_1_chatbot 
    main_1_chatbot.run_chatbot()
    
bot_thread = threading.Thread(target=create_bot, name=CHATBOT_THREAD_NAME)
bot_thread.start()

# main_1_chatbot object variable can be accessed in main thread and main1_chatbot_thread, check if chatbot completely start by is_chatbot_ready instance variable
while not main_1_chatbot.is_chatbot_ready:
    continue

# Start executing test() function when discord bot completely start
print(f'Chatbot starts')`

def complex_function():
    # my logic

def test():
    # send message to my channel to notify my application started
    main_1_chatbot.send_message_by_list_with_response([DiscordMessage(content='Starts application')], channel_type='Channel_1', with_text_to_speech=True)
    while True: 
        # execute my complex function, after the message is sent and get response
        complex_function()
        time.sleep(1800) # execute the test function every 1800 seconds

if __name__ == '__main__':
    test()
```

`discord_chatbot.py`
```
# Suppressing warning/ error 
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

CHANNEL_ID_1 = int(os.environ['DISCORD_CHANNEL_ID_1'])

intents = discord.Intents.default()
intents.reactions = True
intents.members = True
intents.guilds = True
intents.message_content = True

class DiscordChatBot(discord.Client):
    def __init__(self, token: str, *args, **kwargs):
        self.__is_chatbot_ready = False
        self.__token = token
        
        loop = asyncio.new_event_loop()
        super().__init__(*args, **kwargs, intents=intents, loop=loop)

    @property
    def is_chatbot_ready(self):
        return self.__is_chatbot_ready
    
    @is_chatbot_ready.setter
    def is_chatbot_ready(self, is_chatbot_ready):
        self.__is_chatbot_ready = is_chatbot_ready
           
    async def on_ready(self):
        print(f'Chatbot is ready')
        self.__is_chatbot_ready = True
    
    async def on_message(self, message):
        if message.author == self.user:
            return
        
        await message.channel.send(message.content)

    def get_discord_channel(self, channel_type: str):
        channel_id = None
        
        if channel_type == 'Channel_1':
            channel_id = CHANNEL_ID_1

        if not channel_id:
            raise Exception(f"Channel ID not found for: {channel_type}")
        
        channel = self.get_channel(channel_id)
        return channel

    async def send_message_to_channel(self,
                                      message: DiscordMessage, 
                                      channel_type: str, 
                                      with_text_to_speech: bool = False): 
        channel = self.get_discord_channel(channel_type)
        
        try:
            msg_param = dict(content=message.content, 
                             embed=message.embed, 
                             view=message.view, 
                             files=message.files, 
                             tts=with_text_to_speech)

            result = await channel.send(**msg_param)
            return result
        except Exception as e:
            print(f'Failed to send message to channel {channel_type.value}, {e}')
            return dict(exception=e)
    
    async def add_message_list_to_task(self, message_list: list, channel_type: str, with_text_to_speech: bool = False):
        tasks = [self.send_message_to_channel(message=message, channel_type=channel_type, with_text_to_speech=with_text_to_speech) for message in message_list] 
        result_message_list = await asyncio.gather(*tasks, return_exceptions=True, loop=self.loop)
        
        return result_message_list
    
    def send_message(self, message: DiscordMessage, channel_type: str, with_text_to_speech: bool = False):
        # response does not matter when using this method
        try:
            channel = self.get_discord_channel(channel_type)
            loop = self.loop
            msg_param = dict(content=message.content, 
                             embed=message.embed, 
                             view=message.view, 
                             files=message.files, 
                             tts=with_text_to_speech)

            loop.create_task(channel.send(**msg_param))
        except Exception as e:
            print(f'Add message to discord event loop error, {e}')
    
    def send_message_by_list_with_response(self, message_list: list, channel_type: str, with_text_to_speech: bool = False):
        try:
            loop = self.loop
            response = asyncio.run_coroutine_threadsafe(self.add_message_list_to_task(message_list=message_list, channel_type=channel_type, with_text_to_speech=with_text_to_speech), loop)
            try: 
                result_list = response.result() 
                return result_list
            except Exception as ex: 
                print("Get future response error, {ex}")
        except Exception as e:
            print(f'Send message by list with response failed, {e}')
    
    def run_chatbot(self):
        self.run(self.__token)

```

`discord_message.py`
It is just a wrapper class with getter and setter nothing is really special.
```
from abc import ABC
import discord

class DiscordMessage(ABC):
    def __init__(self, embed: discord.Embed = None, content: str = None, view: discord.ui.View = None, files: list = None, jump_url: str = None):
        self.__embed = embed
        self.__content = content
        self.__view = view
        self.__files = files
        self.__jump_url = jump_url
        
    def __members(self):
        return (self.__embed, self.__view, self.__jump_url)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DiscordMessage):
            return self.__members() == other.__members()

    def __hash__(self) -> int:
        return hash(self.__members())

     # getter and setter
```