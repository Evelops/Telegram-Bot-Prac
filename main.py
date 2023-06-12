import telegram
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

token = os.getenv("TOEKN")
chat_id = os.getenv("CHAT_ID")

bot = telegram.Bot(token=token)
asyncio.run(bot.sendMessage(chat_id=chat_id, text="메시지 테스트"))

