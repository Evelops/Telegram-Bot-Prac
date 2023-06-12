import telegram
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

# 토큰 & id 설정
token = os.getenv("TOEKN")
chat_id = os.getenv("CHAT_ID")

bot = telegram.Bot(token=token)
asyncio.run(bot.sendMessage(chat_id=chat_id, text="메시지 테스트"))

