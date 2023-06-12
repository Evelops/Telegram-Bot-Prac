import telegram
import asyncio
import os
from dotenv import load_dotenv
import time

load_dotenv()

# 토큰 & id 설정
token = os.getenv("TOEKN")
chat_id = os.getenv("CHAT_ID")

# v20 이후 메시지 전송 함수 비동기로 정의 되었기에 별도의 asyncio 모듈을 추가해서 적용
async def send_message():
    bot = telegram.Bot(token=token)
    for i in range(0, 500):
        await bot.sendMessage(chat_id=chat_id, text=f"메시지 테스트 {i}")

start = time.time()
asyncio.run(send_message())
end = time.time() - start
print(end) # 총 걸린 시간 확인

