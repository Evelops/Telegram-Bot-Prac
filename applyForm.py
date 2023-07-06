import telegram
import pandas as pd
import numpy as np

import asyncio
import os
from dotenv import load_dotenv
import time
import datetime

from konlpy.tag import Mecab
from gensim.models.doc2vec import Doc2Vec

import pymysql
import boto3

from sqlalchemy import create_engine, text

# env 파일 업로드
load_dotenv()

access_key = os.getenv("AWS_KEY")
secret_key = os.getenv("AWS_SECRET_KEY")

# s3 객체 정의.
s3 = boto3.resource('s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key)

bucket = s3.Bucket('hangle-square')
model_file = 'recsys_model/review_d2v.model'
local_model_path = 'review_d2v.model'
bucket.download_file(model_file, local_model_path)

# 환경 변수에서 DB 연결 정보 로드
DB_HOST = os.getenv("N_DB_HOST")
DB_USER = os.getenv("N_DB_USER")
DB_PASSWORD = os.getenv("N_DB_PWD")
DB_NAME = os.getenv("N_DB_NAME")
DB_PORT = os.getenv("DB_PORT")

conn = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    db=DB_NAME,
    charset='utf8'
)
cur = conn.cursor()

engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}?charset=utf8mb4', echo=False)

now = datetime.datetime.now()
now_format = now.strftime('%Y-%m-%d')
print(now_format)
# 데이터 로드
news_data = pd.read_sql_query("select * from News ", engine)

# 데이터 전처리
news_data['document'] = news_data['title'] + ' ' + news_data['contents']
news_data['label'] = news_data['keyword'].map({'연예': 0, '스포츠': 1, '정치': 2, '국제': 3, '사회': 4, '문화': 5})
news_data = news_data.dropna(how='any')
print(len(news_data))

# mecab 객체 선언
mecab = Mecab()

# 한글스퀘어 뉴스 인기 검색어 목록
rankingQuery = f"SELECT keyword FROM realTimeKeyword ORDER BY cnt_num DESC LIMIT 5"
cur.execute(rankingQuery)
rows = cur.fetchall()

rankings = []
for i, row in enumerate(rows):
    string_value = row[0]  # 튜플의 첫 번째 요소인 문자열 값을 선택합니다.
    string_value = string_value.strip('()')  # 괄호를 제거합니다.
    rank = f"{i + 1}. {string_value}"  # 순위와 검색어를 결합하여 생성합니다.
    rankings.append(rank)

# 구독중인 유저의 user_id, telegeram_id 값을 불러옴
subscribedUserList = "SELECT user_id, telegram_id FROM subScribeTelegramNews"
cur.execute(subscribedUserList)
subscribedListRows = cur.fetchall()
print(f"길이 => {len(subscribedListRows)}")
print(f" {subscribedListRows[0]}")
print(f" {subscribedListRows[0][0]}")
print(f" {subscribedListRows[0][1]}")

subscribedList = []
# 리스트 형식으로 유저 리스트를 저장
for row in subscribedListRows:
    subscribedList.append(list(row))

print(subscribedList)


# 유저 아이디 별 추천 뉴스를 추출하는 로직
def getRecNews(user_id, telegram_id):
    print(f'USERID => {user_id}, TELEGRAMID => {telegram_id}')
    # 유저가 클릭한 정보 로그를 추출
    query = f"SELECT idx FROM Click_News_Info WHERE user_id = {user_id}"
    result = conn.execute(text(query))

    if (result.rowcount == 0):
        # Doc2Vec 모델 로드
        model = Doc2Vec.load('review_d2v.model')

        # 유저의 관심분야 활용해서 뿌려주기 (일단은 첫 입장인 경우)
        query = f"SELECT topics FROM Preferred_Topics WHERE user_id = {user_id}"
        result = conn.execute(text(query))

        row = result.fetchone()

        if row is not None:
            keyword_string = row[0]

            # 유저가 등록한 관심분야 배열로 변환
        keywords = keyword_string.split(',')

        # 배열로 받아온 키워드 형태소 분석해서 벡터 추출
        keyword_vectors = []
        for keyword in keywords:
            keyword_vectors.append(model.infer_vector(mecab.morphs(keyword)))

        keyword_vector = np.mean(keyword_vectors, axis=0)

        # 모든 문서와의 유사도 계산
        doc_vectors = [model.dv[str(i)] for i in range(len(model.dv))]
        # numpy 모듈의 np.dot() 메서드를 사용하여 학습된 모든 문서 벡터 + 입력된 키워드간 내적 계산
        similarities = np.dot(doc_vectors, keyword_vector)
        top_n_indices = np.argsort(similarities)[::-1][:5]
        recommended_documents = pd.DataFrame(news_data.iloc[top_n_indices])
        values = recommended_documents['idx'].tolist()
        print(values)
    else:
        # Doc2Vec 모델 로드
        model = Doc2Vec.load('review_d2v.model')

        # news_data2 데이터 로드 (유저 클릭한 뉴스들)
        news_data2 = pd.read_sql_query(f"select * from Click_News_Info where user_id = {user_id}", conn)

        # 데이터 전처리
        news_data2['document'] = news_data2['title'] + ' ' + news_data2['contents']
        news_data2 = news_data2.dropna(how='any')

        # news_data2 데이터를 벡터화
        news_data2_vectors = [model.infer_vector(mecab.morphs(doc)) for doc in news_data2['document']]

        # 배열로 받아온 news_data2 벡터와 모든 news_data 벡터간 내적 계산
        similarity_matrix = np.dot(news_data2_vectors, np.array([model.dv[str(i)] for i in range(len(model.dv))]).T)

        # 상위 n(5)개 문서 추출하여 반환
        top_n_indices = np.argsort(similarity_matrix, axis=1)[:, ::-1][:, :5]

        # top_n_indices 배열에서 news_data의 인덱스 범위를 벗어나는 인덱스를 필터링하여 제거
        top_n_indices = [np.intersect1d(x, news_data.index) for x in top_n_indices]

        values = [news_data.iloc[top_n_indices[i]]['idx'].tolist() for i in range(len(news_data2))]
        print(values)

    return "tq"


# 토큰 & id 설정
token = os.getenv("TOEKN")
chat_id = os.getenv("CHAT_ID")

# telegram token define
bot = telegram.Bot(token=token)

# Markdown-formatted 문자열 생성
msg = (
    "*[[2023-06-16] HS NEWS 알림봇 입니다]*\n"
    "-----------------------------------------\n"
    "*한글스퀘어 인기 검색어 순위* \n"
    f"{' '.join(rankings)}\n"
    "-----------------------------------------\n"
    "*유저님을 위한 추천 뉴스*\n\n"
    "1. '맨유? 뮌헨?' 김민재 몸값 또 상승! '695억→835억', '손흥민과 공동 1위' ...\n"
    "\n"
    "2. [속보] 바이에른 뮌헨, 김민재 이적료 '1000억' 쏜다…바이아웃 '최상위권' 액수 지...\n"
    "-----------------------------------------\n"
    "👉 서비스 바로가기\n"
    "[바로가기](https://www.hangeulsquare.com/news/)\n"
)


# v20 이후 메시지 전송 함수 비동기로 정의 되었기에 별도의 asyncio 모듈을 추가해서 적용
async def send_message():
    # 리스트에서 user_id, telegram_id 값을 row에서 추출
    for row in subscribedList:
        user_id, telegram_id = row
        print(f"User ID: {user_id}, Telegram ID: {telegram_id}")
        # 유저별 맞춤 추천 데이터를 추출 후 텔레그램으로 전송
        # getRecNews(user_id,telegram_id)
        print(f'USERID => {user_id}, TELEGRAMID => {telegram_id}')
        # 유저가 클릭한 정보 로그를 추출
        query = f"SELECT idx FROM Click_News_Info WHERE user_id = {user_id}"
        result = cur.execute(query)
        print(f'result => {result}')
        print(f'result => {cur.rowcount}')

        if (cur.rowcount == 0):
            print('유저가 클릭한 로그가 없음')
            # Doc2Vec 모델 로드
            model = Doc2Vec.load('review_d2v.model')
            # 유저의 관심분야 활용해서 뿌려주기 (일단은 첫 입장인 경우)
            query = f"SELECT topics FROM Preferred_Topics WHERE user_id = {user_id}"
            # result = cur.execute(query)
            # row = result.fetchone()
            cur.execute(query)
            row = cur.fetchone()
            if row is not None:
                keyword_string = row[0]
                # 유저가 등록한 관심분야 배열로 변환
            keywords = keyword_string.split(',')
            # 배열로 받아온 키워드 형태소 분석해서 벡터 추출
            keyword_vectors = []
            for keyword in keywords:
                keyword_vectors.append(model.infer_vector(mecab.morphs(keyword)))
            keyword_vector = np.mean(keyword_vectors, axis=0)
            # 모든 문서와의 유사도 계산
            doc_vectors = [model.dv[str(i)] for i in range(len(model.dv))]
            # numpy 모듈의 np.dot() 메서드를 사용하여 학습된 모든 문서 벡터 + 입력된 키워드간 내적 계산
            similarities = np.dot(doc_vectors, keyword_vector)
            top_n_indices = np.argsort(similarities)[::-1][:5]
            recommended_documents = pd.DataFrame(news_data.iloc[top_n_indices])
            values = recommended_documents['title'].tolist()

            msg = (
                f"*[[{now_format}] HS NEWS 알림봇 입니다]*\n"
                "-----------------------------------------\n"
                "*🏆 한글스퀘어 인기 검색어 순위* \n"
                f"{' '.join(rankings)}\n"
                "-----------------------------------------\n"
                "*📌 유저님을 위한 추천 뉴스*\n\n")
            for i, value in enumerate(values[:5], 1):
                msg += f"{i}. {value}\n\n"

            msg += (
                "-----------------------------------------\n"
                "👉 서비스 바로가기\n"
                "[바로가기](https://www.hangeulsquare.com/news/)\n")
            print(msg)
            await bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='Markdown')
        else:
            print('유저가 클릭한 로그가 있음')
            # Doc2Vec 모델 로드
            model = Doc2Vec.load('review_d2v.model')

            # news_data2 데이터 로드 (유저 클릭한 뉴스들)
            news_data2 = pd.read_sql_query(f"select * from Click_News_Info where user_id = {user_id}", conn)

            # 데이터 전처리
            news_data2['document'] = news_data2['title'] + ' ' + news_data2['contents']
            news_data2 = news_data2.dropna(how='any')

            # news_data2 데이터를 벡터화
            news_data2_vectors = [model.infer_vector(mecab.morphs(doc)) for doc in news_data2['document']]

            # 배열로 받아온 news_data2 벡터와 모든 news_data 벡터간 내적 계산
            similarity_matrix = np.dot(news_data2_vectors, np.array([model.dv[str(i)] for i in range(len(model.dv))]).T)

            # 상위 n(5)개 문서 추출하여 반환
            top_n_indices = np.argsort(similarity_matrix, axis=1)[:, ::-1][:, :5]

            # top_n_indices 배열에서 news_data의 인덱스 범위를 벗어나는 인덱스를 필터링하여 제거
            top_n_indices = [np.intersect1d(x, news_data.index) for x in top_n_indices]

            values = [news_data.iloc[top_n_indices[i]]['title'].tolist() for i in range(len(news_data2))]
            # Markdown-formatted 문자열 생성
            msg = (
                f"*[[{now_format}] HS NEWS 알림봇 입니다]*\n"
                "-----------------------------------------\n"
                "*🏆 한글스퀘어 인기 검색어 순위* \n"
                f"{' '.join(rankings)}\n"
                "-----------------------------------------\n"
                "*📌 유저님을 위한 추천 뉴스*\n\n"
                f"1. {values[0][0]} \n"
                "\n"
                f"2. {values[0][1]} \n"
                "\n"
                f"3. {values[0][2]} \n"
                "\n"
                f"4. {values[0][3]} \n"
                "\n"
                f"5. {values[0][4]} \n"
                "-----------------------------------------\n"
                "👉 서비스 바로가기\n"
                "[바로가기](https://www.hangeulsquare.com/news/)\n")
            await bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='Markdown')


start = time.time()
asyncio.run(send_message())
end = time.time() - start
# connection 종료
conn.close()

print(end)  # 총 걸린 시간 확인
