# api_producer.py
import requests
from kafka import KafkaProducer
import json
import time

# Kafka Producer 생성
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
# Test용 API 호출
def fetch_data():
    url = "https://jsonplaceholder.typicode.com/posts/1"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"[ERROR] API 요청 실패: {e}")
    return None
# 반복 수집 및 전송
while True:
    data = fetch_data()
    if data:
        producer.send('test-topic', data)
        print("메시지 전송 완료:", data)
    time.sleep(5)