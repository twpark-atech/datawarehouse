# api_producer.py
import requests
from kafka import KafkaProducer
import xmltodict
import json
import time

# Kafka Producer 생성
producer = KafkaProducer(
    bootstrap_servers='kafka:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
# Test용 API 호출
def fetch_data():
    url = "http://openapi.its.ulsan.kr/UlsanAPI/getBusArrivalInfo.xo"
    params = {
        'serviceKey' : 'fK7DUM0wLQwyd1AOzI94HNQ7J2GMrVyggFOUIA2COZRa65MJzY0qqRNXlhgd97QVTNogCda/SUhnm6297K9Uaw==',
        'pageNo' : '1', 
        'numOfRows' : '10', 
        'stopid' : '196040122'
    }
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            parsed = xmltodict.parse(response.text)
            return parsed
    except Exception as e:
        print(f"[ERROR] API 요청 실패: {e}")
    return None
# 반복 수집 및 전송
data = fetch_data()
if data:
    producer.send('BIS-BUS-ARRIVAL-INFO', data)
    print("메시지 전송 완료:", data)