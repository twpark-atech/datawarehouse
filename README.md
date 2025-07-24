### Kafka Topic 생성
```bash
docker exec -it datawarehouse-kafka-1 \
  kafka-topics --create \
  --topic test-topic \
  --bootstrap-server kafka:29092 \
  --replication-factor 1 \
  --partitions 1
```

### Kafka Topic 조회
```bash
docer exec -it datawarehouse-kafka-1 \
  kafka-topics --list --bootstrap-server kafka:29092
```

### Kafka Consumer 실행
```bash
docker exec -it datawarehouse-kafka-1 \
  kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic test-topic --from-beginning
```

### Kafka 오프셋 조회
```bash
docker exec -it datawarehouse-kafka-1 \
  kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:29092 \
  --topic test-topic \
  --time -1
```

### Spark로 Kafka 데이터 조회
```bash
docker exec -it spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4 \
  /app/kafka_streaming.py
```

### Kafka Producer 실행
```bash
python ./producer/api_producer.py
```