### Docker 실행
```bash
docker compose up -d
```

### Airflow Web Server 접속
http://localhost:8088

### DAG 실행
- 'Pause/Unpause DAG' Toggle 선택

### 데이터 확인
- Kafka Topic 조회
```bash
docker exec -it kafka \
  kafka-topics --list --bootstrap-server kafka:29092
```

- Kafka Consumer 실행
```bash
docker exec -it kafka \
  kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic BIS-BUS-ARRIVAL-INFO --from-beginning
```

- Kafka 오프셋 조회 (시험용. 필요없으면 패스)
```bash
docker exec -it kafka \
  kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:29092 \
  --topic BIS-BUS-ARRIVAL-INFO \
  --time -1
```

- MinIO 조회
http://localhost:9001 접속

- PostgreSQL 조회
```bash
docker exec -it postgres bash
```
```bash
psql -U admin_dw -t datawarehouse
```
```bash
SELECT * FROM bus_arrival_info;
```