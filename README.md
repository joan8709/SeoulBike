1) cp .env.example .env
2) .env 에 SEOUL_API_KEY 입력
3) docker compose up -d --build
4) ./scripts/create-topics.sh

확인:
- Kafka raw 토픽에 메시지가 들어가는지: docker compose logs -f collector
- Spark가 파일을 쓰는지: storage/curated/event_date=YYYY-MM-DD/ 아래 JSON 생성 확인
