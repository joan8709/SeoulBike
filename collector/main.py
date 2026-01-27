import os, time, json
import requests
from kafka import KafkaProducer

def build_url(api_key: str) -> str:
    base = os.getenv("SEOUL_API_URL", "https://openapi.seoul.go.kr:8088")
    # 자전거 API는 보통 이런 형태(필요하면 bikeList 부분만 바꾸면 됨)
    return f"{base}/{api_key}/json/bikeList/1/1000"

def fetch_payload(url: str) -> dict:
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def main():
    api_key = os.environ["SEOUL_API_KEY"]
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic_raw = os.getenv("TOPIC_RAW", "bike_raw")
    poll = int(os.getenv("POLL_SECONDS", "60"))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=200,
    )

    url = build_url(api_key)

    while True:
        try:
            payload = fetch_payload(url)
            event = {
                "fetched_at": int(time.time()),   # epoch seconds
                "source": "seoul-bike-api",
                "payload": payload,
            }
            producer.send(topic_raw, value=event)
            producer.flush()
            print(f"[collector] sent 1 event → {topic_raw}")
        except Exception as e:
            print(f"[collector] error: {e}")
        time.sleep(poll)

if __name__ == "__main__":
    main()
