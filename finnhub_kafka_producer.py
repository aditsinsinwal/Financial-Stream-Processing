import os
import time
import json
import logging
import requests
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FinnhubProducer")

# Read configuration from environment (with sensible defaults)
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SYMBOLS = os.getenv("SYMBOLS", "AAPL,MSFT,GOOG").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "1.0"))

if not FINNHUB_API_KEY:
    logger.error("FINNHUB_API_KEY is not set. Exiting.")
    exit(1)

# Initialize Kafka producer (async JSON serializer)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=5,
)

def fetch_quote(symbol: str) -> dict:
    """
    Fetch the latest quote for `symbol` from Finnhub’s REST API.
    Endpoint reference: https://finnhub.io/docs/api/quote
    """
    url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={api_key}"
    try:
        res = requests.get(url, timeout=5)
        res.raise_for_status()
        data = res.json()
        # data has keys: c (current price), h (high of day), l (low of day),
        # o (open of day), pc (previous close), t (timestamp)
        data["symbol"] = symbol
        return data
    except Exception as e:
        logger.warning(f"Failed to fetch quote for {symbol}: {e}")
        return {}

def main():
    logger.info(f"Starting Finnhub → Kafka producer for symbols: {SYMBOLS}")
    try:
        while True:
            timestamp = int(time.time())
            for sym in SYMBOLS:
                quote = fetch_quote(sym)
                if quote:
                    # Add our own timestamp
                    quote["fetched_at"] = timestamp
                    producer.send(KAFKA_TOPIC, value=quote)
                    logger.debug(f"Published quote for {sym}: {quote}")
            # Force-all pending messages to be sent every iteration
            producer.flush()
            time.sleep(POLL_INTERVAL_SEC)
    except KeyboardInterrupt:
        logger.info("Shutting down producer.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
