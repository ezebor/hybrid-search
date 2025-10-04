import os
import json
import time
import pandas as pd
import redis
import pickle
from kafka import KafkaConsumer
import logging
from recommender import ProductRecommender

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURATION ---
KAFKA_TOPIC = 'product-events'
KAFKA_BROKER = 'broker:29092'
REDIS_HOST = 'redis'
REDIS_PORT = 6379
REDIS_MODEL_KEY = 'recommender_model'
DATA_DIR = "/app/data"
CSV_PATH = os.path.join(DATA_DIR, "products.csv")

os.makedirs(DATA_DIR, exist_ok=True)


# --- CONNECTIONS ---
def connect_kafka_consumer():
    """Connects to Kafka with retry logic."""
    consumer = None
    backoff_time = 1
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC, bootstrap_servers=KAFKA_BROKER, auto_offset_reset='earliest',
                group_id='product-recommender-group', value_deserializer=lambda x: json.loads(x.decode('utf-8')))
            logging.info("Successfully connected to Kafka.")
        except Exception as e:
            logging.error(f"Kafka connection failed: {e}. Retrying in {backoff_time}s...")
            time.sleep(backoff_time)
            backoff_time *= 2
    return consumer


def connect_redis_client():
    """Connects to Redis with retry logic."""
    r = None
    backoff_time = 1
    while r is None:
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
            r.ping()
            logging.info("Successfully connected to Redis.")
        except Exception as e:
            logging.error(f"Redis connection failed: {e}. Retrying in {backoff_time}s...")
            time.sleep(backoff_time)
            backoff_time *= 2
    return r


class ProductProcessor:
    def __init__(self, redis_client):
        self.redis_client = redis_client

    # --- MODIFICATION: Renamed to process_event and now accepts the event dictionary directly ---
    def process_event(self, event):
        """Processes an event dictionary to update data and retrain the model."""
        action = event.get('action')
        products = event.get('products')
        logging.info(f"Processing event: {action} for {len(products)} products.")

        df = self._update_csv(products, action)

        if not df.empty:
            recommender = ProductRecommender(k=10)
            recommender.fit(df)

            serialized_model = pickle.dumps(recommender)
            self.redis_client.set(REDIS_MODEL_KEY, serialized_model)
            logging.info(f"New recommender model saved to Redis key '{REDIS_MODEL_KEY}'.")

        self._invalidate_redis_cache()

    def _update_csv(self, products, action):
        """Loads, updates, and saves the products.csv file."""
        try:
            df = pd.read_csv(CSV_PATH) if os.path.exists(CSV_PATH) else pd.DataFrame(
                columns=['id', 'name', 'description', 'price'])
        except pd.errors.EmptyDataError:
            df = pd.DataFrame(columns=['id', 'name', 'description', 'price'])

        if action == 'product-creation':
            new_products_df = pd.DataFrame(products)
            df = pd.concat([df, new_products_df], ignore_index=True).drop_duplicates(subset=['id'], keep='last')
        elif action == 'product-edition':
            for prod in products:
                if prod['id'] in df['id'].values:
                    for key, value in prod.items():
                        df.loc[df['id'] == prod['id'], key] = value
                else:
                    new_prod_df = pd.DataFrame([prod])
                    df = pd.concat([df, new_prod_df], ignore_index=True)

        df.to_csv(CSV_PATH, index=False)
        logging.info(f"CSV file updated. Total products: {len(df)}")
        return df

    def _invalidate_redis_cache(self):
        """Deletes all search query caches from Redis."""
        logging.info("Invalidating Redis search cache...")
        keys = self.redis_client.keys('search:*')
        if keys:
            self.redis_client.delete(*keys)
        logging.info(f"Deleted {len(keys)} cache entries.")


def main():
    """Initializes connections and starts the consumer loop."""
    redis_client = connect_redis_client()
    kafka_consumer = connect_kafka_consumer()
    processor = ProductProcessor(redis_client)

    # --- MODIFICATION: The logic for initial training is now clearer ---
    if os.path.exists(CSV_PATH):
        logging.info("Performing initial training based on existing CSV.")
        initial_event = {'action': 'initial-training', 'products': []}
        # Call the refactored method with the event dictionary directly
        processor.process_event(initial_event)

    logging.info("Consumer is running. Waiting for messages...")
    for message in kafka_consumer:
        # --- MODIFICATION: Pass the message's value (the event dictionary) to the processing method ---
        processor.process_event(message.value)


if __name__ == "__main__":
    main()