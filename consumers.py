import os
import json
import time
import pandas as pd
import redis
import pickle
from kafka import KafkaConsumer
import logging
from recommender import ProductRecommender

# --- CONFIGURATION & CONNECTIONS (Unchanged) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
KAFKA_TOPIC = 'product-events'
KAFKA_BROKER = 'broker:29092'
REDIS_HOST = 'redis'
REDIS_PORT = 6379
REDIS_MODEL_KEY = 'recommender_model'
DATA_DIR = "/app/data"
CSV_PATH = os.path.join(DATA_DIR, "products.csv")
os.makedirs(DATA_DIR, exist_ok=True)


def connect_kafka_consumer():
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

    def process_event(self, event):
        action = event.get('action')
        products = event.get('products')
        logging.info(f"Processing event: {action} for {len(products)} products.")

        df = self._update_csv(products, action)

        # The recommender will handle filtering for active products internally
        recommender = ProductRecommender(k=10)
        recommender.fit(df)

        # Only save a model if it was successfully trained on active products
        if recommender.model is not None:
            serialized_model = pickle.dumps(recommender)
            self.redis_client.set(REDIS_MODEL_KEY, serialized_model)
            logging.info(f"New recommender model saved to Redis key '{REDIS_MODEL_KEY}'.")
        else:
            # If no active products, clear any old model from Redis
            self.redis_client.delete(REDIS_MODEL_KEY)
            logging.warning("No active products found; existing model cleared from Redis.")

        self._invalidate_redis_cache()

    def _update_csv(self, products, action):
        """Loads, updates (handling 'active' column), and saves the products.csv file."""
        try:
            df = pd.read_csv(CSV_PATH) if os.path.exists(CSV_PATH) else pd.DataFrame(
                columns=['id', 'name', 'description', 'price', 'active'])
        except pd.errors.EmptyDataError:
            df = pd.DataFrame(columns=['id', 'name', 'description', 'price', 'active'])

        # --- MODIFICATION: Ensure 'active' column exists in the DataFrame ---
        if 'active' not in df.columns:
            df['active'] = 1

        if action == 'product-creation':
            # Ensure incoming products have a default 'active' status if missing
            for p in products:
                p.setdefault('active', 1)
            new_products_df = pd.DataFrame(products)
            df = pd.concat([df, new_products_df], ignore_index=True).drop_duplicates(subset=['id'], keep='last')

        elif action == 'product-edition':
            for prod in products:
                # Ensure 'active' defaults to 1 if not provided in the update
                prod.setdefault('active', 1)
                if prod['id'] in df['id'].values:
                    for key, value in prod.items():
                        df.loc[df['id'] == prod['id'], key] = value
                else:  # If ID not found, treat as creation
                    new_prod_df = pd.DataFrame([prod])
                    df = pd.concat([df, new_prod_df], ignore_index=True)

        df.to_csv(CSV_PATH, index=False)
        logging.info(f"CSV file updated. Total products: {len(df)}")
        return df

    def _invalidate_redis_cache(self):
        logging.info("Invalidating Redis search cache...")
        keys = self.redis_client.keys('search:*')
        if keys:
            self.redis_client.delete(*keys)
        logging.info(f"Deleted {len(keys)} cache entries.")


def main():
    redis_client = connect_redis_client()
    kafka_consumer = connect_kafka_consumer()
    processor = ProductProcessor(redis_client)

    logging.info("Consumer is running. Waiting for messages...")
    for message in kafka_consumer:
        processor.process_event(message.value)


if __name__ == "__main__":
    main()

