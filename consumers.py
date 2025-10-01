import json
import logging
import os
import pandas as pd
from kafka import KafkaConsumer
import chromadb
from sentence_transformers import SentenceTransformer
import redis
import threading

# --- BASIC CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- FILE PATHS & SHARED RESOURCES ---
CSV_FILE_PATH = 'products.csv'
FILE_LOCK = threading.Lock()  # To prevent race conditions when writing to the CSV

# --- SERVICE CLIENTS (re-initialized for the consumer process) ---
try:
    redis_client = redis.Redis(host='localhost', port=6379)
    redis_client.ping()
    logger.info("Consumer connected to Redis.")
except Exception as e:
    logger.error(f"Consumer could not connect to Redis: {e}")
    redis_client = None

try:
    chroma_client = chromadb.PersistentClient(path="./chroma_db")
    embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
    product_collection = chroma_client.get_or_create_collection(name="products_v2")
    logger.info("Consumer connected to ChromaDB.")
except Exception as e:
    logger.error(f"Consumer could not connect to ChromaDB: {e}")
    product_collection = None


def get_dataframe():
    """Safely reads the CSV file into a pandas DataFrame."""
    with FILE_LOCK:
        if not os.path.exists(CSV_FILE_PATH):
            return pd.DataFrame(columns=['id', 'name', 'description'])
        return pd.read_csv(CSV_FILE_PATH)


def save_dataframe(df):
    """Safely saves the DataFrame to the CSV file."""
    with FILE_LOCK:
        df.to_csv(CSV_FILE_PATH, index=False)


def invalidate_cache():
    """Invalidates the entire Redis search cache."""
    if redis_client:
        redis_client.flushall()
        logger.info("Redis cache invalidated due to product updates.")


class ProductCreationConsumer:
    """Handles the creation of new products."""

    def process(self, products_data):
        logger.info(f"Processing {len(products_data)} new products for creation.")
        df = get_dataframe()

        new_products = []
        for product in products_data:
            # Generate a unique ID
            new_id = f"prod_{len(df) + len(new_products) + 1:04d}"
            new_products.append({
                'id': new_id,
                'name': product.get('name'),
                'description': product.get('description')
            })

        if not new_products:
            return

        new_products_df = pd.DataFrame(new_products)
        df = pd.concat([df, new_products_df], ignore_index=True)
        save_dataframe(df)

        # Update ChromaDB
        if product_collection is not None:
            product_collection.add(
                ids=[p['id'] for p in new_products],
                documents=[f"{p['name']}: {p['description']}" for p in new_products],
                metadatas=[{'name': p['name']} for p in new_products]
            )
            logger.info(f"Added {len(new_products)} products to ChromaDB.")

        invalidate_cache()


class ProductEditionConsumer:
    """Handles updates to existing products."""

    def process(self, products_data):
        logger.info(f"Processing {len(products_data)} products for update.")
        df = get_dataframe()
        df.set_index('id', inplace=True)

        updated_products = []
        for product in products_data:
            prod_id = product.get('id')
            if prod_id in df.index:
                if 'name' in product:
                    df.loc[prod_id, 'name'] = product['name']
                if 'description' in product:
                    df.loc[prod_id, 'description'] = product['description']

                updated_products.append(df.loc[prod_id].to_dict())

        if not updated_products:
            return

        df.reset_index(inplace=True)
        save_dataframe(df)

        # Update ChromaDB (upsert is useful here)
        if product_collection is not None:
            # We need to re-fetch name/desc in case only one was provided
            final_updates = []
            df.set_index('id', inplace=True)
            for prod_id in [p['id'] for p in products_data if p.get('id') in df.index]:
                final_updates.append(df.loc[prod_id].to_dict())

            product_collection.update(
                ids=[p['id'] for p in final_updates],
                documents=[f"{p['name']}: {p['description']}" for p in final_updates],
                metadatas=[{'name': p['name']} for p in final_updates]
            )
            logger.info(f"Updated {len(updated_products)} products in ChromaDB.")

        invalidate_cache()


def run_consumers():
    """
    Initializes and runs the main Kafka consumer loop.
    It listens to the 'product-updates' topic and dispatches
    messages to the appropriate processor.
    """
    try:
        consumer = KafkaConsumer(
            'product-updates',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            group_id='product-processor-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        logger.info("Kafka Consumer connected. Waiting for messages...")
    except Exception as e:
        logger.error(f"Could not connect Kafka Consumer: {e}")
        return

    creation_handler = ProductCreationConsumer()
    edition_handler = ProductEditionConsumer()

    for message in consumer:
        try:
            data = message.value
            action = data.get('action')
            payload = data.get('data')

            if action == 'product-creation':
                creation_handler.process(payload)
            elif action == 'product-edition':
                edition_handler.process(payload)
            else:
                logger.warning(f"Unknown action received: {action}")

        except Exception as e:
            logger.error(f"Failed to process message: {message.value}. Error: {e}")


if __name__ == '__main__':
    run_consumers()
