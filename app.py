from flask import Flask
from controllers import api_bp
import logging
import pandas as pd
import json
import time
import os
import redis
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- CONFIGURATION for Seeding ---
KAFKA_TOPIC = 'product-events'
KAFKA_BROKER = 'broker:29092'
REDIS_HOST = 'redis'
REDIS_PORT = 6379
# This path points to the products.csv copied by the Dockerfile
CSV_PATH = '/app/products.csv'
REDIS_SEED_LOCK_KEY = 'initial_seed_complete'


def seed_initial_data():
    """
    Checks if initial data has been seeded. If not, it reads the CSV
    and sends its content as a single 'product-creation' Kafka event.
    Uses Redis as a lock to ensure it only runs once.
    """
    try:
        # Connect to Redis to check the lock
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        if redis_client.get(REDIS_SEED_LOCK_KEY):
            logging.info("Initial data seeding has already been performed. Skipping.")
            return

        logging.info("Performing initial data seeding...")

        if not os.path.exists(CSV_PATH):
            logging.warning(f"'{CSV_PATH}' not found. No initial products will be seeded.")
            # Set lock even if file not found to prevent retrying on every startup
            redis_client.set(REDIS_SEED_LOCK_KEY, 'true')
            return

        df = pd.read_csv(CSV_PATH)
        products = df.to_dict(orient='records')

        if not products:
            logging.info("products.csv is empty. No seeding necessary.")
            redis_client.set(REDIS_SEED_LOCK_KEY, 'true')
            return

        # Connect to Kafka with retry logic
        producer = None
        backoff_time = 1
        while producer is None:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKER,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                logging.info("Seeder logic successfully connected to Kafka.")
            except NoBrokersAvailable:
                logging.error(f"Seeder could not connect to Kafka. Retrying in {backoff_time}s...")
                time.sleep(backoff_time)
                backoff_time = min(backoff_time * 2, 30)  # Cap retry time

        message = {
            'action': 'product-creation',
            'products': products
        }

        producer.send(KAFKA_TOPIC, message)
        producer.flush()
        producer.close()

        # Set the lock in Redis to prevent this function from running again
        redis_client.set(REDIS_SEED_LOCK_KEY, 'true')
        logging.info(f"Successfully sent initial batch of {len(products)} products and set Redis lock.")

    except Exception as e:
        logging.error(f"A critical error occurred during the seeding process: {e}")


def create_app():
    """Creates and configures the Flask application."""
    app = Flask(__name__, template_folder='./templates')
    app.register_blueprint(api_bp)

    # Set up basic logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    app.logger.setLevel(logging.INFO)

    app.logger.info("Flask application created and configured.")
    return app


if __name__ == '__main__':
    # A delay to give the Kafka broker time to be fully ready before sending a message.
    logging.info("API Server starting up. Waiting 15 seconds before checking for initial seed...")
    time.sleep(15)

    # Run the one-time seeding process
    seed_initial_data()

    # Create and run the Flask application
    app = create_app()
    app.run(host='0.0.0.0', port=5000, debug=True)

