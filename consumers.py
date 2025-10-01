import pandas as pd
from kafka import KafkaConsumer
import json
import os
import chromadb
from sentence_transformers import SentenceTransformer
import logging
import time

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - CONSUMER - %(levelname)s - %(message)s')

# --- Configuration ---
CSV_FILE = 'products.csv'


# --- Service Connections ---
def connect_to_service(service_name, connection_func, retries=5, delay=5):
    """Generic connection function with retries."""
    for i in range(retries):
        try:
            client = connection_func()
            logging.info(f"Successfully connected to {service_name}.")
            return client
        except Exception as e:
            logging.warning(
                f"{service_name} connection attempt {i + 1}/{retries} failed: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    logging.error(f"Could not connect to {service_name} after several retries. Exiting.")
    exit(1)


# Inside Docker, we use the service names from docker-compose.yml, not 'localhost'
chroma_client = connect_to_service("ChromaDB", lambda: chromadb.HttpClient(host='chroma', port=8000))
collection = chroma_client.get_or_create_collection(name="products")

model = SentenceTransformer('all-MiniLM-L6-v2')
logging.info("Sentence Transformer model loaded.")


class ProductProcessor:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.df = self._load_or_create_df()

    def _load_or_create_df(self):
        if os.path.exists(self.csv_file):
            logging.info(f"Loading existing products from {self.csv_file}")
            return pd.read_csv(self.csv_file)
        else:
            logging.info(f"Creating new products file: {self.csv_file}")
            df = pd.DataFrame(columns=['id', 'name', 'description'])
            df.to_csv(self.csv_file, index=False)
            return df

    def _generate_id(self):
        if self.df.empty:
            return "prod_0001"
        last_id = self.df['id'].max()
        last_num = int(last_id.split('_')[1])
        new_num = last_num + 1
        return f"prod_{new_num:04d}"

    def add_products(self, products):
        new_records = []
        for product in products:
            new_id = self._generate_id()
            record = {'id': new_id, 'name': product['name'], 'description': product['description']}
            new_records.append(record)
            new_df_record = pd.DataFrame([record])
            self.df = pd.concat([self.df, new_df_record], ignore_index=True)

        self.df.to_csv(self.csv_file, index=False)
        logging.info(f"Added {len(new_records)} products to CSV.")
        self._update_chroma(new_records)

    def update_products(self, products_to_update):
        updated_records = []
        for product_update in products_to_update:
            product_id = product_update['id']
            idx = self.df.index[self.df['id'] == product_id][0]
            if 'name' in product_update:
                self.df.at[idx, 'name'] = product_update['name']
            if 'description' in product_update:
                self.df.at[idx, 'description'] = product_update['description']
            updated_records.append(self.df.loc[idx].to_dict())

        self.df.to_csv(self.csv_file, index=False)
        logging.info(f"Updated {len(updated_records)} products in CSV.")
        self._update_chroma(updated_records)

    def _update_chroma(self, records):
        if not records:
            return

        documents_to_embed = [f"{p['name']}: {p['description']}" for p in records]
        embeddings = model.encode(documents_to_embed).tolist()
        product_ids = [p['id'] for p in records]
        product_metadata = [{"name": p['name']} for p in records]

        collection.upsert(
            ids=product_ids,
            embeddings=embeddings,
            metadatas=product_metadata,
            documents=documents_to_embed
        )
        logging.info(f"Upserted {len(records)} records into ChromaDB.")


def run_consumer():
    consumer = connect_to_service("Kafka", lambda: KafkaConsumer(
        'product-updates',
        bootstrap_servers='broker:9092',
        auto_offset_reset='earliest',
        group_id='product-processor-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    ))
    processor = ProductProcessor(CSV_FILE)

    logging.info("Consumer is waiting for messages...")
    for message in consumer:
        try:
            data = message.value
            action = data.get('action')
            payload = data.get('payload')
            logging.info(f"Received action: {action}")

            if action == 'product-creation':
                processor.add_products(payload)
            elif action == 'product-edition':
                processor.update_products(payload)
        except Exception as e:
            logging.error(f"Failed to process message: {message.value}. Error: {e}")


if __name__ == '__main__':
    run_consumer()

