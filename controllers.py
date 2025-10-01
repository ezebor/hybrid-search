import chromadb
from flask import Flask, request, jsonify, render_template
from kafka import KafkaProducer
import json
import redis
import logging
import time

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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

redis_client = connect_to_service("Redis", lambda: redis.Redis(host='redis', port=6379, decode_responses=True))
producer = connect_to_service("Kafka", lambda: KafkaProducer(bootstrap_servers='broker:9092',
                                                             value_serializer=lambda v: json.dumps(v).encode('utf-8')))


class Controller:
    def __init__(self, app):
        self.app = app
        self.register_routes()

    def register_routes(self):
        @self.app.route('/')
        def index():
            return render_template('index.html')

        @self.app.route('/products', methods=['POST'])
        def create_products():
            products = request.get_json()
            if not isinstance(products, list):
                return jsonify({"error": "Request body must be a list of products"}), 400
            producer.send('product-updates', {'action': 'product-creation', 'payload': products})
            producer.flush()
            return jsonify({"status": "Product creation request sent"}), 202

        @self.app.route('/products', methods=['PUT'])
        def update_products():
            products = request.get_json()
            if not isinstance(products, list):
                return jsonify({"error": "Request body must be a list of products"}), 400
            producer.send('product-updates', {'action': 'product-edition', 'payload': products})
            producer.flush()
            logging.info("Product update detected. Invalidating all search caches.")
            keys = redis_client.keys('search:*')
            if keys:
                redis_client.delete(*keys)
            return jsonify({"status": "Product update request sent"}), 202

        @self.app.route('/products/search', methods=['GET'])
        def search_products():
            query = request.args.get('q', '')
            if not query:
                return jsonify({"error": "Query parameter 'q' is required"}), 400

            cache_key = f"search:{query.lower().strip()}"
            cached_results = redis_client.get(cache_key)

            if cached_results:
                logging.info(f"CACHE HIT for query: '{query}'")
                return jsonify(json.loads(cached_results))

            logging.info(f"CACHE MISS for query: '{query}'")
            results = collection.query(
                query_texts=[query],
                n_results=10
            )

            formatted_results = []
            if results and results['ids'] and results['ids'][0]:
                for i, doc_id in enumerate(results['ids'][0]):
                    description_full = results['documents'][0][i]
                    description_parts = description_full.split(":", 1)
                    description = description_parts[-1].strip() if len(description_parts) > 1 else description_full

                    formatted_results.append({
                        "id": doc_id,
                        "name": results['metadatas'][0][i]['name'],
                        "description": description
                    })

            redis_client.setex(cache_key, 3600, json.dumps(formatted_results))
            return jsonify(formatted_results)

    def run(self):
        self.app.run(host='0.0.0.0', port=5000)

