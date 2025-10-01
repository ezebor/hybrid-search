import json
import logging
from flask import Flask, request, jsonify, render_template
from kafka import KafkaProducer
import redis
import chromadb
from sentence_transformers import SentenceTransformer

# --- BASIC CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- KAFKA PRODUCER SETUP ---
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logging.info("Kafka Producer connected successfully.")
except Exception as e:
    logging.error(f"Could not connect to Kafka Producer: {e}")
    producer = None

# --- REDIS CLIENT SETUP ---
try:
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    redis_client.ping()
    logging.info("Redis client connected successfully.")
except Exception as e:
    logging.error(f"Could not connect to Redis: {e}")
    redis_client = None

# --- VECTOR DB & MODEL SETUP ---
try:
    # Using a persistent client to store data on disk
    chroma_client = chromadb.PersistentClient(path="./chroma_db")
    # Using a sentence transformer model for creating embeddings
    embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
    # Get or create the collection. This is where vectors are stored.
    product_collection = chroma_client.get_or_create_collection(name="products_v2")
    logging.info("ChromaDB client and collection ready.")
except Exception as e:
    logging.error(f"Error setting up ChromaDB or SentenceTransformer model: {e}")
    chroma_client = None
    embedding_model = None
    product_collection = None


class Controller:
    """
    Handles the Flask API routes for product management and search.
    """

    def __init__(self):
        self.app = Flask(__name__, template_folder='templates')
        self.setup_routes()

    def setup_routes(self):
        """Defines the API endpoints."""
        self.app.route("/", self.index)
        self.app.route("/products", methods=['POST'])(self.create_products)
        self.app.route("/products", methods=['PUT'])(self.update_products)
        self.app.route("/products/search", methods=['GET'])(self.search_products)

    def index(self):
        """Renders the main search page."""
        return render_template('index.html')

    def create_products(self):
        """
        API Endpoint to create a batch of products.
        It sends a message to Kafka for asynchronous processing.
        """
        products_data = request.get_json()
        if not products_data or not isinstance(products_data, list):
            return jsonify({"error": "Invalid request body. Expected a list of products."}), 400

        if producer:
            message = {"action": "product-creation", "data": products_data}
            producer.send('product-updates', value=message)
            producer.flush()
            logging.info(f"Sent {len(products_data)} product creations to Kafka.")
            return jsonify({"status": "success", "message": "Product creation request received."}), 202
        else:
            return jsonify({"error": "Kafka producer is not available."}), 503

    def update_products(self):
        """
        API Endpoint to update a batch of products.
        It sends a message to Kafka for asynchronous processing.
        """
        products_data = request.get_json()
        if not products_data or not isinstance(products_data, list):
            return jsonify({"error": "Invalid request body. Expected a list of products with IDs."}), 400

        if producer:
            message = {"action": "product-edition", "data": products_data}
            producer.send('product-updates', value=message)
            producer.flush()
            logging.info(f"Sent {len(products_data)} product updates to Kafka.")
            return jsonify({"status": "success", "message": "Product update request received."}), 202
        else:
            return jsonify({"error": "Kafka producer is not available."}), 503

    def search_products(self):
        """
        API Endpoint to search for products based on a query.
        It uses Redis for caching to improve performance.
        """
        query = request.args.get('q')
        if not query:
            return jsonify({"error": "Search query 'q' is required."}), 400

        # 1. Check Redis Cache first
        if redis_client:
            cached_results = redis_client.get(query)
            if cached_results:
                logging.info(f"CACHE HIT for query: '{query}'")
                return jsonify(json.loads(cached_results))

        logging.info(f"CACHE MISS for query: '{query}'")

        # 2. If not in cache, query ChromaDB
        if not product_collection:
            return jsonify({"error": "Vector database is not available."}), 503

        try:
            results = product_collection.query(
                query_texts=[query],
                n_results=10
            )

            # Format results for the frontend
            formatted_results = []
            if results and results['ids'][0]:
                for i, doc_id in enumerate(results['ids'][0]):
                    formatted_results.append({
                        "id": doc_id,
                        "name": results['metadatas'][0][i]['name'],
                        "description": results['documents'][0][i].split(":", 1)[1].strip(),
                        "distance": results['distances'][0][i]
                    })

            # 3. Store results in Redis cache
            if redis_client:
                # Cache results for 1 hour (3600 seconds)
                redis_client.set(query, json.dumps(formatted_results), ex=3600)

            return jsonify(formatted_results)
        except Exception as e:
            logging.error(f"An error occurred during search: {e}")
            return jsonify({"error": "Failed to perform search."}), 500

    def run(self):
        """Starts the Flask development server."""
        self.app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
