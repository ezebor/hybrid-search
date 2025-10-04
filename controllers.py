import json
import pickle
import pandas as pd
import os
import redis
from flask import Blueprint, request, jsonify, render_template, current_app
from kafka import KafkaProducer
from recommender import ProductRecommender

# --- CONFIGURATION ---
KAFKA_TOPIC = 'product-events'
KAFKA_BROKER = 'broker:29092'
REDIS_HOST = 'redis'
REDIS_PORT = 6379
REDIS_MODEL_KEY = 'recommender_model'
DATA_DIR = "/app/data"
CSV_PATH = os.path.join(DATA_DIR, "products.csv")

# --- CONNECTIONS ---
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

api_bp = Blueprint('api', __name__)
recommender_cache = {'model': None}


def get_recommender():
    """Loads the recommender model from Redis if not in memory."""
    if recommender_cache['model'] is None:
        try:
            serialized_model = redis_client.get(REDIS_MODEL_KEY)
            if serialized_model:
                recommender_cache['model'] = pickle.loads(serialized_model)
                current_app.logger.info("Loaded recommender model from Redis into memory.")
            else:
                current_app.logger.warning("Recommender model not found in Redis.")
                return None
        except Exception as e:
            current_app.logger.error(f"Failed to load model from Redis: {e}")
            return None
    return recommender_cache['model']


@api_bp.route('/')
def index():
    return render_template('index.html')


@api_bp.route('/products/search')
def search_products():
    query = request.args.get('q', '')
    if not query:
        return jsonify({"error": "Query parameter 'q' is required."}), 400

    cache_key = f"search:{query}"
    cached_results = redis_client.get(cache_key)
    if cached_results:
        current_app.logger.info(f"CACHE HIT for query: '{query}'")
        return jsonify(json.loads(cached_results))

    current_app.logger.info(f"CACHE MISS for query: '{query}'")
    recommender = get_recommender()
    if not recommender:
        return jsonify({"error": "Recommender model is not available yet."}), 503

    recommended_ids = recommender.recommend(name=query, description="", price=0)

    try:
        df = pd.read_csv(CSV_PATH)

        # Filter the DataFrame to only get the rows for the recommended products
        results_df = df[df['id'].isin(recommended_ids)]

        # --- MODIFICATION: Reorder the results to match the similarity ranking ---
        # We convert the 'id' column to a categorical type that has a custom order
        # based on the `recommended_ids` list, then sort by it.
        results_df['id'] = pd.Categorical(results_df['id'], categories=recommended_ids, ordered=True)
        results_df = results_df.sort_values('id')

        results = results_df.to_dict(orient='records')

    except FileNotFoundError:
        return jsonify({"error": "Product data not found."}), 500

    redis_client.setex(cache_key, 3600, json.dumps(results))  # Cache for 1 hour
    return jsonify(results)


@api_bp.route('/products', methods=['POST'])
def create_products():
    products = request.get_json()
    message = {'action': 'product-creation', 'products': products}
    kafka_producer.send(KAFKA_TOPIC, message)
    kafka_producer.flush()
    return jsonify({"status": "success", "message": "Event sent."}), 202


@api_bp.route('/products', methods=['PUT'])
def update_products():
    products = request.get_json()
    message = {'action': 'product-edition', 'products': products}
    kafka_producer.send(KAFKA_TOPIC, message)
    kafka_producer.flush()
    recommender_cache['model'] = None
    return jsonify({"status": "success", "message": "Event sent."}), 202