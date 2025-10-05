import json
import pickle
import pandas as pd
import os
import redis
import threading
import io
import math
from flask import Blueprint, request, jsonify, render_template, current_app
from kafka import KafkaProducer
from recommender import ProductRecommender

# --- CONFIGURATION & CONNECTIONS (Unchanged) ---
KAFKA_TOPIC = 'product-events'
KAFKA_BROKER = 'broker:29092'
REDIS_HOST = 'redis'
REDIS_PORT = 6379
REDIS_MODEL_KEY = 'recommender_model'
DATA_DIR = "/app/data"
CSV_PATH = os.path.join(DATA_DIR, "products.csv")

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
kafka_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

api_bp = Blueprint('api', __name__)
recommender_cache = {'model': None}


# --- Background CSV Processing ---
def process_csv_in_background(app, csv_stream, batch_size=100):
    """Reads a CSV stream, handles the 'active' column, and sends batches to Kafka."""
    with app.app_context():
        try:
            df = pd.read_csv(csv_stream)
            if not {'id', 'name', 'description', 'price'}.issubset(df.columns):
                current_app.logger.error("CSV is missing required columns (id, name, description, price).")
                return

            # --- MODIFICATION: Handle the 'active' column ---
            if 'active' not in df.columns:
                current_app.logger.info(
                    "'active' column not found in uploaded CSV. Defaulting all new products to active=1.")
                df['active'] = 1

            df['name'] = df['name'].fillna('')
            df['description'] = df['description'].fillna('')
            df['price'] = df['price'].fillna(0)
            df['active'] = df['active'].fillna(1)  # Default any missing active values to 1

            total_rows = len(df)
            num_batches = math.ceil(total_rows / batch_size)
            current_app.logger.info(f"Starting background import of {total_rows} products in {num_batches} batches.")

            for i in range(0, total_rows, batch_size):
                batch_df = df.iloc[i:i + batch_size]
                products = batch_df.to_dict(orient='records')

                message = {'action': 'product-creation', 'products': products}
                kafka_producer.send(KAFKA_TOPIC, message)
                current_app.logger.info(f"Sent batch {i // batch_size + 1}/{num_batches} to Kafka.")

            kafka_producer.flush()
            current_app.logger.info("All batches have been sent to Kafka.")

        except Exception as e:
            current_app.logger.error(f"Error processing CSV in background: {e}")


# --- API Endpoints (Unchanged, as filtering logic is now handled by the recommender) ---
def get_recommender():
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
        return jsonify({"error": "Recommender model is not available yet. Try importing data."}), 503

    recommended_ids = recommender.recommend(name=query, description="", price=0)

    try:
        df = pd.read_csv(CSV_PATH)
        results_df = df[df['id'].isin(recommended_ids)]

        results_df['id'] = pd.Categorical(results_df['id'], categories=recommended_ids, ordered=True)
        results_df = results_df.sort_values('id')

        results = results_df.to_dict(orient='records')

    except FileNotFoundError:
        return jsonify({"error": "Product data not found."}), 500

    redis_client.setex(cache_key, 3600, json.dumps(results))
    return jsonify(results)


@api_bp.route('/products/import', methods=['POST'])
def import_products():
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    if file and file.filename.endswith('.csv'):
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)

        app = current_app._get_current_object()
        thread = threading.Thread(target=process_csv_in_background, args=(app, stream))
        thread.daemon = True
        thread.start()

        return jsonify(
            {"status": "success", "message": "Product import started. The model will be updated shortly."}), 202

    return jsonify({"error": "Invalid file type. Please upload a CSV file."}), 400

