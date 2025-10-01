# Semantic Product Search API with Kafka, ChromaDB, and Redis

This project demonstrates a complete, event-driven backend system for an e-commerce application. It allows for the creation and updating of products, which are then indexed into a vector database to enable powerful semantic search. The system is designed to be scalable and responsive, using a message queue to decouple the API from the data processing logic.

# Core Technologies

* Flask: A lightweight Python web framework used to create the REST API endpoints.
* Kafka: A distributed event streaming platform used as a message broker. It enables asynchronous processing of product creation and updates, ensuring the API remains fast.
* ChromaDB: An open-source vector database used to store product embeddings (vectors) and perform efficient similarity searches.
* Sentence-Transformers: A Python library used to generate high-quality vector embeddings from product names and descriptions.
* Redis: An in-memory data store used for caching search query results, significantly improving performance for repeated searches.
* Pandas: A data manipulation library used to manage the product catalog stored in a CSV file.

# Architecture Overview

The application is split into two main asynchronous flows: data ingestion/updating and data searching.

## 1. Product Creation & Update Flow (Event-Driven)

1. A user sends a POST or PUT request to the Flask API (/products) with product data.
2. The Flask controller immediately publishes a message containing the action (product-creation or product-edition) and data to a Kafka topic named product-updates. It then returns a 202 Accepted response to the user, indicating the request has been received but not yet fully processed.
3. A separate Python process (consumers.py) runs Kafka consumers subscribed to the product-updates topic.
4. The consumer receives the message, processes it, and performs the following actions:
    * Updates the products.csv file using Pandas.
    * Creates or updates the vector embedding in the ChromaDB collection.
    * Invalidates the Redis cache to ensure future searches return fresh data.

## 2. Product Search Flow

1. A user accesses the web frontend or sends a GET request to the /products/search?q=... API endpoint.
2. The Flask controller first checks if the exact search query exists as a key in the Redis cache.
3. Cache Hit: If the result is found in Redis, it is returned directly to the user for a very fast response.
4. Cache Miss: If the result is not in Redis, the controller queries the ChromaDB collection. ChromaDB finds the products whose vector embeddings are most similar to the vector of the search query.
5. The results from ChromaDB are then stored in the Redis cache with an expiration time.
6. The results are returned to the user.

# Project Structure

```
/your-project/
├── docker-compose.yml     # Defines the backend services (Kafka, Redis)
├── requirements.txt       # Python dependencies
├── main.py                # Entry point to run the Flask API server
├── controllers.py         # Contains the Flask routes and API logic
├── consumers.py           # Contains the Kafka consumers for data processing
├── /templates/
│   └── index.html         # Simple HTML/CSS/JS frontend
├── (chroma_db/)           # Directory for persistent ChromaDB data (auto-created)
└── (products.csv)         # Product database file (auto-created)
```

# Setup and Installation

## Prerequisites
* Python 3.8+
* Docker and Docker Compose

## Step-by-Step Guide
1. Start Backend Services: Open a terminal in the project root and run Docker Compose. This will download the required images and start Kafka, Zookeeper, and Redis in the background.

```
docker-compose up -d
```

2. Set Up Python Environment: It is highly recommended to use a virtual environment.

```
# Create a virtual environment
python -m venv venv

# Activate it
# On Windows: venv\Scripts\activate
# On macOS/Linux: source venv/bin/activate
```

3. Install Dependencies: Install all the required Python packages from the requirements.txt file.

```
pip install -r requirements.txt
```

## Running the Application
You must run the API server and the Kafka consumers in two separate terminals.

### Terminal 1: Start the Kafka Consumers
This process listens for messages and updates the databases.

```
python consumers.py
```

### Terminal 2: Start the Flask API Server
This process runs the web server and the search frontend.

```
python main.py
```

The API will now be running at http://localhost:5000.

## How to Use
### API Endpoints
You can use a tool like curl or Postman to interact with the API.

1. Create Products (POST /products)

```
curl -X POST http://localhost:5000/products \
-H "Content-Type: application/json" \
-d '[
  {"name": "Organic Green Tea", "description": "A refreshing and healthy blend of premium green tea leaves."},
  {"name": "Artisan Dark Roast Coffee", "description": "Rich and bold dark roast coffee beans, ethically sourced from South America."}
]'
```

2. Update Products (PUT /products)

```
curl -X PUT http://localhost:5000/products \
-H "Content-Type: application/json" \
-d '[
  {"id": "prod_0001", "description": "A refreshing and healthy blend of premium organic green tea leaves from the misty mountains."}
]'
```

3. Search for Products (GET /products/search)

```
curl "http://localhost:5000/products/search?q=a%20healthy%20morning%20drink"
```

### Web Frontend
Navigate to http://localhost:5000 in your web browser to use the simple and modern search interface.

