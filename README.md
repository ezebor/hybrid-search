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
