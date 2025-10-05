from flask import Flask
from controllers import api_bp
import logging

def create_app():
    """Creates and configures the Flask application."""
    app = Flask(__name__, template_folder='./templates')
    app.register_blueprint(api_bp)

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    app.logger.setLevel(logging.INFO)

    app.logger.info("Flask application created and configured.")
    return app


if __name__ == '__main__':
    app = create_app()
    # Running on host='0.0.0.0' is crucial for Docker
    app.run(host='0.0.0.0', port=5000, debug=True)

