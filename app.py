from flask import Flask, jsonify, request
from model.db_connector import DatabaseConnector # Still needed for initial setup
import controller.products_controller as products_controller # Import the controller module

app = Flask(__name__)

DB_TYPE = 'mysql' # <--- CHANGE THIS TO 'snowflake' IF YOU WANT TO USE SNOWFLAKE

MYSQL_CONFIG = {
    'host': 'localhost',
    'database': 'amazon_scraper_db',
    'user': 'Devuser',
    'password': '1234'
}

SNOWFLAKE_CONFIG = {
    'user': 'YOUR_SNOWFLAKE_USER',
    'password': 'YOUR_SNOWFLAKE_PASSWORD',
    'account': 'YOUR_SNOWFLAKE_ACCOUNT',
    'warehouse': 'YOUR_SNOWFLAKE_WAREHOUSE',
    'database': 'YOUR_SNOWFLAKE_DATABASE',
    'schema': 'YOUR_SNOWFLAKE_SCHEMA'
}

# --- Initial Database Setup (runs once on app startup) ---
# Create a temporary db_connector instance ONLY for creating tables.
# The db_connector for request handling is now in products_controller.py
if DB_TYPE == 'mysql':
    initial_db_connector = DatabaseConnector(db_type=DB_TYPE, **MYSQL_CONFIG)
elif DB_TYPE == 'snowflake':
    initial_db_connector = DatabaseConnector(db_type=DB_TYPE, **SNOWFLAKE_CONFIG)
else:
    raise ValueError("Invalid DB_TYPE specified. Must be 'mysql' or 'snowflake'.")

with app.app_context():
    conn = initial_db_connector.connect()
    if conn:
        initial_db_connector.create_tables()
        initial_db_connector.close()
# The 'initial_db_connector' instance is now out of scope and will be garbage collected.

@app.route('/')
def root_index():
    return jsonify({
        "message": "Welcome to the Amazon Product Scraper API Backend!",
        "api_documentation_url": "/api/",
        "instructions": "Append '/api/' to the base URL to access the API endpoints."
    })

@app.route('/api/')
def api_root_info():
    return products_controller.get_api_root_info()

@app.route('/api/scrape', methods=['POST'])
def api_scrape_route():
    """Triggers Amazon scraping and saves results to the database."""
    return products_controller.handle_api_scrape()

for category in products_controller.PRODUCT_CATEGORIES:
    @app.route(f'/api/{category}', methods=['GET'], endpoint=f'get_{category}_products')
    def get_category_products_route(cat=category):
        """Retrieves all products from a specific category table."""
        return products_controller.get_products_by_category(category_name=cat)

@app.route('/api/check-links', methods=['POST'])
def api_check_links_route():
    """Checks the HTTP status of provided URLs or all links in the database."""
    return products_controller.handle_api_check_links()

@app.route('/api/urls', methods=['POST'])
def add_url_to_scrape_route():
    """Adds a URL to the list of URLs to be scraped."""
    return products_controller.handle_add_url_to_scrape()

@app.route('/api/urls', methods=['GET'])
def get_stored_urls_route():
    """Retrieves all URLs stored for scraping."""
    return products_controller.handle_get_stored_urls()

@app.route('/api/urls/<int:url_id>', methods=['DELETE'])
def delete_stored_url_route(url_id):
    """Deletes a URL from the stored list by its ID."""
    return products_controller.handle_delete_stored_url(url_id=url_id)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)

