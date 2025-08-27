from flask import request
from model.amazon_scraper import AmazonScraper
from model.db_connector import DatabaseConnector
from util.url_checker import check_url_status
from util.response_handler import success_response, error_response, info_response, not_found_response, bad_request_response
from datetime import datetime

DB_TYPE = 'mysql' # <--- CHANGE THIS TO 'snowflake' IF YOU WANT TO USE SNOWFLAKE

MYSQL_CONFIG = {
    'host': 'localhost',
    'database': 'YOUR_MYSQL_DATABASE',
    'user': 'YOUR_MYSQL_USER',
    'password': 'YOUR_MYSQL_PASSWORD'
}

SNOWFLAKE_CONFIG = {
    'user': 'YOUR_SNOWFLAKE_USER',
    'password': 'YOUR_SNOWFLAKE_PASSWORD',
    'account': 'YOUR_SNOWFLAKE_ACCOUNT',
    'warehouse': 'YOUR_SNOWFLAKE_WAREHOUSE',
    'database': 'YOUR_SNOWFLAKE_DATABASE',
    'schema': 'YOUR_SNOWFLAKE_SCHEMA'
}

scraper = AmazonScraper()

if DB_TYPE == 'mysql':
    db_connector = DatabaseConnector(db_type=DB_TYPE, **MYSQL_CONFIG)
elif DB_TYPE == 'snowflake':
    db_connector = DatabaseConnector(db_type=DB_TYPE, **SNOWFLAKE_CONFIG)
else:
    raise ValueError("Invalid DB_TYPE specified in products_controller. Must be 'mysql' or 'snowflake'.")


PRODUCT_CATEGORIES = [
    'products',
    'clothes',
    'electronic_gadgets',
    'laptops',
    'home',
    'grocery',
    'kids_toys'
]


def get_api_root_info():
    endpoints_info = {
        "POST /api/scrape": "Scrape Amazon products from a given URL or all stored URLs. Requires 'url' or 'scrape_stored_urls': true in JSON body. (Inserts into 'products' table)",
        "POST /api/check-links": "Check the HTTP status of provided URLs or all links in the database. Requires 'links' (list of URLs) or 'check_all_db_links': true in JSON body.",
        "POST /api/urls": "Add a URL to the list of URLs to be scraped. Requires 'url' and optional 'description' in JSON body.",
        "GET /api/urls": "Retrieve all URLs stored for scraping.",
        "DELETE /api/urls/<int:url_id>": "Delete a URL from the stored list by its ID.",
        "Category Specific Endpoints (GET)": {}
    }

    for category in PRODUCT_CATEGORIES:
        endpoints_info["Category Specific Endpoints (GET)"][f"/api/{category}"] = f"Retrieve all products from the '{category}' table."

    return success_response(
        message="Welcome to the Amazon Product Scraper API!",
        data={"endpoints": endpoints_info}
    )

def handle_api_scrape():
    '''API logic to trigger Amazon scraping and save results to the database.'''
    data = request.get_json()
    
    if not data:
        return bad_request_response(message="Request body cannot be empty.")

    urls_to_scrape = []
    response_messages = []
    total_scraped_count = 0

    conn = db_connector.connect()
    if not conn:
        return error_response(message="Could not connect to database.")

    try:
        if data.get('scrape_stored_urls'):
            stored_urls_data = db_connector.get_all_scrape_urls()
            if not stored_urls_data:
                return info_response(message="No URLs found in the stored list to scrape.")
            urls_to_scrape = [(u['url'], u['id']) for u in stored_urls_data]
            response_messages.append("Scraping all stored URLs.")
        elif 'url' in data:
            single_url = data['url']
            if not single_url.startswith("http"):
                return bad_request_response(message="Invalid URL format. URL must start with http/https.")
            urls_to_scrape.append((single_url, None))
            response_messages.append(f"Scraping single URL: {single_url}")
        else:
            return bad_request_response(message="Missing 'url' or 'scrape_stored_urls': true in request body.")

        all_products_scraped = []
        for url, url_id in urls_to_scrape:
            print(f"API: Initiating scraping for URL: {url}")
            products = scraper.scrape_products(url)

            if products:
                db_connector.insert_products_into_table('products', products)
                if url_id:
                    db_connector.update_last_scraped_time(url_id)
                total_scraped_count += len(products)
                all_products_scraped.extend(products)
                response_messages.append(f"Scraped {len(products)} products from {url}.")
            else:
                response_messages.append(f"No products found or scraping failed for {url}.")

        return success_response(
            message=" | ".join(response_messages),
            data={
                "total_scraped_count": total_scraped_count,
                "products_preview": all_products_scraped[:10]
            }
        )
    finally:
        db_connector.close()

def get_products_by_category(category_name: str):
    '''API logic to retrieve all products from a specific category table.'''
    if category_name not in PRODUCT_CATEGORIES:
        return not_found_response(message=f"Category '{category_name}' not found.")

    conn = db_connector.connect()
    if not conn:
        return error_response(message="Could not connect to database to fetch products.")
    
    try:
        products_from_db = db_connector.fetch_products_from_table(category_name)
        return success_response(
            message=f"Retrieved {len(products_from_db)} products from the '{category_name}' table.",
            data={"products": products_from_db}
        )
    finally:
        db_connector.close()

def handle_api_check_links():
    '''API logic to check the HTTP status of provided URLs or all links in the database.'''
    data = request.get_json()
    links_to_check = []
    
    if data and data.get('check_all_db_links'):
        conn = db_connector.connect()
        if conn:
            all_db_links = set()
            for category in PRODUCT_CATEGORIES:
                products_in_category = db_connector.fetch_products_from_table(category)
                for p in products_in_category:
                    if p.get('link') and p['link'] != 'N/A':
                        all_db_links.add(p['link'])
            db_connector.close()
            links_to_check = list(all_db_links)

            if not links_to_check:
                return info_response(message="No valid links found in any product table to check.")
        else:
            return error_response(message="Could not connect to database to fetch links for checking.")
    elif data and 'links' in data and isinstance(data['links'], list):
        links_to_check = [link for link in data['links'] if isinstance(link, str) and link.startswith("http")]
        if not links_to_check:
            return bad_request_response(message="No valid HTTP/HTTPS links provided in the 'links' list.")
    else:
        return bad_request_response(message="Invalid request. Provide 'links' (list of URLs) or set 'check_all_db_links': true.")

    results = []
    for link in links_to_check:
        results.append(check_url_status(link))

    return success_response(
        message=f"Checked {len(results)} links.",
        data={"results": results}
    )

def handle_add_url_to_scrape():
    '''API logic to add a URL to the list of URLs to be scraped.'''
    data = request.get_json()
    if not data or 'url' not in data:
        return bad_request_response(message="Missing 'url' in request body.")

    url = data['url']
    description = data.get('description', '')

    conn = db_connector.connect()
    if not conn:
        return error_response(message="Could not connect to database.")
    
    try:
        success = db_connector.add_scrape_url(url, description)
        if success:
            return success_response(message=f"URL '{url}' added successfully.", status_code=201)
        else:
            return info_response(message=f"URL '{url}' already exists.")
    finally:
        db_connector.close()

def handle_get_stored_urls():
    '''API logic to retrieve all URLs stored for scraping.'''
    conn = db_connector.connect()
    if not conn:
        return error_response(message="Could not connect to database.")
    
    try:
        urls = db_connector.get_all_scrape_urls()
        return success_response(
            message=f"Retrieved {len(urls)} stored URLs.",
            data={"urls": urls}
        )
    finally:
        db_connector.close()

def handle_delete_stored_url(url_id: int):
    '''API logic to delete a URL from the stored list by its ID.'''
    conn = db_connector.connect()
    if not conn:
        return error_response(message="Could not connect to database.")
    
    try:
        success = db_connector.delete_scrape_url(url_id)
        if success:
            return success_response(message=f"URL with ID {url_id} deleted successfully.")
        else:
            return not_found_response(message=f"URL with ID {url_id} not found.")
    finally:
        db_connector.close()
