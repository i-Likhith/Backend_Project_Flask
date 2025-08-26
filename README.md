# Amazon Product Scraper API

A Flask-based RESTful API for interacting with Amazon product data, offering both database-driven retrieval and on-demand web scraping capabilities.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Setup & Running](#setup--running)
- [API Usage](#api-usage)
- [Troubleshooting](#troubleshooting)

## Overview

This project provides a backend API with dual functionality:
- **Option 1: Data Retrieval from Database:** Serve product information already stored in a MySQL or Snowflake database.
- **Option 2: On-Demand Web Scraping:** Dynamically acquire new product data from Amazon URLs through an API trigger.

## Features

-   **Option 1: Retrieving Data from Database (Serving Pre-existing Data)**
    *   Exposes product data stored in category-specific tables (e.g., `products`, `clothes`, `laptops`, `grocery`, etc.).
    *   Ideal for applications needing to display or analyze historical product information.

-   **Option 2: Triggering On-Demand Web Scraping (Generating Data from URLs)**
    *   Initiates web scraping of Amazon.in via API calls.
    *   Supports scraping specific user-provided URLs or a list of pre-configured URLs.
    *   Automatically saves newly scraped data into the database.

-   **URL Management:** API to add, list, and delete Amazon URLs for scraping.
-   **URL Health Check:** Verify the HTTP status of product links.
-   **Modular Design:** Organized into `model`, `controller`, and `util` layers for clear separation of concerns.

## Project Structure

The project follows an MVC-like pattern:
-   `app.py`: Main Flask application, defines all API routes.
-   `model/`: Handles database connections and web scraping logic.
-   `util/`: Provides general utility functions and standardized API responses.
-   `controller/`: Contains the core business logic for API operations.

## Prerequisites

Before you begin, ensure you have:
-   Python 3.8+
-   Git
-   MySQL Server (or Snowflake account)

## Setup & Running

1.  Clone the repository.
2.  Create and activate a Python virtual environment.
3.  Install project dependencies (listed in `requirements.txt`).
4.  **Database Setup:** Manually create the `amazon_scraper_db` database and all necessary tables (e.g., `products`, `clothes`, `scrape_urls`) in your chosen database. Refer to the project's SQL scripts for detailed table schemas and sample data insertion.
5.  Configure your database credentials within `controller/products_controller.py`.
6.  Run the Flask application.

## API Usage

The API is accessible at `http://127.0.0.1:5000/`. All core API endpoints are prefixed with `/api/`.

-   **API Overview:** `GET /api/`
-   **Data Retrieval (Option 1):** `GET /api/{category_name}` (e.g., `/api/laptops`)
-   **Web Scraping (Option 2):** `POST /api/scrape`
-   **URL Management:** `POST /api/urls`, `GET /api/urls`, `DELETE /api/urls/{url_id}`
-   **URL Health Check:** `POST /api/check-links`

## Troubleshooting

-   **"Method Not Allowed"**: Ensure correct HTTP method and request body format.
-   **Database Connection Errors**: Verify database server is running and credentials are correct. Confirm tables exist.
-   **Scraping Issues (e.g., 403 Forbidden)**: Amazon's anti-scraping measures are strict. Consider adding delays or using proxies.
-   **`ModuleNotFoundError`**: Check virtual environment activation and dependency installation.

---