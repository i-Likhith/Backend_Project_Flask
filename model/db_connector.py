import mysql.connector
from mysql.connector import Error as MySQL_Error
import snowflake.connector
from datetime import datetime

class DatabaseConnector:
    def __init__(self, db_type='mysql', **db_config):
        self.db_type = db_type
        self.db_config = db_config
        self.conn = None

        # Set default configurations if not provided (for development convenience)
        if self.db_type == 'mysql' and not self.db_config:
            self.db_config = {
                'host': 'localhost',
                'database': 'amazon_scraper_db',
                'user': 'Devuser',
                'password': '1234'
            }
            print("\n--- MySQL Configuration Warning ---")
            print("Default MySQL config used. Please update db_config in app.py or controller/products_controller.py with your actual MySQL credentials.")
            print("Example: DatabaseConnector(db_type='mysql', host='your_host', database='your_db', user='your_user', password='your_password')")
            print("--- End MySQL Configuration Warning ---\n")
        elif self.db_type == 'snowflake' and not self.db_config:
            self.db_config = {
                'user': 'YOUR_SNOWFLAKE_USER',
                'password': 'YOUR_SNOWFLAKE_PASSWORD',
                'account': 'YOUR_SNOWFLAKE_ACCOUNT',
                'warehouse': 'YOUR_SNOWFLAKE_WAREHOUSE',
                'database': 'YOUR_SNOWFLAKE_DATABASE',
                'schema': 'YOUR_SNOWFLAKE_SCHEMA'
            }
            print("\n--- Snowflake Configuration Warning ---")
            print("Default Snowflake config used. Please update db_config in app.py or controller/products_controller.py with your actual Snowflake credentials.")
            print("--- End Snowflake Configuration Warning ---\n")

    def connect(self):
        '''Establishes a database connection.'''
        if self.conn and self.conn.is_connected():
            # print(f"Already connected to {self.db_type} database.") # Can be noisy
            return self.conn

        try:
            if self.db_type == 'mysql':
                self.conn = mysql.connector.connect(**self.db_config)
                if self.conn.is_connected():
                    print(f"Connected to MySQL database: {self.db_config.get('database')}")
                else:
                    print("Failed to connect to MySQL database.")
                    self.conn = None
            elif self.db_type == 'snowflake':
                self.conn = snowflake.connector.connect(**self.db_config)
                print(f"Connected to Snowflake database: {self.db_config.get('database')}")
            else:
                print("Unsupported database type.")
                self.conn = None
        except MySQL_Error as e:
            print(f"Error connecting to MySQL: {e}")
            self.conn = None
        except snowflake.connector.errors.ProgrammingError as e:
            print(f"Error connecting to Snowflake: {e}")
            self.conn = None
        except Exception as e:
            print(f"An unexpected error occurred during database connection: {e}")
            self.conn = None
        return self.conn

    def close(self):
        '''Closes the database connection.'''
        if self.conn and self.conn.is_connected():
            self.conn.close()
            # print("Database connection closed.") # Can be noisy
            self.conn = None

    def create_tables(self):
        '''Creates all necessary tables if they don't exist.'''
        if not self.conn or not self.conn.is_connected():
            print("No active database connection to create tables.")
            return

        cursor = self.conn.cursor()
        try:
            # Table definitions for all product categories
            table_schemas = {
                'products': """
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    price VARCHAR(50),
                    rating VARCHAR(50),
                    link VARCHAR(1024) NOT NULL
                """,
                'clothes': """
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    price VARCHAR(50),
                    rating VARCHAR(50),
                    link VARCHAR(1024) NOT NULL
                """,
                'electronic_gadgets': """
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    price VARCHAR(50),
                    rating VARCHAR(50),
                    link VARCHAR(1024) NOT NULL
                """,
                'laptops': """
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    price VARCHAR(50),
                    rating VARCHAR(50),
                    link VARCHAR(1024) NOT NULL
                """,
                'home': """
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    price VARCHAR(50),
                    rating VARCHAR(50),
                    link VARCHAR(1024) NOT NULL
                """,
                'grocery': """
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    price VARCHAR(50),
                    rating VARCHAR(50),
                    link VARCHAR(1024) NOT NULL
                """,
                'kids_toys': """
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    price VARCHAR(50),
                    rating VARCHAR(50),
                    link VARCHAR(1024) NOT NULL
                """
            }

            # Create product tables and add unique indexes
            for table_name, schema in table_schemas.items():
                if self.db_type == 'mysql':
                    create_query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            {schema},
                            INDEX idx_{table_name}_name (name(191)),
                            UNIQUE INDEX idx_{table_name}_link_unique (link(255))
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                    """
                elif self.db_type == 'snowflake':
                     create_query = f"""
                        CREATE OR REPLACE TABLE {table_name} (
                            id INT IDENTITY(1,1),
                            name VARCHAR(255) NOT NULL,
                            price VARCHAR(50),
                            rating VARCHAR(50),
                            link VARCHAR(1024) UNIQUE
                        );
                    """
                cursor.execute(create_query)
                print(f"{self.db_type} table '{table_name}' ensured.")

            # Create scrape_urls table (remains the same)
            if self.db_type == 'mysql':
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS scrape_urls (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        url VARCHAR(1024) NOT NULL,
                        description VARCHAR(255),
                        last_scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_scrape_url (url(191)),
                        UNIQUE INDEX idx_url_unique (url(255))
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                ''')
            elif self.db_type == 'snowflake':
                cursor.execute('''
                    CREATE OR REPLACE TABLE scrape_urls (
                        id INT IDENTITY(1,1),
                        url VARCHAR(1024) UNIQUE NOT NULL,
                        description VARCHAR(255),
                        last_scraped_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
                    );
                ''')
            print(f"{self.db_type} table 'scrape_urls' ensured.")

            self.conn.commit()
        except (MySQL_Error, snowflake.connector.errors.ProgrammingError) as e:
            print(f"Error creating tables: {e}")
            self.conn.rollback()
        except Exception as e:
            print(f"An unexpected error occurred during table creation: {e}")
            self.conn.rollback()
        finally:
            cursor.close()

    def insert_products_into_table(self, table_name, products_data):
        '''Inserts a list of product dictionaries into the specified table.'''
        if not self.conn or not self.conn.is_connected():
            print(f"No active database connection to insert products into {table_name}.")
            return

        cursor = self.conn.cursor()
        inserted_count = 0
        insert_query = f"INSERT IGNORE INTO {table_name} (name, price, rating, link) VALUES (%s, %s, %s, %s)"
        if self.db_type == 'snowflake':
            # Snowflake doesn't have INSERT IGNORE, so we rely on UNIQUE constraint for duplicates
            insert_query = f"INSERT INTO {table_name} (name, price, rating, link) VALUES (%s, %s, %s, %s)"

        for product in products_data:
            name = product.get("Product Name")
            price = product.get("Price")
            rating = product.get("Rating")
            link = product.get("Link")
            try:
                cursor.execute(insert_query, (name, price, rating, link))
                self.conn.commit()
                if cursor.rowcount > 0:
                    inserted_count += 1
            except (MySQL_Error, snowflake.connector.errors.ProgrammingError) as e:
                # Handle duplicate entry errors specifically
                if (self.db_type == 'mysql' and e.errno == 1062) or \
                   (self.db_type == 'snowflake' and "unique constraint" in str(e).lower()):
                    # print(f"Skipping duplicate product '{name}' (Link: {link}) in {table_name}.") # Can be noisy
                    pass
                else:
                    print(f"Error inserting product '{name}' into {table_name} (Link: {link}): {e}")
                    self.conn.rollback()
            except Exception as e:
                print(f"An unexpected error occurred inserting product '{name}' into {table_name}: {e}")
                self.conn.rollback()
        print(f"Attempted to insert {len(products_data)} products into {table_name}. Successfully inserted/ignored {inserted_count} new products.")
        cursor.close()

    def fetch_products_from_table(self, table_name):
        '''Fetches all products from the specified table.'''
        if not self.conn or not self.conn.is_connected():
            print(f"No active database connection to fetch products from {table_name}.")
            return []

        cursor = self.conn.cursor(dictionary=True) if self.db_type == 'mysql' else self.conn.cursor()
        try:
            cursor.execute(f"SELECT name, price, rating, link FROM {table_name}")
            if self.db_type == 'mysql':
                return cursor.fetchall()
            elif self.db_type == 'snowflake':
                columns = [col[0] for col in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except (MySQL_Error, snowflake.connector.errors.ProgrammingError) as e:
            print(f"Error fetching products from {table_name}: {e}")
            return []
        except Exception as e:
            print(f"An unexpected error occurred fetching products from {table_name}: {e}")
            return []
        finally:
            cursor.close()

    # --- Methods for scrape_urls table (remain largely the same, but use generic connect/close) ---
    def add_scrape_url(self, url, description=""):
        '''Adds a URL to the scrape_urls table.'''
        if not self.conn or not self.conn.is_connected():
            print("No active database connection to add URL.")
            return False

        cursor = self.conn.cursor()
        try:
            if self.db_type == 'mysql':
                cursor.execute("INSERT IGNORE INTO scrape_urls (url, description) VALUES (%s, %s)", (url, description))
            elif self.db_type == 'snowflake':
                cursor.execute("INSERT INTO scrape_urls (url, description) VALUES (%s, %s)", (url, description))
            self.conn.commit()
            if cursor.rowcount > 0:
                print(f"URL '{url}' added to scrape_urls.")
                return True
            else:
                # print(f"URL '{url}' already exists in scrape_urls.") # Can be noisy
                return False
        except (MySQL_Error, snowflake.connector.errors.ProgrammingError) as e:
            if (self.db_type == 'mysql' and e.errno == 1062) or \
               (self.db_type == 'snowflake' and "unique constraint" in str(e).lower()):
                # print(f"URL '{url}' already exists in scrape_urls (duplicate detected).") # Can be noisy
                return False
            else:
                print(f"Error adding URL '{url}': {e}")
                self.conn.rollback()
                return False
        except Exception as e:
            print(f"An unexpected error occurred adding URL '{url}': {e}")
            self.conn.rollback()
            return False
        finally:
            cursor.close()

    def get_all_scrape_urls(self):
        '''Fetches all URLs from the scrape_urls table.'''
        if not self.conn or not self.conn.is_connected():
            print("No active database connection to fetch URLs.")
            return []

        cursor = self.conn.cursor(dictionary=True) if self.db_type == 'mysql' else self.conn.cursor()
        try:
            cursor.execute("SELECT id, url, description, last_scraped_at FROM scrape_urls")
            if self.db_type == 'mysql':
                return cursor.fetchall()
            elif self.db_type == 'snowflake':
                columns = [col[0] for col in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except (MySQL_Error, snowflake.connector.errors.ProgrammingError) as e:
            print(f"Error fetching scrape URLs: {e}")
            return []
        except Exception as e:
            print(f"An unexpected error occurred fetching scrape URLs: {e}")
            return []
        finally:
            cursor.close()

    def delete_scrape_url(self, url_id):
        '''Deletes a URL from the scrape_urls table by ID.'''
        if not self.conn or not self.conn.is_connected():
            print("No active database connection to delete URL.")
            return False

        cursor = self.conn.cursor()
        try:
            if self.db_type == 'mysql':
                cursor.execute("DELETE FROM scrape_urls WHERE id = %s", (url_id,))
            elif self.db_type == 'snowflake':
                cursor.execute("DELETE FROM scrape_urls WHERE id = %s", (url_id,))
            self.conn.commit()
            if cursor.rowcount > 0:
                print(f"URL with ID {url_id} deleted from scrape_urls.")
                return True
            else:
                print(f"URL with ID {url_id} not found.")
                return False
        except (MySQL_Error, snowflake.connector.errors.ProgrammingError) as e:
            print(f"Error deleting URL with ID {url_id}: {e}")
            self.conn.rollback()
            return False
        except Exception as e:
            print(f"An unexpected error occurred deleting URL with ID {url_id}: {e}")
            self.conn.rollback()
            return False
        finally:
            cursor.close()

    def update_last_scraped_time(self, url_id):
        '''Updates the last_scraped_at timestamp for a given URL ID.'''
        if not self.conn or not self.conn.is_connected():
            print("No active database connection to update timestamp.")
            return False

        cursor = self.conn.cursor()
        try:
            current_timestamp = datetime.now()
            if self.db_type == 'mysql':
                cursor.execute(
                    "UPDATE scrape_urls SET last_scraped_at = %s WHERE id = %s",
                    (current_timestamp, url_id)
                )
            elif self.db_type == 'snowflake':
                cursor.execute(
                    "UPDATE scrape_urls SET last_scraped_at = %s WHERE id = %s",
                    (current_timestamp, url_id)
                )
            self.conn.commit()
            if cursor.rowcount > 0:
                return True
            else:
                print(f"URL with ID {url_id} not found for timestamp update.")
                return False
        except (MySQL_Error, snowflake.connector.errors.ProgrammingError) as e:
            print(f"Error updating last_scraped_at for URL ID {url_id}: {e}")
            self.conn.rollback()
            return False
        except Exception as e:
            print(f"An unexpected error occurred updating timestamp for URL ID {url_id}: {e}")
            self.conn.rollback()
            return False
        finally:
            cursor.close()

