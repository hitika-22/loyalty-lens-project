import pandas as pd
import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_connection_string, get_connection_string_without_db, DB_CONFIG, PROCESSED_DATA_PATH


def create_database_if_not_exists():
    """Create the database if it doesn't exist"""
    try:
        # Connect without specifying database
        engine = create_engine(get_connection_string_without_db())
        with engine.connect() as conn:
            # Create database if it doesn't exist
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}"))
            conn.commit()
            print(f"✅ Database '{DB_CONFIG['database']}' is ready")
        engine.dispose()
        return True
    except SQLAlchemyError as e:
        print(f"❌ Error creating database: {e}")
        return False


def create_tables(engine):
    """Create all necessary tables with proper schema"""
    try:
        with engine.connect() as conn:
            # Drop existing tables (fresh load) - in reverse order due to foreign keys
            tables = [
                "fact_sales", "dim_promotion", "dim_store", 
                "dim_product", "dim_customer", "dim_loyalty_rules", "dim_rfm_rules"
            ]
            
            print("\n🗑️  Dropping existing tables...")
            for tbl in tables:
                conn.execute(text(f"DROP TABLE IF EXISTS {tbl}"))
            
            print("🏗️  Creating dimension and fact tables...")
            
            # Create dimension tables first (no foreign keys)
            conn.execute(text("""
                CREATE TABLE dim_customer (
                    customer_id VARCHAR(10) PRIMARY KEY,
                    full_name VARCHAR(255),
                    email VARCHAR(255),
                    loyalty_status VARCHAR(50),
                    total_loyalty_points INT,
                    last_purchase_date DATE,
                    phone VARCHAR(50),
                    customer_since DATE,
                    segment_id VARCHAR(10),
                    earned_points FLOAT
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE dim_product (
                    product_id VARCHAR(10) PRIMARY KEY,
                    product_name VARCHAR(255),
                    product_category VARCHAR(100),
                    unit_price DECIMAL(10,2),
                    current_stock INT
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE dim_store (
                    store_id VARCHAR(10) PRIMARY KEY,
                    store_name VARCHAR(255),
                    city VARCHAR(100),
                    store_region VARCHAR(50),
                    opening_date DATE
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE dim_promotion (
                    promotion_id VARCHAR(10) PRIMARY KEY,
                    rule_name VARCHAR(255),
                    discount_percent INT,
                    applicable_category VARCHAR(100),
                    start_date DATE,
                    end_date DATE
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE dim_loyalty_rules (
                    rule_id INT PRIMARY KEY,
                    rule_name VARCHAR(255),
                    points_per_unit_spend INT,
                    min_spend_threshold INT,
                    bonus_points INT,
                    is_active VARCHAR(10)
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE dim_rfm_rules (
                    segment_name VARCHAR(100) PRIMARY KEY,
                    rfm_score_min INT,
                    rfm_score_max INT
                )
            """))
            
            # Create fact table with foreign keys
            conn.execute(text("""
                CREATE TABLE fact_sales (
                    line_item_id VARCHAR(20) PRIMARY KEY,
                    transaction_id VARCHAR(20),
                    customer_id VARCHAR(10),
                    product_id VARCHAR(10),
                    promotion_id VARCHAR(10),
                    store_id VARCHAR(10),
                    quantity INT,
                    total_amount DECIMAL(10,2),
                    line_item_amount DECIMAL(10,2),
                    transaction_date DATE,
                    product_name VARCHAR(255),
                    product_category VARCHAR(100),
                    unit_price DECIMAL(10,2),
                    current_stock INT,
                    rule_name VARCHAR(255),
                    discount_percent INT,
                    applicable_category VARCHAR(100),
                    start_date DATE,
                    end_date DATE,
                    customer_phone VARCHAR(50),
                    INDEX idx_customer (customer_id),
                    INDEX idx_product (product_id),
                    INDEX idx_store (store_id),
                    INDEX idx_transaction (transaction_id),
                    INDEX idx_date (transaction_date)
                )
            """))
            
            conn.commit()
            print("✅ All tables created successfully")
            return True
            
    except SQLAlchemyError as e:
        print(f"❌ Error creating tables: {e}")
        return False


def load_data_to_mysql(engine):
    """Load transformed data into MySQL tables"""
    try:
        print("\n📥 Loading data into MySQL tables...")
        
        # Load dataframes from processed CSV files
        data_files = {
            "dim_customer": "customers_cleaned.csv",
            "dim_product": "products_cleaned.csv",
            "dim_store": "stores_cleaned.csv",
            "dim_promotion": "promotions_cleaned.csv",
            "fact_sales": "fact_sales.csv"
        }
        
        # Load reference data from raw files
        raw_files = {
            "dim_loyalty_rules": "data/raw/loyalty_rules.csv",
            "dim_rfm_rules": "data/raw/rfm_rules.csv"
        }
        
        # Load and insert dimension tables first
        for table_name, filename in data_files.items():
            file_path = os.path.join(PROCESSED_DATA_PATH, filename)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                
                # Handle NaN values and data types
                df = df.where(pd.notnull(df), None)
                
                # Insert data
                rows_inserted = df.to_sql(
                    table_name, 
                    engine, 
                    if_exists='append', 
                    index=False,
                    chunksize=1000
                )
                print(f"   ✓ {table_name}: {len(df)} rows inserted")
            else:
                print(f"   ⚠️  Warning: {file_path} not found")
        
        # Load reference tables
        for table_name, filename in raw_files.items():
            if os.path.exists(filename):
                df = pd.read_csv(filename)
                df = df.where(pd.notnull(df), None)
                
                rows_inserted = df.to_sql(
                    table_name, 
                    engine, 
                    if_exists='append', 
                    index=False,
                    chunksize=1000
                )
                print(f"   ✓ {table_name}: {len(df)} rows inserted")
            else:
                print(f"   ⚠️  Warning: {filename} not found")
        
        print("\n✅ All data loaded successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return False


def verify_data_load(engine):
    """Verify that data was loaded correctly"""
    try:
        print("\n🔍 Verifying data load...")
        with engine.connect() as conn:
            tables = [
                "dim_customer", "dim_product", "dim_store", 
                "dim_promotion", "dim_loyalty_rules", "dim_rfm_rules", "fact_sales"
            ]
            
            for table in tables:
                result = conn.execute(text(f"SELECT COUNT(*) as count FROM {table}"))
                count = result.fetchone()[0]
                print(f"   {table}: {count} rows")
        
        return True
    except SQLAlchemyError as e:
        print(f"❌ Error verifying data: {e}")
        return False


def load_to_db():
    """Main function to load data into MySQL database"""
    print("\n" + "="*60)
    print("📦 LOAD STAGE: Loading Data to MySQL Database")
    print("="*60)
    
    try:
        # Step 1: Create database if it doesn't exist
        if not create_database_if_not_exists():
            return False
        
        # Step 2: Create SQLAlchemy engine with database
        print(f"\n🔌 Connecting to MySQL database: {DB_CONFIG['database']}")
        engine = create_engine(get_connection_string(), echo=False)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT VERSION()"))
            version = result.fetchone()[0]
            print(f"✅ Connected to MySQL version: {version}")
        
        # Step 3: Create tables
        if not create_tables(engine):
            return False
        
        # Step 4: Load data
        if not load_data_to_mysql(engine):
            return False
        
        # Step 5: Verify data load
        verify_data_load(engine)
        
        # Close engine
        engine.dispose()
        
        print("\n" + "="*60)
        print("🎯 LOAD STAGE COMPLETED SUCCESSFULLY!")
        print(f"📌 Database: {DB_CONFIG['database']} @ {DB_CONFIG['host']}")
        print("="*60)
        
        return True
        
    except SQLAlchemyError as e:
        print(f"\n❌ Database Error: {e}")
        return False
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        return False


if __name__ == "__main__":
    load_to_db()
