import os
import pandas as pd

RAW_DATA_PATH = "data/raw/"

# Define required filenames and expected columns (schema validation)
TABLE_SCHEMAS = {
    "customers.csv": [
        "customer_id", "fist_name", "email", "loyalty_status",
        "total_loyalty_points", "last_purchase_date",
        "segment_id", "customer_phone", "customer_since"
    ],
    "products.csv": [
        "product_id", "product_name", "product_category",
        "unit_price", "current_stock_level"
    ],
    "stores.csv": [
        "store_id", "store_name", "store_city", "store_region", "opening_date"
    ],
    "promotions.csv": [
        "promotion_id", "promotion_name", "discount_percentage",
        "applicable_category", "start_date", "end_date"
    ],
    "sales_header.csv": [
        "transaction_id", "customer_id", "store_id",
        "transaction_date", "total_amount", "customer_phone"
    ],
    "sales_line_items.csv": [
        "line_item_id", "transaction_id", "product_id",
        "promotion_id", "quantity", "line_item_amount"
    ],
    "loyalty_rules.csv": [
        "rule_id", "rule_name", "points_per_unit_spend",
        "min_spend_threshold", "bonus_points", "is_active"
    ],
    "rfm_rules.csv": [
        "segment_name", "rfm_score_min", "rfm_score_max"
    ]
}


def validate_columns(df, expected_columns, filename):
    """Check if CSV has correct columns"""
    missing_cols = set(expected_columns) - set(df.columns)
    extra_cols = set(df.columns) - set(expected_columns)

    if missing_cols:
        print(f"⚠️ Missing columns in {filename}: {missing_cols}")
    if extra_cols:
        print(f"⚠️ Extra columns in {filename}: {extra_cols}")

    return df


def extract_raw_data():
    """Extract and validate all raw tables from CSV files"""

    dataframes = {}

    for filename, expected_cols in TABLE_SCHEMAS.items():
        file_path = os.path.join(RAW_DATA_PATH, filename)

        if not os.path.exists(file_path):
            print(f"❌ File not found: {filename}")
            continue

        print(f"📥 Loading: {filename}")
        df = pd.read_csv(file_path)
        df = validate_columns(df, expected_cols, filename)

        dataframes[filename.replace(".csv", "")] = df

    print("✅ Extract stage completed successfully!")
    return dataframes


if __name__ == "__main__":
    extracted_data = extract_raw_data()
    for name, df in extracted_data.items():
        print(f"{name}: {df.shape} rows")
