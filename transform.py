import pandas as pd
import numpy as np
import os
from datetime import datetime

RAW_DATA_PATH = "data/raw/"
PROC_DATA_PATH = "data/processed/"


def transform(data):

    print("\n🔧 Running Transformation Step...")

    # Load extracted data
    customers = data.get("customers")
    products = data.get("products")
    stores = data.get("stores")
    promotions = data.get("promotions")
    sales_header = data.get("sales_header")
    sales_line_items = data.get("sales_line_items")
    loyalty_rules = data.get("loyalty_rules")
    rfm_rules = data.get("rfm_rules")

    # ========= DATA CLEANING ========= #

    # Rename inconsistent columns (Fixing schema mismatch)
    customers.rename(columns={"fist_name": "full_name",
                              "customer_phone": "phone"}, inplace=True)

    products.rename(columns={"current_stock_level": "current_stock"}, inplace=True)
    stores.rename(columns={"store_city": "city"}, inplace=True)

    promotions.rename(columns={"promotion_name": "rule_name",
                               "discount_percentage": "discount_percent"},
                      inplace=True)

    # Fix Date Columns
    date_fields = {
        "last_purchase_date": customers,
        "transaction_date": sales_header,
        "start_date": promotions,
        "end_date": promotions
    }

    for col, df in date_fields.items():
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Remove negative / wrong values
    products.loc[products["unit_price"] < 0, "unit_price"] = np.nan
    products.loc[products["current_stock"] < 0, "current_stock"] = 0

    sales_header = sales_header[sales_header["total_amount"] >= 0]
    sales_line_items = sales_line_items[(sales_line_items["quantity"] > 0)]

    # Remove invalid product references
    sales_line_items = sales_line_items[
        sales_line_items["product_id"].isin(products["product_id"])
    ]

    # Remove orphan transactions
    sales_line_items = sales_line_items[
        sales_line_items["transaction_id"].isin(sales_header["transaction_id"])
    ]

    # ========= FACT TABLE CREATION ========= #
    fact_sales = sales_line_items.merge(
        sales_header, on="transaction_id", how="inner"
    ).merge(products, on="product_id", how="left"
    ).merge(promotions, on="promotion_id", how="left")

    # ========= DYNAMIC LOYALTY ENGINE ========= #

    # Ensure numeric
    sales_header["total_amount"] = pd.to_numeric(sales_header["total_amount"], errors="coerce")

    # Initialize
    sales_header["earned_points"] = 0

    # Apply each rule dynamically
    for _, rule in loyalty_rules[loyalty_rules["is_active"] == "TRUE"].iterrows():
        condition = sales_header["total_amount"] >= rule["min_spend_threshold"]
        sales_header.loc[condition, "earned_points"] += (
            (sales_header.loc[condition, "total_amount"] /
             rule["min_spend_threshold"]).astype(int)
            * rule["points_per_unit_spend"]
            + rule["bonus_points"]
        )

    # Sum points by customer
    points = sales_header.groupby("customer_id")["earned_points"].sum().reset_index()
    customers = customers.merge(points, on="customer_id", how="left")
    customers["earned_points"].fillna(0, inplace=True)
    customers["total_loyalty_points"] = customers["earned_points"]

    # ========= RFM ANALYTICS ========= #
    snapshot_date = sales_header["transaction_date"].max() + pd.Timedelta(days=1)

    rfm = sales_header.groupby("customer_id").agg({
        "transaction_date": lambda x: (snapshot_date - x.max()).days,
        "transaction_id": "count",
        "total_amount": "sum"
    }).reset_index()

    rfm.columns = ["customer_id", "recency", "frequency", "monetary"]

    # Score using binning
    rfm["R"] = pd.qcut(rfm["recency"], 3, labels=[3,2,1])
    rfm["F"] = pd.qcut(rfm["frequency"].rank(method="first"),3,labels=[1,2,3])
    rfm["M"] = pd.qcut(rfm["monetary"],3,labels=[1,2,3])
    rfm["RFM_Score"] = rfm[["R","F","M"]].sum(axis=1).astype(int)

    # ========= DYNAMIC SEGMENTATION (from rules table) ========= #
    def segment_customer(score):
        rule = rfm_rules[
            (rfm_rules["rfm_score_min"] <= score) &
            (rfm_rules["rfm_score_max"] >= score)
        ]
        if not rule.empty:
            return rule["segment_name"].values[0]
        return "Unclassified"

    rfm["segment"] = rfm["RFM_Score"].apply(segment_customer)

    customer_loyalty_segments = customers.merge(rfm, on="customer_id", how="left")

    # ========= SAVE OUTPUT ========= #
    os.makedirs(PROC_DATA_PATH, exist_ok=True)

    customers.to_csv(PROC_DATA_PATH + "customers_cleaned.csv", index=False)
    products.to_csv(PROC_DATA_PATH + "products_cleaned.csv", index=False)
    stores.to_csv(PROC_DATA_PATH + "stores_cleaned.csv", index=False)
    promotions.to_csv(PROC_DATA_PATH + "promotions_cleaned.csv", index=False)
    fact_sales.to_csv(PROC_DATA_PATH + "fact_sales.csv", index=False)
    customer_loyalty_segments.to_csv(
        PROC_DATA_PATH + "customer_loyalty_segments.csv", index=False
    )

    print("✔ Transform Stage Completed Successfully!")
    return {
        "customers": customers,
        "products": products,
        "stores": stores,
        "promotions": promotions,
        "fact_sales": fact_sales,
        "customer_loyalty_segments": customer_loyalty_segments
    }


if __name__ == "__main__":
    print("⚠️ Run transform via run_etl.py after extract completes.")
