Retail Loyalty Project (Python + Pandas)

This project calculates loyalty points, updates customer information, builds RFM metrics, and creates customer segments using simple Python and pandas. It is designed for retail data and easy for beginners to understand.

Project Structure
data/           → input CSV files  
scripts/        → Python script  
output/         → generated result files  
README.md       
.gitignore

Input Files (inside data/)

customers_2000.csv

sales_header.csv

sales_line_items.csv

loyalty_rules.csv

Optional:

promotions.csv

products.csv

Output Files (inside output/)

transaction_points.csv

customers_updated.csv

rfm_metrics.csv

customers_segments.csv

What This Project Does

Reads all input CSV files using pandas.

Calculates loyalty points from loyalty_rules.csv.

Updates each customer's total loyalty points.

Creates RFM values:

Recency

Frequency

Monetary

Segments customers into:

High-Spenders (top 10% monetary)

At-Risk (>30 days no purchase but has points)

How to Run

Install pandas:

pip install pandas


Make sure the folders data/, scripts/, and output/ exist.

Run the Python script:

python scripts/loyalty_pipeline.py


Check the output folder for all generated CSV files.

Why This Project Is Useful

Shows a real-world retail customer analysis workflow.

Uses core analytics concepts like loyalty, customer value, and segmentation.

Very simple Python and pandas code, easy to learn and modify.

Great for GitHub, resume, and interview discussions.

Future Improvements

Add a Power BI dashboard.

Add product-level analytics.

Add promotion rule logic.

Add cohort analysis.
