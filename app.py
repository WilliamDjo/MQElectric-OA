from flask import Flask, request, render_template, flash, redirect, url_for, jsonify
import pandas as pd
import os
from werkzeug.utils import secure_filename
import sqlite3
from datetime import datetime
import re

app = Flask(__name__)
app.config["SECRET_KEY"] = "your-secret-key-here"
app.config["UPLOAD_FOLDER"] = "uploads"
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB max file size

# Create uploads directory if it doesn't exist
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)


# Initialize SQLite database
def init_db():
    conn = sqlite3.connect("upload_logs.db")
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS upload_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            upload_timestamp TEXT NOT NULL,
            filename TEXT NOT NULL,
            transactions_count INTEGER,
            customers_count INTEGER,
            products_count INTEGER,
            file_path TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ["xlsx"]


def parse_customer_data(raw_data):
    """Parse the malformed customer data from the single column format"""
    customers = []

    for row in raw_data:
        if pd.isna(row) or not isinstance(row, str):
            continue

        # Remove curly braces and split by underscore
        cleaned = row.strip("{}")
        parts = cleaned.split("_")

        if len(parts) >= 6:
            try:
                customer_data = {
                    "customer_id": parts[0],
                    "name": parts[1],
                    "email": parts[2],
                    "dob": parts[3],
                    "address": parts[4],
                    "created_date": float(parts[5]),  # Excel date serial number
                }
                customers.append(customer_data)
            except (ValueError, IndexError) as e:
                print(f"Error parsing customer row: {row}, Error: {e}")
                continue

    return pd.DataFrame(customers)


def validate_excel_structure(file_path):
    """Validate that the Excel file has the correct structure"""
    try:
        # Read all sheets
        xl_file = pd.ExcelFile(file_path)
        required_sheets = ["Transactions", "Customers", "Products"]

        # Check if all required sheets exist
        missing_sheets = [
            sheet for sheet in required_sheets if sheet not in xl_file.sheet_names
        ]
        if missing_sheets:
            return False, f"Missing required sheets: {', '.join(missing_sheets)}"

        # Validate Transactions sheet
        transactions_df = pd.read_excel(file_path, sheet_name="Transactions")
        required_transaction_cols = [
            "transaction_id",
            "customer_id",
            "transaction_date",
            "product_code",
            "amount",
            "payment_type",
        ]
        missing_trans_cols = [
            col
            for col in required_transaction_cols
            if col not in transactions_df.columns
        ]
        if missing_trans_cols:
            return (
                False,
                f"Transactions sheet missing columns: {', '.join(missing_trans_cols)}",
            )

        # Validate Products sheet
        products_df = pd.read_excel(file_path, sheet_name="Products")
        required_product_cols = [
            "product_code",
            "product_name",
            "category",
            "unit_price",
        ]
        missing_prod_cols = [
            col for col in required_product_cols if col not in products_df.columns
        ]
        if missing_prod_cols:
            return (
                False,
                f"Products sheet missing columns: {', '.join(missing_prod_cols)}",
            )

        # For Customers sheet, we'll handle the special parsing
        customers_raw = pd.read_excel(file_path, sheet_name="Customers")

        # Check if customers data can be parsed
        if customers_raw.empty:
            return False, "Customers sheet is empty"

        # Try to parse customer data
        customers_df = parse_customer_data(customers_raw.iloc[:, 0])
        if customers_df.empty:
            return False, "Could not parse customer data from the provided format"

        # Basic data validation
        if len(transactions_df) == 0:
            return False, "Transactions sheet has no data"
        if len(products_df) == 0:
            return False, "Products sheet has no data"
        if len(customers_df) == 0:
            return False, "Customers sheet has no valid data"

        # Check for required data types
        if not pd.api.types.is_numeric_dtype(transactions_df["amount"]):
            return False, "Transaction amount column must be numeric"

        if not pd.api.types.is_numeric_dtype(products_df["unit_price"]):
            return False, "Product unit_price column must be numeric"

        return True, {
            "transactions_count": len(transactions_df),
            "customers_count": len(customers_df),
            "products_count": len(products_df),
            "transactions_sample": transactions_df.head(3).to_dict("records"),
            "customers_sample": customers_df.head(3).to_dict("records"),
            "products_sample": products_df.head(3).to_dict("records"),
        }

    except Exception as e:
        return False, f"Error reading file: {str(e)}"


def log_upload(filename, file_path, validation_result):
    """Log upload details to SQLite database"""
    conn = sqlite3.connect("upload_logs.db")
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO upload_logs 
        (upload_timestamp, filename, transactions_count, customers_count, products_count, file_path)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        (
            datetime.now().isoformat(),
            filename,
            validation_result["transactions_count"],
            validation_result["customers_count"],
            validation_result["products_count"],
            file_path,
        ),
    )

    conn.commit()
    conn.close()
