import pandas as pd
import numpy as np
from datetime import datetime
import re

# from app import parse_customer_data --> can't do this due to circular import


def convert_excel_date(excel_date):
    """Convert Excel date serial number to datetime"""
    try:
        if pd.isna(excel_date):
            return None
        # Excel epoch starts from 1900-01-01, but Excel incorrectly treats 1900 as a leap year
        # So we need to adjust by subtracting 2 days
        if isinstance(excel_date, (int, float)):
            return pd.to_datetime("1899-12-30") + pd.Timedelta(days=excel_date)
        return pd.to_datetime(excel_date)
    except:
        return None


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


def process_data(file_path):
    """
    Main function to process Excel data and perform all required analyses
    Returns a dictionary with all processed results
    """

    # Read all sheets
    transactions_df = pd.read_excel(file_path, sheet_name="Transactions")
    products_df = pd.read_excel(file_path, sheet_name="Products")
    customers_raw = pd.read_excel(file_path, sheet_name="Customers")

    # Parse customers data from the special format
    customers_df = parse_customer_data(customers_raw.iloc[:, 0])

    # Convert date columns
    transactions_df["transaction_date"] = pd.to_datetime(
        transactions_df["transaction_date"], origin="1899-12-30", unit="D"
    )
    customers_df["created_date"] = customers_df["created_date"].apply(
        convert_excel_date
    )
    customers_df["dob"] = pd.to_datetime(customers_df["dob"])

    # Ensure amount is numeric
    transactions_df["amount"] = pd.to_numeric(
        transactions_df["amount"], errors="coerce"
    )

    # Step 3a: Detect customer address changes and keep history
    address_history = detect_address_changes(customers_df, transactions_df)

    # Step 3b: Calculate total transaction amount for each customer by product category
    customer_category_totals = calculate_customer_category_totals(
        transactions_df, products_df
    )

    # Step 3c: Identify top spender in each category
    top_spenders_by_category = identify_top_spenders_by_category(
        customer_category_totals
    )

    # Step 3d: Rank all customers by total purchase value
    customer_rankings = rank_customers_by_total_value(transactions_df)

    return {
        "processed_data": {
            "transactions_df": transactions_df,
            "customers_df": customers_df,
            "products_df": products_df,
        },
        "analysis_results": {
            "address_history": address_history,
            "customer_category_totals": customer_category_totals,
            "top_spenders_by_category": top_spenders_by_category,
            "customer_rankings": customer_rankings,
        },
        "summary_stats": {
            "total_customers": len(customers_df),
            "total_transactions": len(transactions_df),
            "total_revenue": transactions_df["amount"].sum(),
            "date_range": {
                "first_transaction": transactions_df["transaction_date"].min(),
                "last_transaction": transactions_df["transaction_date"].max(),
            },
            "customers_with_address_changes": len(address_history),
            "product_categories": products_df["category"].nunique(),
        },
    }


def detect_address_changes(customers_df, transactions_df):
    """
    Detect changes in customer addresses over time and keep a history

    Since we only have one address per customer in this dataset, we'll simulate
    address change detection by looking at the relationship between customer
    creation dates and transaction dates to identify potential address updates.
    """

    # Get customer transaction timeline
    customer_transactions = (
        transactions_df.groupby("customer_id")
        .agg({"transaction_date": ["min", "max", "count"]})
        .round(2)
    )
    customer_transactions.columns = [
        "first_transaction",
        "last_transaction",
        "total_transactions",
    ]
    customer_transactions = customer_transactions.reset_index()

    # Merge with customer data
    customer_timeline = customers_df.merge(
        customer_transactions, on="customer_id", how="left"
    )

    # Create address history structure
    address_history = []

    for _, customer in customer_timeline.iterrows():
        # For this dataset, we'll create a single address record per customer
        # In a real scenario, you'd have multiple address records over time
        address_record = {
            "customer_id": customer["customer_id"],
            "address": customer["address"],
            "effective_from": customer["created_date"],
            "effective_to": (
                customer["last_transaction"]
                if pd.notna(customer["last_transaction"])
                else customer["created_date"]
            ),
            "is_current": True,
            "change_detected": False,  # No changes in this dataset
            "days_active": (
                (customer["last_transaction"] - customer["created_date"]).days
                if pd.notna(customer["last_transaction"])
                else 0
            ),
        }
        address_history.append(address_record)

    address_history_df = pd.DataFrame(address_history)

    # Identify customers who might have address changes (heuristic)
    # Customers with very long transaction periods might have moved
    long_term_customers = address_history_df[address_history_df["days_active"] > 365]

    return {
        "address_history_df": address_history_df,
        "summary": {
            "total_customers_tracked": len(address_history_df),
            "customers_with_potential_changes": len(long_term_customers),
            "average_days_at_address": address_history_df["days_active"].mean(),
            "long_term_customers": long_term_customers[
                ["customer_id", "days_active"]
            ].to_dict("records"),
        },
    }


def calculate_customer_category_totals(transactions_df, products_df):
    """
    Calculate total transaction amount of each customer for each product category
    """

    # Merge transactions with products to get categories
    transactions_with_categories = transactions_df.merge(
        products_df[["product_code", "category"]], on="product_code", how="left"
    )

    # Group by customer and category, sum amounts
    customer_category_totals = (
        transactions_with_categories.groupby(["customer_id", "category"])["amount"]
        .sum()
        .reset_index()
    )

    # Pivot to get categories as columns
    customer_category_pivot = customer_category_totals.pivot(
        index="customer_id", columns="category", values="amount"
    ).fillna(0)

    # Add total column
    customer_category_pivot["Total_Spending"] = customer_category_pivot.sum(axis=1)

    # Reset index to make customer_id a column
    customer_category_pivot = customer_category_pivot.reset_index()

    # Create summary statistics
    category_summary = {}
    for category in customer_category_pivot.columns[
        1:-1
    ]:  # Exclude customer_id and Total_Spending
        category_summary[category] = {
            "total_revenue": customer_category_pivot[category].sum(),
            "customers_purchased": (customer_category_pivot[category] > 0).sum(),
            "average_spending": customer_category_pivot[category][
                customer_category_pivot[category] > 0
            ].mean(),
            "max_spending": customer_category_pivot[category].max(),
            "min_spending": customer_category_pivot[category][
                customer_category_pivot[category] > 0
            ].min(),
        }

    return {
        "customer_category_totals": customer_category_pivot,
        "category_summary": category_summary,
        "top_customers_per_category": get_top_customers_per_category(
            customer_category_pivot
        ),
    }


def get_top_customers_per_category(customer_category_pivot):
    """Get top 5 customers for each category"""
    top_customers = {}

    for category in customer_category_pivot.columns[
        1:-1
    ]:  # Exclude customer_id and Total_Spending
        top_5 = customer_category_pivot.nlargest(5, category)[["customer_id", category]]
        top_5 = top_5[
            top_5[category] > 0
        ]  # Only include customers who actually bought in this category
        top_customers[category] = top_5.to_dict("records")

    return top_customers


def identify_top_spenders_by_category(customer_category_data):
    """
    Identify the top spender in each category
    """
    customer_category_totals = customer_category_data["customer_category_totals"]
    top_spenders = {}

    for category in customer_category_totals.columns[
        1:-1
    ]:  # Exclude customer_id and Total_Spending
        if customer_category_totals[category].max() > 0:
            top_spender_idx = customer_category_totals[category].idxmax()
            top_spender = customer_category_totals.loc[top_spender_idx]

            top_spenders[category] = {
                "customer_id": top_spender["customer_id"],
                "amount_spent": top_spender[category],
                "total_spending_all_categories": top_spender["Total_Spending"],
                "percentage_of_category": (
                    top_spender[category] / customer_category_totals[category].sum()
                )
                * 100,
            }

    return top_spenders


def rank_customers_by_total_value(transactions_df):
    """
    Rank all customers based on their total purchase value across all products
    """

    # Calculate total spending per customer
    customer_totals = (
        transactions_df.groupby("customer_id")
        .agg({"amount": ["sum", "count", "mean"], "transaction_date": ["min", "max"]})
        .round(2)
    )

    # Flatten column names
    customer_totals.columns = [
        "total_spent",
        "transaction_count",
        "avg_transaction",
        "first_purchase",
        "last_purchase",
    ]
    customer_totals = customer_totals.reset_index()

    # Calculate customer lifetime (days between first and last purchase)
    customer_totals["customer_lifetime_days"] = (
        customer_totals["last_purchase"] - customer_totals["first_purchase"]
    ).dt.days

    # Calculate average spending per day for active customers
    customer_totals["avg_spending_per_day"] = customer_totals["total_spent"] / (
        customer_totals["customer_lifetime_days"] + 1
    )

    # Rank customers by total spending
    customer_totals["rank_by_total_spending"] = customer_totals["total_spent"].rank(
        method="dense", ascending=False
    )

    # Rank by transaction frequency
    customer_totals["rank_by_frequency"] = customer_totals["transaction_count"].rank(
        method="dense", ascending=False
    )

    # Rank by average transaction value
    customer_totals["rank_by_avg_transaction"] = customer_totals[
        "avg_transaction"
    ].rank(method="dense", ascending=False)

    # Create customer segments based on spending
    customer_totals["customer_segment"] = pd.cut(
        customer_totals["total_spent"],
        bins=[
            0,
            customer_totals["total_spent"].quantile(0.5),
            customer_totals["total_spent"].quantile(0.8),
            customer_totals["total_spent"].quantile(0.95),
            customer_totals["total_spent"].max(),
        ],
        labels=["Low Value", "Medium Value", "High Value", "VIP"],
        include_lowest=True,
    )

    # Sort by total spending (highest first)
    customer_rankings = customer_totals.sort_values(
        "total_spent", ascending=False
    ).reset_index(drop=True)

    # Add percentile ranks
    customer_rankings["spending_percentile"] = (
        customer_rankings["total_spent"].rank(pct=True) * 100
    )

    return {
        "customer_rankings": customer_rankings,
        "summary_stats": {
            "total_customers": len(customer_rankings),
            "total_revenue": customer_rankings["total_spent"].sum(),
            "average_customer_value": customer_rankings["total_spent"].mean(),
            "median_customer_value": customer_rankings["total_spent"].median(),
            "top_10_percent_revenue_share": customer_rankings.head(
                int(len(customer_rankings) * 0.1)
            )["total_spent"].sum()
            / customer_rankings["total_spent"].sum()
            * 100,
            "customer_segments": customer_rankings["customer_segment"]
            .value_counts()
            .to_dict(),
        },
        "top_10_customers": customer_rankings.head(10)[
            ["customer_id", "total_spent", "transaction_count", "customer_segment"]
        ].to_dict("records"),
        "bottom_10_customers": customer_rankings.tail(10)[
            ["customer_id", "total_spent", "transaction_count", "customer_segment"]
        ].to_dict("records"),
    }


# Additional utility functions for analysis


def generate_insights(processed_results):
    """Generate business insights from the processed data"""

    analysis = processed_results["analysis_results"]
    summary = processed_results["summary_stats"]

    insights = {
        "revenue_insights": {
            "total_revenue": summary["total_revenue"],
            "average_transaction_value": summary["total_revenue"]
            / summary["total_transactions"],
            "revenue_per_customer": summary["total_revenue"]
            / summary["total_customers"],
        },
        "customer_insights": {
            "most_valuable_segment": analysis["customer_rankings"]["summary_stats"][
                "customer_segments"
            ],
            "top_spenders_by_category": analysis["top_spenders_by_category"],
            "customer_retention_period": analysis["address_history"]["summary"][
                "average_days_at_address"
            ],
        },
        "product_insights": {
            "category_performance": analysis["customer_category_totals"][
                "category_summary"
            ]
        },
        "recommendations": generate_recommendations(analysis),
    }

    return insights


def generate_recommendations(analysis_results):
    """Generate actionable business recommendations"""

    recommendations = []

    # Top spender recommendations
    top_spenders = analysis_results["top_spenders_by_category"]
    for category, spender_info in top_spenders.items():
        if spender_info["percentage_of_category"] > 20:
            recommendations.append(
                {
                    "type": "Customer Retention",
                    "priority": "High",
                    "category": category,
                    "recommendation": f"Focus on retaining {spender_info['customer_id']} - they represent {spender_info['percentage_of_category']:.1f}% of {category} revenue",
                    "potential_impact": "Revenue Protection",
                }
            )

    # Category performance recommendations
    category_summary = analysis_results["customer_category_totals"]["category_summary"]
    categories_by_revenue = sorted(
        category_summary.items(), key=lambda x: x[1]["total_revenue"], reverse=True
    )

    if len(categories_by_revenue) > 1:
        top_category = categories_by_revenue[0]
        recommendations.append(
            {
                "type": "Product Strategy",
                "priority": "Medium",
                "category": top_category[0],
                "recommendation": f"Expand {top_category[0]} product line - highest revenue category (${top_category[1]['total_revenue']:.2f})",
                "potential_impact": "Revenue Growth",
            }
        )

    # Customer segmentation recommendations
    customer_segments = analysis_results["customer_rankings"]["summary_stats"][
        "customer_segments"
    ]
    if customer_segments.get("Low Value", 0) > customer_segments.get("High Value", 0):
        recommendations.append(
            {
                "type": "Customer Development",
                "priority": "Medium",
                "category": "All",
                "recommendation": "Implement customer development program to move Low Value customers to higher segments",
                "potential_impact": "Customer Lifetime Value Increase",
            }
        )

    return recommendations
