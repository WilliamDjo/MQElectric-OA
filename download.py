import pandas as pd
import os
from datetime import datetime
from flask import Response, send_file, flash, redirect, url_for
import tempfile
import zipfile
from io import BytesIO


def create_processed_excel_file(processing_result, original_filename):
    """
    Create a comprehensive Excel file with multiple sheets containing processed data
    """

    # Create a temporary file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    processed_filename = f"processed_{timestamp}_{original_filename}"
    temp_file_path = os.path.join(tempfile.gettempdir(), processed_filename)

    # Get processed data
    transactions_df = processing_result["processed_data"]["transactions_df"]
    customers_df = processing_result["processed_data"]["customers_df"]
    products_df = processing_result["processed_data"]["products_df"]

    # Create Excel writer
    with pd.ExcelWriter(temp_file_path, engine="openpyxl") as writer:

        # Sheet 1: Original Data (cleaned)
        transactions_df.to_excel(writer, sheet_name="Transactions_Cleaned", index=False)
        customers_df.to_excel(writer, sheet_name="Customers_Enhanced", index=False)
        products_df.to_excel(writer, sheet_name="Products", index=False)

        # Sheet 2: Customer Rankings
        customer_rankings_df = pd.DataFrame(
            processing_result["analysis_results"]["customer_rankings"][
                "customer_rankings"
            ]
        )
        customer_rankings_df.to_excel(
            writer, sheet_name="Customer_Rankings", index=False
        )

        # Sheet 3: Customer Category Totals
        category_totals_df = processing_result["analysis_results"][
            "customer_category_totals"
        ]["customer_category_totals"]
        category_totals_df.to_excel(
            writer, sheet_name="Customer_Category_Spending", index=False
        )

        # Sheet 4: Top Spenders by Category
        top_spenders_data = []
        for category, spender_info in processing_result["analysis_results"][
            "top_spenders_by_category"
        ].items():
            top_spenders_data.append(
                {
                    "Category": category,
                    "Top_Customer_ID": spender_info["customer_id"],
                    "Amount_Spent": spender_info["amount_spent"],
                    "Percentage_of_Category": spender_info["percentage_of_category"],
                    "Total_Customer_Spending": spender_info[
                        "total_spending_all_categories"
                    ],
                }
            )

        top_spenders_df = pd.DataFrame(top_spenders_data)
        top_spenders_df.to_excel(
            writer, sheet_name="Top_Spenders_by_Category", index=False
        )

        # Sheet 5: Address History
        if (
            "address_history_df"
            in processing_result["analysis_results"]["address_history"]
        ):
            address_history_df = processing_result["analysis_results"][
                "address_history"
            ]["address_history_df"]
            address_history_df.to_excel(
                writer, sheet_name="Address_History", index=False
            )

        # Sheet 6: Category Performance Summary
        category_summary_data = []
        for category, stats in processing_result["analysis_results"][
            "customer_category_totals"
        ]["category_summary"].items():
            category_summary_data.append(
                {
                    "Category": category,
                    "Total_Revenue": stats["total_revenue"],
                    "Customers_Purchased": stats["customers_purchased"],
                    "Average_Spending": stats["average_spending"],
                    "Max_Spending": stats["max_spending"],
                    "Min_Spending": stats["min_spending"],
                }
            )

        category_summary_df = pd.DataFrame(category_summary_data)
        category_summary_df.to_excel(
            writer, sheet_name="Category_Performance", index=False
        )

        # Sheet 7: Summary Statistics
        summary_data = {
            "Metric": [
                "Total Customers",
                "Total Transactions",
                "Total Revenue",
                "Average Transaction Value",
                "Average Customer Value",
                "Product Categories",
                "First Transaction Date",
                "Last Transaction Date",
                "Geocoded Customers",
                "Geocoding Success Rate",
            ],
            "Value": [
                processing_result["summary_stats"]["total_customers"],
                processing_result["summary_stats"]["total_transactions"],
                processing_result["summary_stats"]["total_revenue"],
                processing_result["summary_stats"]["total_revenue"]
                / processing_result["summary_stats"]["total_transactions"],
                processing_result["summary_stats"]["total_revenue"]
                / processing_result["summary_stats"]["total_customers"],
                processing_result["summary_stats"]["product_categories"],
                processing_result["summary_stats"]["date_range"]["first_transaction"],
                processing_result["summary_stats"]["date_range"]["last_transaction"],
                processing_result["summary_stats"].get("geocoded_customers", 0),
                f"{processing_result['summary_stats'].get('geocoding_success_rate', 0):.1f}%",
            ],
        }

        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name="Summary_Statistics", index=False)

        # Sheet 8: Business Insights (if available)
        if (
            "insights" in processing_result
            and "recommendations" in processing_result["insights"]
        ):
            insights_data = []
            for rec in processing_result["insights"]["recommendations"]:
                insights_data.append(
                    {
                        "Type": rec["type"],
                        "Category": rec["category"],
                        "Priority": rec["priority"],
                        "Recommendation": rec["recommendation"],
                        "Potential_Impact": rec["potential_impact"],
                    }
                )

            if insights_data:
                insights_df = pd.DataFrame(insights_data)
                insights_df.to_excel(
                    writer, sheet_name="Business_Insights", index=False
                )

    return temp_file_path


def create_csv_exports(processing_result, original_filename):
    """
    Create individual CSV files for each analysis and return as a ZIP file
    """

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f"csv_exports_{timestamp}_{original_filename.rsplit('.', 1)[0]}.zip"
    zip_path = os.path.join(tempfile.gettempdir(), zip_filename)

    # Get processed data
    transactions_df = processing_result["processed_data"]["transactions_df"]
    customers_df = processing_result["processed_data"]["customers_df"]
    products_df = processing_result["processed_data"]["products_df"]

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:

        # Add main data files
        transactions_csv = transactions_df.to_csv(index=False)
        zipf.writestr("01_transactions_cleaned.csv", transactions_csv)

        customers_csv = customers_df.to_csv(index=False)
        zipf.writestr("02_customers_enhanced.csv", customers_csv)

        products_csv = products_df.to_csv(index=False)
        zipf.writestr("03_products.csv", products_csv)

        # Add analysis files
        customer_rankings_df = pd.DataFrame(
            processing_result["analysis_results"]["customer_rankings"][
                "customer_rankings"
            ]
        )
        rankings_csv = customer_rankings_df.to_csv(index=False)
        zipf.writestr("04_customer_rankings.csv", rankings_csv)

        category_totals_df = processing_result["analysis_results"][
            "customer_category_totals"
        ]["customer_category_totals"]
        category_csv = category_totals_df.to_csv(index=False)
        zipf.writestr("05_customer_category_spending.csv", category_csv)

        # Add top spenders analysis
        top_spenders_data = []
        for category, spender_info in processing_result["analysis_results"][
            "top_spenders_by_category"
        ].items():
            top_spenders_data.append(
                {
                    "Category": category,
                    "Top_Customer_ID": spender_info["customer_id"],
                    "Amount_Spent": spender_info["amount_spent"],
                    "Percentage_of_Category": spender_info["percentage_of_category"],
                    "Total_Customer_Spending": spender_info[
                        "total_spending_all_categories"
                    ],
                }
            )

        top_spenders_df = pd.DataFrame(top_spenders_data)
        top_spenders_csv = top_spenders_df.to_csv(index=False)
        zipf.writestr("06_top_spenders_by_category.csv", top_spenders_csv)

        # Add README file
        readme_content = f"""
# Processed Data Export - {original_filename}
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Files Included:

1. **01_transactions_cleaned.csv** - Cleaned transaction data with date formatting
2. **02_customers_enhanced.csv** - Customer data with geolocation (if enabled)
3. **03_products.csv** - Product catalog
4. **04_customer_rankings.csv** - Customers ranked by spending with segments
5. **05_customer_category_spending.csv** - Customer spending by product category
6. **06_top_spenders_by_category.csv** - Top spending customer per category

## Summary Statistics:
- Total Customers: {processing_result['summary_stats']['total_customers']}
- Total Transactions: {processing_result['summary_stats']['total_transactions']}
- Total Revenue: ${processing_result['summary_stats']['total_revenue']:,.2f}
- Geocoded Customers: {processing_result['summary_stats'].get('geocoded_customers', 0)}

## Notes:
- All dates are formatted as YYYY-MM-DD
- Monetary values are in original currency
- Geolocation data (lat/lng) included if geocoding was enabled
- Customer segments: VIP, High Value, Medium Value, Low Value

For questions about this data, refer to the original analysis report.
"""
        zipf.writestr("README.txt", readme_content)

    return zip_path


def create_geolocation_kml(processing_result, original_filename):
    """
    Create a KML file for viewing customer locations in Google Earth/Maps
    """

    customers_df = processing_result["processed_data"]["customers_df"]

    # Filter customers with valid coordinates
    geocoded_customers = customers_df.dropna(subset=["latitude", "longitude"])

    if len(geocoded_customers) == 0:
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    kml_filename = (
        f"customer_locations_{timestamp}_{original_filename.rsplit('.', 1)[0]}.kml"
    )
    kml_path = os.path.join(tempfile.gettempdir(), kml_filename)

    # Create KML content
    kml_content = """<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>
    <name>Customer Locations</name>
    <description>Customer locations from processed data</description>
    
    <Style id="customer-icon">
        <IconStyle>
            <Icon>
                <href>http://maps.google.com/mapfiles/kml/pal2/icon26.png</href>
            </Icon>
        </IconStyle>
    </Style>
"""

    # Add customer placemarks
    for _, customer in geocoded_customers.iterrows():
        kml_content += f"""
    <Placemark>
        <name>{customer['customer_id']} - {customer['name']}</name>
        <description>
            <![CDATA[
            <b>Customer:</b> {customer['name']}<br/>
            <b>Email:</b> {customer['email']}<br/>
            <b>Address:</b> {customer['address']}<br/>
            <b>Geocoded by:</b> {customer.get('geo_provider', 'Unknown')}<br/>
            <b>Confidence:</b> {customer.get('geo_confidence', 'N/A')}
            ]]>
        </description>
        <styleUrl>#customer-icon</styleUrl>
        <Point>
            <coordinates>{customer['longitude']},{customer['latitude']},0</coordinates>
        </Point>
    </Placemark>"""

    kml_content += """
</Document>
</kml>"""

    with open(kml_path, "w", encoding="utf-8") as f:
        f.write(kml_content)

    return kml_path


def add_download_routes_to_app(app):
    """
    Add download routes to the Flask application
    """

    # Import process_data within the function to avoid circular imports
    from data_processing import process_data

    @app.route("/download/processed-excel/<filename>")
    def download_processed_excel(filename):
        """Download complete processed data as Excel file"""

        file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)

        if not os.path.exists(file_path):
            return "Original file not found", 404

        try:
            # Re-process the data to get results
            processing_result = process_data(file_path, use_geolocation=True)

            # Create processed Excel file
            processed_file_path = create_processed_excel_file(
                processing_result, filename
            )

            # Send file and clean up
            return send_file(
                processed_file_path,
                as_attachment=True,
                download_name=f"processed_{filename}",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        except Exception as e:
            app.logger.error(f"Error creating processed file: {str(e)}")
            return f"Error creating processed file: {str(e)}", 500
        finally:
            # Clean up temporary file
            if "processed_file_path" in locals() and os.path.exists(
                processed_file_path
            ):
                try:
                    os.remove(processed_file_path)
                except:
                    pass

    @app.route("/download/csv-export/<filename>")
    def download_csv_export(filename):
        """Download all data as CSV files in a ZIP archive"""

        file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)

        if not os.path.exists(file_path):
            return "Original file not found", 404

        try:
            # Re-process the data
            processing_result = process_data(file_path, use_geolocation=True)

            # Create CSV export ZIP
            zip_file_path = create_csv_exports(processing_result, filename)

            # Send file and clean up
            return send_file(
                zip_file_path,
                as_attachment=True,
                download_name=f"csv_export_{filename.rsplit('.', 1)[0]}.zip",
                mimetype="application/zip",
            )

        except Exception as e:
            app.logger.error(f"Error creating CSV export: {str(e)}")
            return f"Error creating CSV export: {str(e)}", 500
        finally:
            # Clean up temporary file
            if "zip_file_path" in locals() and os.path.exists(zip_file_path):
                try:
                    os.remove(zip_file_path)
                except:
                    pass

    @app.route("/download/geolocation-kml/<filename>")
    def download_geolocation_kml(filename):
        """Download customer locations as KML file for Google Earth/Maps"""

        file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)

        if not os.path.exists(file_path):
            return "Original file not found", 404

        try:
            # Re-process the data
            processing_result = process_data(file_path, use_geolocation=True)

            # Create KML file
            kml_file_path = create_geolocation_kml(processing_result, filename)

            if not kml_file_path:
                return "No geolocation data available for KML export", 400

            # Send file and clean up
            return send_file(
                kml_file_path,
                as_attachment=True,
                download_name=f"customer_locations_{filename.rsplit('.', 1)[0]}.kml",
                mimetype="application/vnd.google-earth.kml+xml",
            )

        except Exception as e:
            app.logger.error(f"Error creating KML file: {str(e)}")
            return f"Error creating KML file: {str(e)}", 500
        finally:
            # Clean up temporary file
            if "kml_file_path" in locals() and os.path.exists(kml_file_path):
                try:
                    os.remove(kml_file_path)
                except:
                    pass

    @app.route("/download/summary-report/<filename>")
    def download_summary_report(filename):
        """Download a summary report as PDF (future enhancement)"""

        # Placeholder for PDF report generation
        return "PDF report generation coming soon!", 200
