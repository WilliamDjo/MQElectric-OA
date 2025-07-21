import requests
import time
from typing import Dict, List, Tuple, Optional
import pandas as pd
from geopy.geocoders import Nominatim, GoogleV3
from geopy.exc import GeocoderTimedOut, GeocoderQuotaExceeded
import sqlite3
from datetime import datetime
import hashlib


class GeolocationService:
    """Service for fetching geolocation data with multiple API providers and caching"""

    def __init__(self, use_google_api=False, google_api_key=None):
        self.use_google_api = use_google_api
        self.google_api_key = google_api_key

        # Initialize geocoders
        self.nominatim = Nominatim(user_agent="excel_data_pipeline")
        if use_google_api and google_api_key:
            self.google_geocoder = GoogleV3(api_key=google_api_key)

        # Initialize cache database
        self.init_cache_db()

        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 1.1  # Nominatim limit: 1 request per second

    def init_cache_db(self):
        """Initialize SQLite cache for geolocation results"""
        conn = sqlite3.connect("geolocation_cache.db")
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS geolocation_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address_hash TEXT UNIQUE NOT NULL,
                original_address TEXT NOT NULL,
                normalized_address TEXT,
                latitude REAL,
                longitude REAL,
                provider TEXT,
                confidence_score REAL,
                created_timestamp TEXT NOT NULL,
                last_used_timestamp TEXT NOT NULL
            )
        """
        )
        conn.commit()
        conn.close()

    def get_address_hash(self, address: str) -> str:
        """Generate a hash for the address to use as cache key"""
        return hashlib.md5(address.lower().strip().encode()).hexdigest()

    def get_cached_location(self, address: str) -> Optional[Dict]:
        """Retrieve cached geolocation data"""
        address_hash = self.get_address_hash(address)

        conn = sqlite3.connect("geolocation_cache.db")
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT latitude, longitude, normalized_address, provider, confidence_score
            FROM geolocation_cache 
            WHERE address_hash = ?
        """,
            (address_hash,),
        )

        result = cursor.fetchone()

        if result:
            # Update last used timestamp
            cursor.execute(
                """
                UPDATE geolocation_cache 
                SET last_used_timestamp = ? 
                WHERE address_hash = ?
            """,
                (datetime.now().isoformat(), address_hash),
            )
            conn.commit()

            conn.close()
            return {
                "latitude": result[0],
                "longitude": result[1],
                "normalized_address": result[2],
                "provider": result[3],
                "confidence_score": result[4],
                "cached": True,
            }

        conn.close()
        return None

    def cache_location(self, address: str, location_data: Dict):
        """Cache geolocation results"""
        address_hash = self.get_address_hash(address)
        timestamp = datetime.now().isoformat()

        conn = sqlite3.connect("geolocation_cache.db")
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT OR REPLACE INTO geolocation_cache 
            (address_hash, original_address, normalized_address, latitude, longitude, 
             provider, confidence_score, created_timestamp, last_used_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                address_hash,
                address,
                location_data.get("normalized_address", address),
                location_data.get("latitude"),
                location_data.get("longitude"),
                location_data.get("provider", "unknown"),
                location_data.get("confidence_score", 0.5),
                timestamp,
                timestamp,
            ),
        )

        conn.commit()
        conn.close()

    def respect_rate_limit(self):
        """Ensure we respect API rate limits"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last_request
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def clean_address(self, address: str) -> str:
        """Clean and normalize address for better geocoding results"""
        if not address or pd.isna(address):
            return ""

        # Basic cleaning
        cleaned = str(address).strip()

        # Remove extra whitespace
        cleaned = " ".join(cleaned.split())

        # Ensure it ends with Australia if not already specified
        if "australia" not in cleaned.lower() and "aus" not in cleaned.lower():
            cleaned += ", Australia"

        return cleaned

    def geocode_with_nominatim(self, address: str) -> Optional[Dict]:
        """Geocode using Nominatim (OpenStreetMap) - Free"""
        try:
            self.respect_rate_limit()

            location = self.nominatim.geocode(
                address,
                timeout=10,
                exactly_one=True,
                addressdetails=True,
                country_codes="au",  # Limit to Australia
            )

            if location:
                return {
                    "latitude": location.latitude,
                    "longitude": location.longitude,
                    "normalized_address": location.address,
                    "provider": "nominatim",
                    "confidence_score": 0.8,  # Nominatim doesn't provide confidence scores
                    "cached": False,
                }

        except (GeocoderTimedOut, Exception) as e:
            print(f"Nominatim geocoding failed for '{address}': {e}")

        return None

    def geocode_with_google(self, address: str) -> Optional[Dict]:
        """Geocode using Google Maps API - Requires API key"""
        if not self.use_google_api or not self.google_api_key:
            return None

        try:
            location = self.google_geocoder.geocode(
                address,
                timeout=10,
                region="au",  # Bias towards Australia
                components={"country": "AU"},
            )

            if location:
                return {
                    "latitude": location.latitude,
                    "longitude": location.longitude,
                    "normalized_address": location.address,
                    "provider": "google",
                    "confidence_score": 0.9,  # Google typically has high accuracy
                    "cached": False,
                }

        except Exception as e:
            print(f"Google geocoding failed for '{address}': {e}")

        return None

    def geocode_address(self, address: str) -> Dict:
        """
        Geocode a single address with fallback providers and caching
        """
        if not address or pd.isna(address):
            return {
                "original_address": address,
                "latitude": None,
                "longitude": None,
                "normalized_address": None,
                "provider": None,
                "confidence_score": 0.0,
                "cached": False,
                "error": "Empty address",
            }

        cleaned_address = self.clean_address(address)

        # Check cache first
        cached_result = self.get_cached_location(cleaned_address)
        if cached_result:
            cached_result["original_address"] = address
            return cached_result

        # Try different geocoding providers
        result = None

        # Try Google first if available (higher accuracy)
        if self.use_google_api:
            result = self.geocode_with_google(cleaned_address)

        # Fallback to Nominatim (free)
        if not result:
            result = self.geocode_with_nominatim(cleaned_address)

        # If geocoding failed, return with error
        if not result:
            return {
                "original_address": address,
                "latitude": None,
                "longitude": None,
                "normalized_address": None,
                "provider": None,
                "confidence_score": 0.0,
                "cached": False,
                "error": "Geocoding failed",
            }

        # Cache the result
        self.cache_location(cleaned_address, result)

        # Add original address to result
        result["original_address"] = address
        return result

    def geocode_addresses_bulk(
        self, addresses: List[str], progress_callback=None
    ) -> List[Dict]:
        """
        Geocode multiple addresses with progress tracking
        """
        results = []
        unique_addresses = list(set(addresses))  # Remove duplicates

        print(f"Geocoding {len(unique_addresses)} unique addresses...")

        for i, address in enumerate(unique_addresses):
            if progress_callback:
                progress_callback(i + 1, len(unique_addresses), address)

            result = self.geocode_address(address)
            results.append(result)

            # Progress update
            if (i + 1) % 10 == 0:
                print(f"Processed {i + 1}/{len(unique_addresses)} addresses")

        # Create a mapping for all original addresses (including duplicates)
        address_to_result = {result["original_address"]: result for result in results}

        # Return results in original order, handling duplicates
        final_results = []
        for address in addresses:
            if address in address_to_result:
                final_results.append(address_to_result[address])
            else:
                # Fallback for any missing addresses
                final_results.append(self.geocode_address(address))

        return final_results


def add_geolocation_to_customers(
    customers_df: pd.DataFrame, use_google_api: bool = False, google_api_key: str = None
) -> pd.DataFrame:
    """
    Add geolocation data to customer DataFrame
    """
    print("Starting geolocation processing...")

    # Initialize geolocation service
    geo_service = GeolocationService(
        use_google_api=use_google_api, google_api_key=google_api_key
    )

    # Get unique addresses
    unique_addresses = customers_df["address"].dropna().unique().tolist()

    def progress_callback(current, total, address):
        percent = (current / total) * 100
        print(
            f"Progress: {current}/{total} ({percent:.1f}%) - Processing: {address[:50]}..."
        )

    # Geocode all unique addresses
    geocoding_results = geo_service.geocode_addresses_bulk(
        unique_addresses, progress_callback
    )

    # Create mapping from address to geolocation data
    address_to_geo = {}
    for result in geocoding_results:
        original_addr = result["original_address"]
        address_to_geo[original_addr] = result

    # Add geolocation columns to customers DataFrame
    customers_with_geo = customers_df.copy()

    # Initialize new columns
    customers_with_geo["latitude"] = None
    customers_with_geo["longitude"] = None
    customers_with_geo["normalized_address"] = None
    customers_with_geo["geo_provider"] = None
    customers_with_geo["geo_confidence"] = None
    customers_with_geo["geo_cached"] = None
    customers_with_geo["geo_error"] = None

    # Populate geolocation data
    for idx, row in customers_with_geo.iterrows():
        address = row["address"]
        if address in address_to_geo:
            geo_data = address_to_geo[address]
            customers_with_geo.at[idx, "latitude"] = geo_data.get("latitude")
            customers_with_geo.at[idx, "longitude"] = geo_data.get("longitude")
            customers_with_geo.at[idx, "normalized_address"] = geo_data.get(
                "normalized_address"
            )
            customers_with_geo.at[idx, "geo_provider"] = geo_data.get("provider")
            customers_with_geo.at[idx, "geo_confidence"] = geo_data.get(
                "confidence_score"
            )
            customers_with_geo.at[idx, "geo_cached"] = geo_data.get("cached", False)
            customers_with_geo.at[idx, "geo_error"] = geo_data.get("error")

    # Generate summary statistics
    total_customers = len(customers_with_geo)
    geocoded_customers = customers_with_geo["latitude"].notna().sum()
    cached_results = (
        customers_with_geo["geo_cached"].sum()
        if "geo_cached" in customers_with_geo
        else 0
    )

    print(f"\nGeolocation Summary:")
    print(f"Total customers: {total_customers}")
    print(
        f"Successfully geocoded: {geocoded_customers} ({geocoded_customers/total_customers*100:.1f}%)"
    )
    print(f"Cached results used: {cached_results}")
    print(f"New API calls made: {geocoded_customers - cached_results}")

    return customers_with_geo


def generate_geolocation_insights(customers_with_geo: pd.DataFrame) -> Dict:
    """Generate insights from geolocation data"""

    # Filter customers with valid coordinates
    geocoded_customers = customers_with_geo.dropna(subset=["latitude", "longitude"])

    if len(geocoded_customers) == 0:
        return {"error": "No customers have valid geolocation data"}

    insights = {
        "geocoding_stats": {
            "total_customers": len(customers_with_geo),
            "geocoded_customers": len(geocoded_customers),
            "geocoding_success_rate": len(geocoded_customers)
            / len(customers_with_geo)
            * 100,
            "cached_results": (
                customers_with_geo["geo_cached"].sum()
                if "geo_cached" in customers_with_geo
                else 0
            ),
        },
        "geographic_distribution": {
            "center_latitude": geocoded_customers["latitude"].mean(),
            "center_longitude": geocoded_customers["longitude"].mean(),
            "latitude_range": {
                "min": geocoded_customers["latitude"].min(),
                "max": geocoded_customers["latitude"].max(),
            },
            "longitude_range": {
                "min": geocoded_customers["longitude"].min(),
                "max": geocoded_customers["longitude"].max(),
            },
        },
        "provider_stats": (
            customers_with_geo["geo_provider"].value_counts().to_dict()
            if "geo_provider" in customers_with_geo
            else {}
        ),
        "confidence_stats": {
            "average_confidence": (
                customers_with_geo["geo_confidence"].mean()
                if "geo_confidence" in customers_with_geo
                else 0
            ),
            "high_confidence_count": (
                (customers_with_geo["geo_confidence"] > 0.8).sum()
                if "geo_confidence" in customers_with_geo
                else 0
            ),
        },
    }

    return insights
