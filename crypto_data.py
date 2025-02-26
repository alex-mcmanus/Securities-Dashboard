import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging
import os

# Set the output directory to the data folder in the repository
OUTPUT_DIR = "data"
OUTPUT_FILE = "crypto_historical_data.csv"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure API settings
BASE_URL = "https://api.coingecko.com/api/v3"
REQUEST_DELAY = 1.2  # More conservative delay for rate limiting
MAX_RETRIES = 3
RETRY_STATUS_CODES = (429, 500, 502, 503, 504)

# Configure requests session with retry logic
session = requests.Session()
retry_strategy = Retry(
    total=MAX_RETRIES,
    backoff_factor=1,
    status_forcelist=RETRY_STATUS_CODES,
    allowed_methods=["GET"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

def get_top_100_coin_ids():
    """Retrieve top 100 coin IDs with retry mechanism"""
    url = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1
    }
    
    try:
        response = session.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if len(data) != 100:
            logger.warning(f"Received {len(data)} coins instead of 100")
            
        return [coin["id"] for coin in data]
    
    except Exception as e:
        logger.error(f"Failed to fetch top 100 coins: {e}")
        return []

def fetch_coin_data(coin_id, days=365):
    """Fetch historical data with enhanced error handling and retries"""
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": "usd",
        "days": days,
        "interval": "daily"
    }
    
    try:
        # Initial attempt with specified days parameter
        response = session.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        # Handle empty responses
        if not data.get("prices"):
            logger.info(f"No data for {coin_id} with days={days}. Trying max range.")
            return fetch_max_data(coin_id)

        return process_data(data, coin_id)

    except requests.HTTPError as e:
        if e.response.status_code == 429:
            logger.warning("Rate limit detected. Implementing backoff delay.")
            time.sleep(10)
            return fetch_coin_data(coin_id, days)
        logger.error(f"HTTP error for {coin_id}: {e}")
    except Exception as e:
        logger.error(f"General error fetching {coin_id}: {e}")
    
    return None

def fetch_max_data(coin_id):
    """Handle max days fallback with date filtering"""
    try:
        response = session.get(
            f"{BASE_URL}/coins/{coin_id}/market_chart",
            params={"vs_currency": "usd", "days": "max", "interval": "daily"}
        )
        response.raise_for_status()
        data = response.json()

        if not data.get("prices"):
            logger.warning(f"No data available for {coin_id} even with max days")
            return None

        df = process_data(data, coin_id)
        cutoff_date = datetime.now() - timedelta(days=365)
        return df[df["date"] >= cutoff_date]

    except Exception as e:
        logger.error(f"Error fetching max data for {coin_id}: {e}")
        return None

def process_data(data, coin_id):
    """Process and validate API response data"""
    try:
        prices = data["prices"]
        market_caps = data.get("market_caps", [])
        volumes = data.get("total_volumes", [])
        
        # Create DataFrame with validation
        df = pd.DataFrame(prices, columns=["timestamp", "price"])
        if len(df) < 365:
            logger.info(f"{coin_id} has only {len(df)} days of data")
            
        # Add additional metrics if available
        df["market_cap"] = [x[1] for x in market_caps] if market_caps else pd.NA
        df["volume"] = [x[1] for x in volumes] if volumes else pd.NA
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["coin"] = coin_id
        
        return df[["date", "coin", "price", "market_cap", "volume"]]
    
    except KeyError as e:
        logger.error(f"Missing expected data field {e} for {coin_id}")
        return None

def main():
    logger.info("Starting data retrieval process")
    
    # Get coin IDs
    coin_ids = get_top_100_coin_ids()
    if not coin_ids:
        logger.error("Failed to retrieve coin IDs. Exiting.")
        return

    logger.info(f"Retrieved {len(coin_ids)} coin IDs")
    
    # Fetch historical data
    dfs = []
    for i, coin_id in enumerate(coin_ids, 1):
        logger.info(f"Processing {coin_id} ({i}/{len(coin_ids)})")
        try:
            df = fetch_coin_data(coin_id)
            if df is not None:
                dfs.append(df)
                logger.info(f"Retrieved {len(df)} records for {coin_id}")
            else:
                logger.warning(f"Failed to retrieve data for {coin_id}")
            
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            logger.error(f"Unexpected error processing {coin_id}: {e}")
    
    # Save results
    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
        try:
            final_df.to_csv("crypto_historical_data.csv", index=False)
            logger.info(f"Successfully saved data with {len(final_df)} records")
        except Exception as e:
            logger.error(f"Failed to save CSV: {e}")
    else:
        logger.error("No data collected. CSV not created.")

if __name__ == "__main__":
    main()
