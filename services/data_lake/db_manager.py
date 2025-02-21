import os
import time
import pandas as pd
from services.data_lake.data_cleaner import clean_data
from services.data_lake.data_integrity import validate_data
from services.data_lake.app import db
from services.data_lake.models import RealTimeData
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta
from sqlalchemy import text
import pickle  # For serializing the DataFrame
import logging
import redis
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Redis parameters from environment variables (with defaults)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# For real-time data, use a short TTL (e.g., 5 seconds)
REALTIME_CACHE_TTL = int(os.getenv("REALTIME_CACHE_TTL", 20))

# Initialize Redis client with connection pooling and timeout settings
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    socket_timeout=5,  # seconds
    socket_connect_timeout=5
)


CSV_FOLDER = "G:/Fyers"  # Update the path

def ingest_data():
    """
    Reads CSV files from the folder, processes only the last 6 months' data, cleans, validates, 
    and inserts it into the database.
    """
    try:
        # Get the date 6 months ago from today
        six_months_ago = datetime.today() - timedelta(days=6*30)  # Approximate 6 months

        for file in os.listdir(CSV_FOLDER):
            if file.endswith('.csv'):
                file_path = os.path.join(CSV_FOLDER, file)
                df = pd.read_csv(file_path)

                # Ensure column names are in lowercase
                df.columns = df.columns.str.lower()

                # Check if 'date' column exists
                if 'date' not in df.columns:
                    print(f"⚠️ Skipping {file} - No 'date' column found.")
                    continue

                # Convert 'date' column to datetime
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                print(" df['date']....",  df['date'])
                # Filter data for the last 6 months
                df = df[df['date'] >= six_months_ago]

                if df.empty:
                    print(f"⚠️ Skipping {file} - No data in the last 6 months.")
                    continue

                # Clean data
                df = clean_data(df)

                # Validate data
                if not validate_data(df):
                    print(f"❌ Integrity check failed for {file}. Skipping.")
                    continue

                # Insert data into the database
                if insert_data(df):
                    print(f"✅ Successfully ingested {file}")
                else:
                    print(f"❌ Failed to ingest {file}")
    except Exception as e:
        print(f"❌ Error in ingest_data: {e}")
        

def insert_data(df: pd.DataFrame) -> bool:
    """Insert data efficiently with batch processing and automatic partition creation."""
    try:
        if df.empty:
            return False

        # Ensure correct time format
        df['date'] = pd.to_datetime(df['date']).dt.date  # Convert to just date (without time)
        df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S').dt.time  # Convert to time only
        # df["time"] = pd.to_datetime(df["time"], errors="coerce")

        for symbol, symbol_df in df.groupby("symbol"):
            # Create symbol table if not exists
            table_name = RealTimeData.create_symbol_table(symbol)

            # Create partitions dynamically
            for date in symbol_df["date"].unique():
                start_date = datetime.strptime(str(date), "%Y-%m-%d").replace(day=1)
                RealTimeData.create_partition(symbol, start_date)

            # Batch insert data using ON CONFLICT DO NOTHING to avoid duplicates
            insert_sql = f"""
                INSERT INTO {table_name} (symbol, date, time, open, high, low, close, volume)
                VALUES (:symbol, :date, :time, :open_price, :high_price, :low_price, :close_price, :volume)
                ON CONFLICT DO NOTHING;
            """
            db.session.execute(text(insert_sql), symbol_df.to_dict(orient="records"))
            db.session.commit()

        return True
    except Exception as e:
        db.session.rollback()  # Rollback in case of an error
        print(f"Error inserting data: {e}")
        return False



def fetch_data(symbol: str, start_date: str, end_date: str, use_cache: bool = True) -> pd.DataFrame:
    
    try:
        # Convert date strings to date objects
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        # Create a unique cache key based on parameters
        cache_key = f"{symbol.lower()}_{start_date}_{end_date}"
        
        if use_cache:
            start_cache_time = time.time()
            try:
                cached_data = redis_client.get(cache_key)
            except redis.RedisError as re:
                logger.error("Redis error during get: %s", re)
                cached_data = None
            
            if cached_data:
                logger.info("Returning data from cache for key: %s", cache_key)
                end_cache_time = time.time()
                logger.info("Cache lookup time: %.2f ms", (end_cache_time - start_cache_time) * 1000)
                return pickle.loads(cached_data)
        
        # If cache is disabled or cache miss, execute the query against the database
        start_db_time = time.time()
        table_name = f"{symbol.lower()}"  # Use consistent table naming
        sql_query = f"""
            SELECT date, time, open, high, low, close, volume
            FROM {table_name}
            WHERE date BETWEEN :start_date AND :end_date
            ORDER BY date, time
        """
        df = pd.read_sql(
            text(sql_query),
            db.engine,
            params={"start_date": start_date_obj, "end_date": end_date_obj}
        )
        end_db_time = time.time()
        db_query_time = (end_db_time - start_db_time) * 1000
        logger.info("Database query executed in %.2f ms", db_query_time)
        
        # Cache the result in Redis with the short TTL
        if use_cache:
            try:
                redis_client.setex(cache_key, REALTIME_CACHE_TTL, pickle.dumps(df))
                logger.info("Cached data with key: %s (TTL: %s seconds)", cache_key, REALTIME_CACHE_TTL)
            except redis.RedisError as re:
                logger.error("Redis error during setex: %s", re)
        
        if db_query_time > 20:
            logger.warning("Query time exceeded 20ms: %.2f ms", db_query_time)
        
        return df

    except SQLAlchemyError as e:
        logger.error("Error fetching data: %s", e)
        return pd.DataFrame()
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        return pd.DataFrame()
