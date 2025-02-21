import pandas as pd
from services.data_lake.app import db
from services.data_lake.models import RealTimeData
from services.data_lake.gap_detector import detect_gaps  
from services.data_lake.db_manager import insert_data  

def fetch_data_from_db(symbol: str) -> pd.DataFrame:
    """
    Fetch stock data for a given symbol from the database.
    """
    try:
        query = db.session.query(RealTimeData).filter(
            RealTimeData.symbol == symbol
        ).order_by(RealTimeData.date, RealTimeData.time)

        df = pd.read_sql(query.statement, db.session.bind)

        if df.empty:
            print(f"⚠️ No data found for symbol: {symbol}")
            return pd.DataFrame()

        return df
    except Exception as e:
        print(f"❌ Error fetching data from DB: {e}")
        return pd.DataFrame()

def fill_gaps(df: pd.DataFrame, missing_times: pd.DataFrame) -> pd.DataFrame:
    """
    Fills missing timestamps using forward-fill strategy.
    """
    if df.empty or missing_times.empty:
        return pd.DataFrame()

    # Convert date & time to datetime for processing
    df["datetime"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str))
    missing_times["datetime"] = pd.to_datetime(missing_times["date"].astype(str) + " " + missing_times["time"].astype(str))

    # Sort data
    df = df.sort_values(by="datetime")

    # Generate missing rows using forward fill
    missing_df = missing_times.copy()
    missing_df["symbol"] = df["symbol"].iloc[0]  # Set symbol

    merged_df = pd.concat([df, missing_df], ignore_index=True).sort_values(by="datetime")
    merged_df.fillna(method="ffill", inplace=True)  # Forward fill missing values

    return merged_df.drop(columns=["datetime"])

def process_gaps_for_all_symbols():
    """
    Checks for missing timestamps for all symbols and fills gaps.
    """
    try:
        # Fetch unique symbols from the database
        symbols = db.session.query(RealTimeData.symbol).distinct().all()
        symbols = [s[0] for s in symbols]

        for symbol in symbols:
            print(f"🔍 Checking for missing data for {symbol}...")

            df = fetch_data_from_db(symbol)
            if df.empty:
                continue

            missing_times = detect_gaps(df, symbol)

            if not missing_times.empty:
                print(f"⚠️ Found {len(missing_times)} missing rows for {symbol}. Filling gaps...")
                filled_df = fill_gaps(df, missing_times)
                
                if not filled_df.empty:
                    if insert_data(filled_df):  # Use existing insert_data function
                        print(f"✅ Successfully inserted missing data for {symbol}.")
                    else:
                        print(f"❌ Failed to insert missing data for {symbol}.")
            else:
                print(f"✅ No missing data found for {symbol}.")
    
    except Exception as e:
        print(f"❌ Error processing gaps: {e}")


