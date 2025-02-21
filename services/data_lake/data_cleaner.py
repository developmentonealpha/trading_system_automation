import pandas as pd

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    try:
        if df.empty:
            print("⚠️ Warning: Empty DataFrame. No cleaning performed.")
            return df  # Return empty DataFrame

        df.dropna(inplace=True)  # Remove missing values
        df.drop_duplicates(inplace=True)  # Remove duplicates

        # Rename columns to match database schema
        df.columns = df.columns.str.lower().str.strip()
        column_mapping = {
            "symbol": "symbol",
            "date": "date",
            "time": "time",
            "open": "open_price",
            "high": "high_price",
            "low": "low_price",
            "close": "close_price",
            "volume": "volume",
        }
        df.rename(columns=column_mapping, inplace=True)

        # Check required columns
        required_columns = set(column_mapping.values())
        missing_cols = required_columns - set(df.columns)
        if missing_cols:
            print(f"❌ Error: Missing columns: {missing_cols}")
            return df

        # Convert date & time safely
        df["date"] = pd.to_datetime(df["date"], errors="coerce")  # Auto-detect format
        df["time"] = pd.to_datetime(df["time"], format="%H:%M:%S", errors="coerce").dt.time

        # Convert numeric values safely
        for col in ["open_price", "high_price", "low_price", "close_price"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

        return df
    except Exception as e:
        print(f"❌ Error in clean_data: {e}")
        return df
