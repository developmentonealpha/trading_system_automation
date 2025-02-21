import pandas as pd

def detect_gaps(df: pd.DataFrame, symbol: str):
    try:
        if df.empty:
            print("⚠️ Warning: DataFrame is empty. Cannot detect gaps.")
            return
        
        required_columns = {'symbol', 'date', 'time'}
        if not required_columns.issubset(df.columns):
            print(f"❌ Error: Missing required columns {required_columns - set(df.columns)}.")
            return

        df = df[df['symbol'] == symbol]
        
        if df.empty:
            print(f"⚠️ Warning: No data found for {symbol}.")
            return

        df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))
        df = df.sort_values(by='datetime')

        df['time_diff'] = df['datetime'].diff()

        missing_times = df[df['time_diff'] > pd.Timedelta(minutes=1)]
        
        if not missing_times.empty:
            print(f"⚠️ Gaps detected for {symbol}:")
            print(missing_times[['date', 'time', 'time_diff']])
        else:
            print(f"✅ No gaps detected for {symbol}")

    except Exception as e:
        print(f"❌ Error in detect_gaps: {e}")
