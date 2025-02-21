import pandas as pd

def validate_data(df: pd.DataFrame) -> bool:
    try:
        if df.empty:
            print("⚠️ Warning: Empty DataFrame. Skipping integrity check.")
            return False
        print('validate_data', df)
        if df.isnull().sum().sum() > 0:
            print("❌ Data Integrity Issue: Null values detected")
            return False

        if df.duplicated().sum() > 0:
            print("❌ Data Integrity Issue: Duplicate records found")
            return False

        return True
    except Exception as e:
        print(f"❌ Error in validate_data: {e}")
        return False
