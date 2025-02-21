# import os
# import pandas as pd
# from services.data_lake.db_manager import insert_data
# from services.data_lake.data_cleaner import clean_data
# from services.data_lake.data_integrity import validate_data
# from services.data_lake.app import db
# from services.data_lake.models import RealTimeData
# from sqlalchemy.exc import SQLAlchemyError

# CSV_FOLDER = "G:/Fyers"  # Update the path

# def ingest_data():
#     try:
#         for file in os.listdir(CSV_FOLDER):
#             if file.endswith('.csv'):
#                 file_path = os.path.join(CSV_FOLDER, file)
                
#                 df = pd.read_csv(file_path)
#                 print("hello df", df)
#                 df.columns = df.columns.str.lower()

#                 df = clean_data(df)

#                 if not validate_data(df):
#                     print(f"❌ Integrity check failed for {file}. Skipping.")
#                     continue

#                 if insert_data(df):
#                     print(f"✅ Successfully ingested {file}")
#                 else:
#                     print(f"❌ Failed to ingest {file}")
#     except Exception as e:
#         print(f"❌ Error in ingest_data: {e}")
        
# def insert_data(df: pd.DataFrame) -> bool:
#     try:
#         if df.empty:
#             print("⚠️ Warning: Empty DataFrame. No data inserted.")
#             return False

#         records = df.to_dict(orient='records')
#         db.session.bulk_insert_mappings(RealTimeData, records)
#         db.session.commit()
#         return True
#     except SQLAlchemyError as e:
#         db.session.rollback()
#         print(f"❌ E206 - Database insertion failed: {e}")
#         return False