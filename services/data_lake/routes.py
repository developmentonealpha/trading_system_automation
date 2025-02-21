import os
import pandas as pd
from flask import Blueprint, request, jsonify
# from werkzeug.utils import secure_filename
from services.data_lake.db_manager import insert_data, fetch_data
# from services.data_lake.data_cleaner import clean_data
# from services.data_lake.data_integrity import check_integrity
# from services.data_lake.gap_detector import detect_gaps
from services.data_lake.db_manager import ingest_data

data_lake_bp = Blueprint("data_lake", __name__)

UPLOAD_FOLDER = "G:/Fyers"

@data_lake_bp.route("/", methods=["GET"])
def home():
    return jsonify({"message": "✅ Trading System Automation API is Running!"})

@data_lake_bp.route("/fetch/<symbol>/<start_date>/<end_date>", methods=["GET"])
def get_stock_data(symbol, start_date, end_date):
    df = fetch_data(symbol, start_date, end_date)
    return df.to_json(orient="records") if not df.empty else jsonify({"error": "No data found"})


@data_lake_bp.route("/upload-csv", methods=["GET"])
def upload_csv():
    ingest_data()
    return jsonify({"message": "✅ Data ingestion complete"})