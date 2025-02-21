#!/usr/bin/env python
import time
import os
import json
import pandas as pd
import pandas_ta as ta
from datetime import datetime
from services.real_time_stream.stream_client import client

# Parameters for Supertrend.
length = 1
multiplier = 0.01


def format_timestamp(ts):
    """Converts a timestamp (in seconds or milliseconds) to a formatted string."""
    if ts is None:
        return None
    try:
        if ts > 1e11:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        return ts


def safe_float(val):
    """Converts val to float or returns None if conversion fails."""
    try:
        if val is None:
            return None
        return float(val)
    except Exception:
        return None


def supertrend_strategy(data):
    """
    Calculate the Supertrend indicator and generate buy/sell signals.
    Expects the DataFrame to have columns: 'High', 'Low', 'Close'.
    """
    SuperTrend = ta.supertrend(
        high=data['High'],
        low=data['Low'],
        close=data['Close'],
        length=length,
        multiplier=multiplier
    )
    data = data.copy()
    col_name = f"SUPERT_{length}_{multiplier}"
    data.loc[:, 'SuperTrend'] = SuperTrend[col_name]
    data.loc[:, 'signal'] = None

    for i in range(1, len(data.index)):
        if data['SuperTrend'].iloc[i] < data['Close'].iloc[i] and data['SuperTrend'].iloc[i - 1] >= data['Close'].iloc[
            i - 1]:
            data.loc[data.index[i], 'signal'] = 1
        elif data['SuperTrend'].iloc[i] > data['Close'].iloc[i] and data['SuperTrend'].iloc[i - 1] <= \
                data['Close'].iloc[i - 1]:
            data.loc[data.index[i], 'signal'] = -1
    return data


# Global DataFrame to accumulate live data.
live_data_df = pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume', 'time_date'])


def on_live_data_update(new_data):
    """
    Callback to handle new live data from Fyers or Binance.
    Normalizes data into a standard format and computes the Supertrend indicator.
    The processed data is printed.
    """
    if isinstance(new_data, str):
        try:
            new_data = json.loads(new_data)
        except Exception:
            return

    if not isinstance(new_data, dict):
        return

    normalized = {}
    if "data" in new_data:
        # Binance message.
        source = new_data.get("data")
        if source is None:
            return
        normalized = {
            "Open": safe_float(source.get("o")),
            "High": safe_float(source.get("h")),
            "Low": safe_float(source.get("l")),
            "Close": safe_float(source.get("c")),
            "Volume": safe_float(source.get("v")),
            "time_date": format_timestamp(source.get("E"))
        }
    elif "ltp" in new_data:
        # Fyers message.
        normalized = {
            "Open": safe_float(new_data.get("open_price")),
            "High": safe_float(new_data.get("high_price")),
            "Low": safe_float(new_data.get("low_price")),
            "Close": safe_float(new_data.get("ltp")),
            "Volume": safe_float(new_data.get("vol_traded_today")),
            "time_date": format_timestamp(new_data.get("last_traded_time"))
        }
    else:
        normalized = {
            "Open": safe_float(new_data.get("Open") or new_data.get("open")),
            "High": safe_float(new_data.get("High") or new_data.get("high")),
            "Low": safe_float(new_data.get("Low") or new_data.get("low")),
            "Close": safe_float(new_data.get("Close") or new_data.get("close")),
            "Volume": safe_float(new_data.get("Volume") or new_data.get("volume")),
            "time_date": format_timestamp(new_data.get("time_date"))
        }

    for field in ["Open", "High", "Low", "Close", "Volume", "time_date"]:
        if normalized.get(field) is None:
            return

    global live_data_df
    try:
        live_data_df = pd.concat([live_data_df, pd.DataFrame([normalized])], ignore_index=True)
    except Exception:
        return

    try:
        result = supertrend_strategy(live_data_df)
        latest_row = result.iloc[-1].to_dict()
    except Exception:
        return

    print("Supertrend filtered new data:", latest_row)


def main():
    """
    Subscribes to the data stream. Uncomment the desired subscription.
    """
    # To subscribe to Binance:
    client.subscribe_binance(['BTCUSDT'], callback=on_live_data_update)

    # To subscribe to Fyers:
    # client.subscribe_fyers(['NSE:SBIN-EQ'], callback=on_live_data_update)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down.")


if __name__ == '__main__':
    main()
