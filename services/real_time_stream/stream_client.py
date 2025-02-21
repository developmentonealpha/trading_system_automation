import json
import logging
import websocket
from fyers_apiv3.FyersWebsocket import data_ws  # Using data_ws instead of order_ws
from .config import FYERS_TOKEN, BINANCE_SYMBOLS

# Set up logging.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


class StreamClient:
    """
    Provides methods to subscribe to real-time data feeds and forward parsed JSON data to a callback.
    """

    def __init__(self, fyers_token):
        self.fyers_token = fyers_token

    def subscribe_fyers(self, symbols, callback=None):
        """
        Subscribe to the Fyers real-time data stream.

        Parameters:
            symbols (list): List of symbols to subscribe to.
            callback (function): A function to call with each received message.
        """
        print(f"Subscribed to {symbols}")

        def onmessage(message):
            if callback:
                callback(message)

        def onerror(message):
            logging.error("Fyers Error: %s", message)

        def onclose(message):
            logging.info("Fyers Connection closed: %s", message)

        def onopen():
            data_type = "SymbolUpdate"
            logging.info("Fyers WebSocket connected. Subscribing to symbols: %s with data type: %s", symbols, data_type)
            fyers.subscribe(symbols=symbols, data_type=data_type)
            fyers.keep_running()

        fyers = data_ws.FyersDataSocket(
            access_token=self.fyers_token,
            log_path="",
            litemode=False,
            write_to_file=False,
            reconnect=True,
            on_connect=onopen,
            on_close=onclose,
            on_error=onerror,
            on_message=onmessage
        )

        logging.info("Attempting to connect to the Fyers WebSocket...")
        fyers.connect()

    def subscribe_binance(self, symbols, callback=None):
        """
        Subscribe to the Binance real-time stream.

        Parameters:
            symbols (list): List of symbols to subscribe to.
            callback (function): A function to call with each received message.
        """
        streams = [f"{symbol.lower()}@ticker" for symbol in symbols]
        ws_url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)
        logging.info("Connecting to Binance stream: %s", ws_url)

        def on_open(ws):
            logging.info("Binance WebSocket connection opened.")

        def on_message(ws, message):
            if callback:
                callback(message)

        def on_error(ws, error):
            logging.error("Binance WebSocket error: %s", error)

        def on_close(ws, close_status_code, close_msg):
            logging.info("Binance WebSocket closed: [%s] %s", close_status_code, close_msg)

        ws_app = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws_app.run_forever()


# Create a global instance named 'client' that can be imported elsewhere.
client = StreamClient(FYERS_TOKEN)
