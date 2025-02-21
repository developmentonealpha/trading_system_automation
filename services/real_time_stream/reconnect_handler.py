import logging
import time

class ReconnectHandler:
    """
    ReconnectHandler handles reconnection logic for streaming connections.
    """
    def __init__(self, reconnect_interval=5):
        self.reconnect_interval = reconnect_interval

    def handle_reconnect(self, reconnect_function):
        """
        Attempts to reconnect using the provided reconnect_function.
        """
        logging.info("Attempting to reconnect in %d seconds...", self.reconnect_interval)
        time.sleep(self.reconnect_interval)
        try:
            reconnect_function()
            logging.info("Reconnection successful.")
        except Exception as e:
            logging.error("Reconnection failed: %s", e)
            # Retry reconnection.
            self.handle_reconnect(reconnect_function)
