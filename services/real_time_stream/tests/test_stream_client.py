import unittest
from ..stream_client import StreamClient

class TestStreamClient(unittest.TestCase):
    def setUp(self):
        # Use a dummy token for testing.
        self.client = StreamClient()

    def test_subscribe_fyers(self):
        try:
            self.client.subscribe_fyers()
        except Exception as e:
            self.fail(f"subscribe_fyers() raised an exception: {e}")

    def test_subscribe_binance(self):
        try:
            self.client.subscribe_binance(["btcusdt", "ethusdt"])
        except Exception as e:
            self.fail(f"subscribe_binance() raised an exception: {e}")

if __name__ == "__main__":
    unittest.main()
