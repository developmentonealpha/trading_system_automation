import unittest
from ..subscription_manager import SubscriptionManager

class TestSubscriptionManager(unittest.TestCase):
    def test_start_subscriptions(self):
        manager = SubscriptionManager()
        try:
            manager.start_subscriptions()
        except Exception as e:
            self.fail(f"start_subscriptions() raised an exception: {e}")

if __name__ == "__main__":
    unittest.main()
