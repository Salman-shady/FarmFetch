import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import app
from app import app as flask_app, Order, ProduceItem, OrderStatus, mongo_db

class UIAndTemplateRenderingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        flask_app.config['TESTING'] = True
        flask_app.config['WTF_CSRF_ENABLED'] = False
        cls.client = flask_app.test_client()

    def test_customer_dashboard_rendering(self):
        self.client.get('/quick-login/customer')
        resp = self.client.get('/customer/dashboard')
        self.assertEqual(resp.status_code, 200, "Customer dashboard should load with 200 OK")
        self.assertIn(b'Order History', resp.data)

    def test_checkout_page_rendering(self):
        self.client.get('/quick-login/customer')
        item = ProduceItem.query.first()
        with self.client.session_transaction() as sess:
            sess['cart'] = {str(item.id): 2}
        resp = self.client.get('/checkout')
        self.assertEqual(resp.status_code, 200, "Checkout page should load with 200 OK")
        self.assertIn(b'name="idempotency_key"', resp.data)
        self.assertIn(b'id="btn-place-order"', resp.data)

    def test_order_details_rendering(self):
        self.client.get('/quick-login/customer')
        order = Order.query.first()
        if order:
            resp = self.client.get(f'/order/{order.id}')
            self.assertEqual(resp.status_code, 200, f"Order #{order.id} details should load with 200 OK")
            self.assertIn(b'Live Order Progress Timeline', resp.data)
            self.assertIn(b'id="map"', resp.data)

    def test_farmer_dashboard_and_orders(self):
        self.client.get('/quick-login/farmer')
        resp1 = self.client.get('/farmer/dashboard')
        self.assertEqual(resp1.status_code, 200, "Farmer dashboard should load with 200 OK")
        resp2 = self.client.get('/farmer/orders')
        self.assertEqual(resp2.status_code, 200, "Farm orders should load with 200 OK")
        self.assertIn(b'Manage Incoming Orders', resp2.data)

    def test_driver_dashboard_and_delivery(self):
        self.client.get('/quick-login/driver')
        resp = self.client.get('/driver/dashboard')
        self.assertEqual(resp.status_code, 200, "Driver dashboard should load with 200 OK")
        self.assertIn(b'Available Orders Ready for Pickup', resp.data)

        # Check an active driver order
        driver_doc = mongo_db.users.find_one({'email': 'driver@farm.com'})
        if driver_doc:
            order_doc = mongo_db.orders.find_one({'driver_id': str(driver_doc['_id'])})
            if order_doc:
                resp_deliv = self.client.get(f'/driver/delivery/{order_doc["_id"]}')
                self.assertEqual(resp_deliv.status_code, 200, "Driver delivery page should load with 200 OK")
                self.assertIn(b'driver-map', resp_deliv.data)

if __name__ == '__main__':
    unittest.main()
