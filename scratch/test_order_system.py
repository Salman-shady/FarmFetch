import sys
import os
import unittest
import datetime

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import app
from app import (
    app as flask_app,
    mongo_db,
    User,
    Farm,
    ProduceItem,
    Order,
    OrderItem,
    OrderStatus,
    transition_order_status,
    restore_order_stock_once,
    hash_otp,
    verify_otp_hash,
    generate_secure_otp
)

class OrderManagementTestSuite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        flask_app.config['TESTING'] = True
        flask_app.config['WTF_CSRF_ENABLED'] = False
        cls.client = flask_app.test_client()

        # Ensure demo users exist
        cls.farmer_user = User.query.filter_by(email='farmer@farm.com').first()
        if not cls.farmer_user:
            cls.farmer_user = User(email='farmer@farm.com', role='farmer')
            cls.farmer_user.set_password('password123')
            cls.farmer_user.save()

        cls.customer_user = User.query.filter_by(email='customer@farm.com').first()
        if not cls.customer_user:
            cls.customer_user = User(email='customer@farm.com', role='customer')
            cls.customer_user.set_password('password123')
            cls.customer_user.save()

        cls.driver_user = User.query.filter_by(email='driver@farm.com').first()
        if not cls.driver_user:
            cls.driver_user = User(email='driver@farm.com', role='driver')
            cls.driver_user.set_password('password123')
            cls.driver_user.save()

        # Ensure second driver for race condition test
        cls.driver2_user = User.query.filter_by(email='driver2@farm.com').first()
        if not cls.driver2_user:
            cls.driver2_user = User(email='driver2@farm.com', role='driver')
            cls.driver2_user.set_password('password123')
            cls.driver2_user.save()

        # Ensure farm exists
        cls.farm = Farm.query.filter_by(user_id=cls.farmer_user.id).first()
        if not cls.farm:
            cls.farm = Farm(
                name="Green Valley Organics",
                address="Highway 44, Green Meadows",
                farm_type="Organic Vegetables",
                user_id=cls.farmer_user.id,
                latitude=12.9716,
                longitude=77.5946
            )
            cls.farm.save()

    def test_01_concurrent_inventory_safety(self):
        """Scenario 1: Two customers attempt to purchase the same last available item."""
        print("\n--- Test 1: Concurrent Inventory Safety ---")
        # Create a test item with stock = 1
        item = ProduceItem(
            name="Limited Heirloom Tomato",
            price=5.00,
            stock=1,
            unit="kg",
            farm_id=self.farm.id
        )
        item.save()
        item_id = item.id

        # Customer 1 attempts purchase (should succeed)
        res1 = mongo_db['produce_items'].find_one_and_update(
            {'_id': item_id, 'stock': {'$gte': 1}},
            {'$inc': {'stock': -1}},
            return_document=app.pymongo.ReturnDocument.AFTER
        )
        self.assertIsNotNone(res1, "First customer should successfully decrement stock")
        self.assertEqual(res1['stock'], 0, "Stock should be 0 after first customer")

        # Customer 2 attempts purchase of the same item (must fail)
        res2 = mongo_db['produce_items'].find_one_and_update(
            {'_id': item_id, 'stock': {'$gte': 1}},
            {'$inc': {'stock': -1}},
            return_document=app.pymongo.ReturnDocument.AFTER
        )
        self.assertIsNone(res2, "Second customer must be blocked from purchasing unavailable stock")
        
        # Verify stock did not become negative
        final_doc = mongo_db['produce_items'].find_one({'_id': item_id})
        self.assertEqual(final_doc['stock'], 0, "Stock must never become negative")
        print("PASS: Concurrent stock purchase handled atomically without overselling.")

    def test_02_duplicate_order_idempotency(self):
        """Scenario 2: A customer submits the same order twice with the same idempotency key."""
        print("\n--- Test 2: Idempotency Prevention ---")
        self.client.get('/quick-login/customer')

        # Add item to cart
        item = ProduceItem.query.filter_by(farm_id=self.farm.id).first()
        with self.client.session_transaction() as sess:
            sess['cart'] = {str(item.id): 1}

        idempotency_token = "test_idem_token_" + generate_secure_otp(8)

        # First checkout submission
        resp1 = self.client.post('/checkout', data={
            'name': 'Test Buyer',
            'address': '123 Test St',
            'phone': '9876543210',
            'idempotency_key': idempotency_token,
            'customer_latitude': '12.9716',
            'customer_longitude': '77.5946'
        }, follow_redirects=False)

        self.assertIn(resp1.status_code, [302, 303], "First order should redirect to details")
        order_redirect_url = resp1.headers['Location']
        created_order_id = int(order_redirect_url.split('/')[-1])

        # Second submission with identical idempotency token
        with self.client.session_transaction() as sess:
            sess['cart'] = {str(item.id): 1}

        resp2 = self.client.post('/checkout', data={
            'name': 'Test Buyer',
            'address': '123 Test St',
            'phone': '9876543210',
            'idempotency_key': idempotency_token,
            'customer_latitude': '12.9716',
            'customer_longitude': '77.5946'
        }, follow_redirects=False)

        self.assertEqual(resp2.headers['Location'], order_redirect_url, "Duplicate submission must redirect to existing order")

        # Count orders with this idempotency key in DB
        count = mongo_db['orders'].count_documents({'idempotency_key': idempotency_token})
        self.assertEqual(count, 1, "Exactly one order must exist in MongoDB for that idempotency key")
        print("PASS: Duplicate order prevented; idempotency strictly enforced.")

    def test_03_unauthorized_order_update_rejection(self):
        """Scenario 3: An unauthorized user attempts to update or cancel another user's order."""
        print("\n--- Test 3: Unauthorized Access Rejection ---")
        # Create an order belonging to customer
        order = Order(
            customer_id=self.customer_user.id,
            farm_id=self.farm.id,
            customer_name='Legit Customer',
            customer_address='456 Elm St',
            customer_phone='9998887776',
            total_price=15.00,
            status=OrderStatus.PLACED
        )
        order.save()

        # Login as Driver (driver has no authority to cancel or accept on behalf of farmer)
        self.client.get('/quick-login/driver')
        resp = self.client.post(f'/farmer/order/accept/{order.id}', follow_redirects=False)
        self.assertIn(resp.status_code, [302, 403], "Driver should be denied farmer order accept")

        # Driver attempts to cancel customer's order
        resp_cancel = self.client.post(f'/order/{order.id}/cancel', follow_redirects=False)
        self.assertIn(resp_cancel.status_code, [302, 403])
        
        # Verify order remains PLACED
        refreshed = Order.query.get(order.id)
        self.assertEqual(refreshed.status, OrderStatus.PLACED, "Order status must remain PLACED")
        print("PASS: Unauthorized action rejected and order integrity maintained.")

    def test_04_driver_double_accept_prevention(self):
        """Scenario 4: Two drivers attempt to accept the same delivery assignment."""
        print("\n--- Test 4: Driver Assignment Race Condition Prevention ---")
        order = Order(
            customer_id=self.customer_user.id,
            farm_id=self.farm.id,
            customer_name='Recipient',
            customer_address='789 Oak Rd',
            customer_phone='5551234567',
            total_price=22.50,
            status=OrderStatus.READY_FOR_PICKUP,
            driver_id=None
        )
        order.save()

        # Driver 1 accepts
        self.client.get('/quick-login/driver')
        resp1 = self.client.post(f'/driver/accept/{order.id}', follow_redirects=False)
        self.assertIn(resp1.status_code, [302, 303])

        refreshed1 = Order.query.get(order.id)
        self.assertEqual(refreshed1.status, OrderStatus.DRIVER_ASSIGNED)
        self.assertEqual(refreshed1.driver_id, self.driver_user.id)
        self.assertIsNotNone(refreshed1.pickup_otp, "Pickup OTP must be generated upon assignment")

        # Driver 2 attempts to accept the same order
        # Switch to Driver 2
        with self.client.session_transaction() as sess:
            sess['_user_id'] = str(self.driver2_user.id)

        resp2 = self.client.post(f'/driver/accept/{order.id}', follow_redirects=True)
        self.assertIn(b'no longer available', resp2.data)

        # Verify driver_id remains Driver 1
        refreshed2 = Order.query.get(order.id)
        self.assertEqual(refreshed2.driver_id, self.driver_user.id)
        print("PASS: Second driver rejected; race condition prevented.")

    def test_05_otp_verification_limits_and_expiry(self):
        """Scenario 5: Incorrect, expired, or reused OTP verification."""
        print("\n--- Test 5: OTP Cryptographic Verification & Limits ---")
        pickup_code = "654321"
        pickup_hash = hash_otp(pickup_code)

        order = Order(
            customer_id=self.customer_user.id,
            farm_id=self.farm.id,
            customer_name='OTP Test',
            customer_address='Test Ave',
            customer_phone='1234567890',
            total_price=10.00,
            status=OrderStatus.DRIVER_ASSIGNED,
            driver_id=self.driver_user.id,
            pickup_otp=pickup_code,
            pickup_otp_hash=pickup_hash,
            pickup_otp_expires_at=datetime.datetime.now() + datetime.timedelta(hours=24),
            pickup_otp_attempts=0
        )
        order.save()

        self.client.get('/quick-login/farmer')

        # 1. Incorrect OTP submitted
        resp_wrong = self.client.post(f'/farmer/order/verify-pickup/{order.id}', data={
            'pickup_otp': '000000'
        }, follow_redirects=True)
        self.assertIn(b'Invalid Pickup OTP', resp_wrong.data)

        refreshed_wrong = Order.query.get(order.id)
        self.assertEqual(refreshed_wrong.pickup_otp_attempts, 1, "Failed attempts counter must increment")
        self.assertEqual(refreshed_wrong.status, OrderStatus.DRIVER_ASSIGNED, "Status must stay DRIVER_ASSIGNED")

        # 2. Correct OTP submitted
        resp_correct = self.client.post(f'/farmer/order/verify-pickup/{order.id}', data={
            'pickup_otp': pickup_code
        }, follow_redirects=True)
        self.assertIn(b'Pickup verified', resp_correct.data)

        refreshed_verified = Order.query.get(order.id)
        self.assertEqual(refreshed_verified.status, OrderStatus.PICKED_UP, "Status must transition to PICKED_UP")
        self.assertIsNone(refreshed_verified.pickup_otp, "Pickup OTP must be invalidated after verification")
        self.assertIsNotNone(refreshed_verified.delivery_otp, "Delivery OTP must be generated upon pickup")

        # 3. Delivery OTP verification by Driver
        delivery_code = refreshed_verified.delivery_otp
        self.client.get('/quick-login/driver')

        # Submit wrong delivery OTP
        resp_deliv_wrong = self.client.post(f'/driver/complete_delivery/{order.id}', data={
            'delivery_otp': '999999'
        }, follow_redirects=True)
        self.assertIn(b'Invalid Delivery OTP', resp_deliv_wrong.data)

        # Submit correct delivery OTP
        resp_deliv_ok = self.client.post(f'/driver/complete_delivery/{order.id}', data={
            'delivery_otp': delivery_code
        }, follow_redirects=True)
        self.assertIn(b'Delivered', resp_deliv_ok.data)

        refreshed_delivered = Order.query.get(order.id)
        self.assertEqual(refreshed_delivered.status, OrderStatus.DELIVERED)
        self.assertIsNone(refreshed_delivered.delivery_otp, "Delivery OTP must be wiped after delivery")
        print("PASS: Pickup and Delivery OTP verification and attempt limiting verified.")

    def test_06_cancellation_stock_restored_exactly_once(self):
        """Scenario 6: Cancellation request retried; stock restored exactly once."""
        print("\n--- Test 6: Single-Execution Stock Restoration on Cancellation ---")
        test_item = ProduceItem(
            name="Stock Restoration Test Melon",
            price=6.00,
            stock=10,
            unit="unit",
            farm_id=self.farm.id
        )
        test_item.save()

        # Place order reserving 3 units
        mongo_db['produce_items'].update_one({'_id': test_item.id}, {'$inc': {'stock': -3}})
        
        order = Order(
            customer_id=self.customer_user.id,
            farm_id=self.farm.id,
            customer_name='Cancel Buyer',
            customer_address='Cancel St',
            customer_phone='1112223333',
            total_price=18.00,
            status=OrderStatus.PLACED,
            stock_reserved=True,
            stock_restored=False
        )
        order.save()

        order_item = OrderItem(
            order_id=order.id,
            produce_item_id=test_item.id,
            quantity=3,
            price_per_item=6.00
        )
        order_item.save()

        # Check stock is 7 before cancellation
        doc1 = mongo_db['produce_items'].find_one({'_id': test_item.id})
        self.assertEqual(doc1['stock'], 7)

        # First cancellation call
        restored1 = restore_order_stock_once(order.id)
        self.assertTrue(restored1, "First restoration must succeed")

        doc2 = mongo_db['produce_items'].find_one({'_id': test_item.id})
        self.assertEqual(doc2['stock'], 10, "Stock should be restored to 10")

        # Retry cancellation call (simulating network retry / double webhook)
        restored2 = restore_order_stock_once(order.id)
        self.assertFalse(restored2, "Second restoration must return False (idempotent)")

        doc3 = mongo_db['produce_items'].find_one({'_id': test_item.id})
        self.assertEqual(doc3['stock'], 10, "Stock must still be 10, never double-restored")
        print("PASS: Stock restoration is atomic and strictly idempotent.")

    def test_07_full_lifecycle_timeline_persistence(self):
        """Scenario 7 & 8: Complete lifecycle transitions with timestamps and status history persistence."""
        print("\n--- Test 7 & 8: Order Lifecycle & Timeline Persistence ---")
        order = Order(
            customer_id=self.customer_user.id,
            farm_id=self.farm.id,
            customer_name='Lifecycle Tester',
            customer_address='Timeline Rd',
            customer_phone='4445556666',
            total_price=30.00,
            status=OrderStatus.PLACED
        )
        order.save()

        # 1. PLACED -> FARMER_ACCEPTED
        ok, err = transition_order_status(order, OrderStatus.FARMER_ACCEPTED, self.farmer_user)
        self.assertTrue(ok, err)
        self.assertEqual(order.status, OrderStatus.FARMER_ACCEPTED)

        # 2. FARMER_ACCEPTED -> PREPARING
        ok, err = transition_order_status(order, OrderStatus.PREPARING, self.farmer_user)
        self.assertTrue(ok, err)

        # 3. PREPARING -> READY_FOR_PICKUP
        ok, err = transition_order_status(order, OrderStatus.READY_FOR_PICKUP, self.farmer_user)
        self.assertTrue(ok, err)

        # 4. READY_FOR_PICKUP -> DRIVER_ASSIGNED
        ok, err = transition_order_status(order, OrderStatus.DRIVER_ASSIGNED, self.driver_user, extra_updates={'driver_id': self.driver_user.id})
        self.assertTrue(ok, err)
        self.assertIsNotNone(order.pickup_otp)

        # 5. DRIVER_ASSIGNED -> PICKED_UP
        ok, err = transition_order_status(order, OrderStatus.PICKED_UP, self.farmer_user)
        self.assertTrue(ok, err)
        self.assertIsNotNone(order.delivery_otp)

        # 6. PICKED_UP -> DELIVERED
        ok, err = transition_order_status(order, OrderStatus.DELIVERED, self.driver_user)
        self.assertTrue(ok, err)

        # Reload from MongoDB to verify persistence across reloads / reconnects
        persisted_order = Order.query.get(order.id)
        self.assertEqual(persisted_order.status, OrderStatus.DELIVERED)
        self.assertEqual(len(persisted_order.status_history), 7, "All 7 transitions must be recorded in status_history")
        
        # Verify timeline_steps helper
        steps = persisted_order.timeline_steps
        self.assertEqual(len(steps), 7)
        for s in steps:
            self.assertEqual(s['state'], 'completed', f"Step {s['label']} should be marked completed")
            self.assertIsNotNone(s['time'], f"Step {s['label']} should have a recorded timestamp")

        print("PASS: Full order lifecycle traversed, timeline milestones and timestamps persisted cleanly.")

if __name__ == '__main__':
    unittest.main()
