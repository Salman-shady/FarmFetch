import unittest
import time
import json
from unittest.mock import patch, MagicMock
from app import (
    app, socketio, mongo_db, User, Order, Farm, ProduceItem,
    OrderStatus, DriverAvailability, get_road_route,
    get_recommended_drivers_for_order, ROUTE_CACHE, hash_otp
)

class DeliveryTrackingAndDriverManagementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        app.config['TESTING'] = True
        app.config['WTF_CSRF_ENABLED'] = False
        cls.flask_client = app.test_client()

        # Seed clean test entities
        cls.now = time.time()
        
        # 1. Farmer & Farm
        cls.farmer_email = "test_farmer_track@farm.com"
        cls.farmer_id = 9101
        mongo_db.users.delete_many({'email': cls.farmer_email})
        mongo_db.users.insert_one({
            '_id': cls.farmer_id,
            'name': 'Farmer Track',
            'email': cls.farmer_email,
            'password': 'hashed_password',
            'role': 'farmer',
            'phone': '9876543210'
        })

        cls.farm_id = 8101
        mongo_db.farms.delete_many({'_id': cls.farm_id})
        mongo_db.farms.insert_one({
            '_id': cls.farm_id,
            'user_id': cls.farmer_id,
            'farmer_id': cls.farmer_id,
            'name': 'Green Track Farm',
            'address': 'Rural Way 1, Bangalore',
            'latitude': 12.9716,
            'longitude': 77.5946
        })

        # 2. Customer
        cls.customer_email = "test_customer_track@farm.com"
        cls.customer_id = 9102
        mongo_db.users.delete_many({'email': cls.customer_email})
        mongo_db.users.insert_one({
            '_id': cls.customer_id,
            'name': 'Customer Track',
            'email': cls.customer_email,
            'password': 'hashed_password',
            'role': 'customer',
            'phone': '9876543211',
            'address': 'MG Road 10, Bangalore',
            'latitude': 12.9750,
            'longitude': 77.6050
        })

        # 3. Driver 1 (Nearby & Available)
        cls.driver1_email = "test_driver1_track@farm.com"
        cls.driver1_id = 9103
        mongo_db.users.delete_many({'email': cls.driver1_email})
        mongo_db.users.insert_one({
            '_id': cls.driver1_id,
            'name': 'Driver One',
            'email': cls.driver1_email,
            'password': 'hashed_password',
            'role': 'driver',
            'phone': '9876543212',
            'availability': DriverAvailability.AVAILABLE,
            'last_latitude': 12.9720,
            'last_longitude': 77.5950,
            'last_location_update': cls.now
        })

        # 4. Driver 2 (Further / Busy)
        cls.driver2_email = "test_driver2_track@farm.com"
        cls.driver2_id = 9104
        mongo_db.users.delete_many({'email': cls.driver2_email})
        mongo_db.users.insert_one({
            '_id': cls.driver2_id,
            'name': 'Driver Two',
            'email': cls.driver2_email,
            'password': 'hashed_password',
            'role': 'driver',
            'phone': '9876543213',
            'availability': DriverAvailability.AVAILABLE,
            'last_latitude': 13.0827,
            'last_longitude': 80.2707,
            'last_location_update': cls.now
        })

        # 5. Unrelated User (Spy / Attacker)
        cls.unrelated_email = "test_unrelated@farm.com"
        cls.unrelated_id = 9105
        mongo_db.users.delete_many({'email': cls.unrelated_email})
        mongo_db.users.insert_one({
            '_id': cls.unrelated_id,
            'name': 'Unrelated User',
            'email': cls.unrelated_email,
            'password': 'hashed_password',
            'role': 'customer',
            'phone': '9876543299'
        })

        # Produce Item
        cls.item_id = 7101
        mongo_db.produce.delete_many({'_id': cls.item_id})
        mongo_db.produce.insert_one({
            '_id': cls.item_id,
            'farm_id': cls.farm_id,
            'name': 'Organic Apples',
            'price': 10.0,
            'quantity': 100,
            'unit': 'kg'
        })

    def setUp(self):
        # Create fresh test order
        self.order_id = 55001
        mongo_db.orders.delete_many({'_id': self.order_id})
        mongo_db.orders.insert_one({
            '_id': self.order_id,
            'customer_id': self.customer_id,
            'farm_id': self.farm_id,
            'status': OrderStatus.READY_FOR_PICKUP,
            'driver_id': None,
            'total_price': 50.0,
            'customer_name': 'Customer Track',
            'customer_address': 'MG Road 10, Bangalore',
            'customer_phone': '9876543211',
            'customer_latitude': 12.9750,
            'customer_longitude': 77.6050,
            'items': [{'item_id': self.item_id, 'quantity': 5, 'price_per_item': 10.0}],
            'pickup_otp': '123456',
            'delivery_otp': '654321',
            'timeline': [],
            'created_at': time.time()
        })

    # ---------------------------------------------------------
    # TEST 1: Driver grants GPS permission & broadcasts location
    # ---------------------------------------------------------
    def test_01_driver_grants_gps_and_updates_location(self):
        # Assign driver1 to order in PICKED_UP status
        mongo_db.orders.update_one(
            {'_id': self.order_id},
            {'$set': {'driver_id': self.driver1_id, 'status': OrderStatus.PICKED_UP}}
        )

        flask_drv = app.test_client()
        with flask_drv.session_transaction() as sess:
            sess['_user_id'] = str(self.driver1_id)
            sess['_fresh'] = True

        driver_client = socketio.test_client(app, flask_test_client=flask_drv)
        driver_client.emit('join_order_room', {'order_id': self.order_id})

        # Emit valid GPS coords
        test_lat = 12.9725
        test_lng = 77.5960
        driver_client.emit('driver_location_update', {
            'order_id': self.order_id,
            'lat': test_lat,
            'lng': test_lng
        })

        # Verify DB is updated
        updated_order = mongo_db.orders.find_one({'_id': self.order_id})
        self.assertAlmostEqual(updated_order['driver_latitude'], test_lat, places=4)
        self.assertAlmostEqual(updated_order['driver_longitude'], test_lng, places=4)
        self.assertTrue(updated_order.get('last_location_update') or updated_order.get('last_location_at'))

        # Verify driver user record updated
        driver_doc = mongo_db.users.find_one({'_id': self.driver1_id})
        self.assertAlmostEqual(driver_doc['last_latitude'], test_lat, places=4)
        self.assertAlmostEqual(driver_doc['last_longitude'], test_lng, places=4)
        driver_client.disconnect()

    # ---------------------------------------------------------
    # TEST 2: Driver denies GPS permission or invalid coordinates
    # ---------------------------------------------------------
    def test_02_driver_denies_gps_or_invalid_coords(self):
        mongo_db.orders.update_one(
            {'_id': self.order_id},
            {'$set': {'driver_id': self.driver1_id, 'status': OrderStatus.PICKED_UP}}
        )

        flask_drv = app.test_client()
        with flask_drv.session_transaction() as sess:
            sess['_user_id'] = str(self.driver1_id)
            sess['_fresh'] = True

        driver_client = socketio.test_client(app, flask_test_client=flask_drv)
        driver_client.emit('join_order_room', {'order_id': self.order_id})

        # Case A: Out of bound coordinates (> 90 lat)
        driver_client.emit('driver_location_update', {
            'order_id': self.order_id,
            'lat': 999.0,
            'lng': 77.5960
        })
        received = driver_client.get_received()
        error_events = [r for r in received if r['name'] == 'error']
        self.assertTrue(len(error_events) > 0)
        self.assertIn("Invalid coordinates range", error_events[0]['args'][0]['message'])

        # Case B: Null/non-numeric coordinates (simulating permission error or missing hardware)
        driver_client.emit('driver_location_update', {
            'order_id': self.order_id,
            'lat': 'null',
            'lng': None
        })
        received2 = driver_client.get_received()
        error_events2 = [r for r in received2 if r['name'] == 'error']
        self.assertTrue(len(error_events2) > 0)
        driver_client.disconnect()

    # ---------------------------------------------------------
    # TEST 3: Customer receives authorized live location updates
    # ---------------------------------------------------------
    def test_03_customer_receives_authorized_location_update(self):
        mongo_db.orders.update_one(
            {'_id': self.order_id},
            {'$set': {'driver_id': self.driver1_id, 'status': OrderStatus.PICKED_UP}}
        )

        # 1. Customer connects with isolated session
        flask_cust = app.test_client()
        with flask_cust.session_transaction() as sess:
            sess['_user_id'] = str(self.customer_id)
            sess['_fresh'] = True
        customer_client = socketio.test_client(app, flask_test_client=flask_cust)
        customer_client.emit('join_order_room', {'order_id': self.order_id})
        _ = customer_client.get_received() # Clear join ack

        # 2. Driver connects with isolated session
        flask_drv = app.test_client()
        with flask_drv.session_transaction() as sess:
            sess['_user_id'] = str(self.driver1_id)
            sess['_fresh'] = True
        driver_client = socketio.test_client(app, flask_test_client=flask_drv)
        driver_client.emit('join_order_room', {'order_id': self.order_id})
        
        test_lat = 12.9730
        test_lng = 77.5980
        driver_client.emit('driver_location_update', {
            'order_id': self.order_id,
            'lat': test_lat,
            'lng': test_lng
        })

        # 3. Customer must receive 'customer_location_update'
        customer_received = customer_client.get_received()
        loc_updates = [r for r in customer_received if r['name'] == 'customer_location_update']
        self.assertTrue(len(loc_updates) > 0, "Customer should receive live location broadcast")
        data = loc_updates[0]['args'][0]
        self.assertAlmostEqual(data['lat'], test_lat, places=4)
        self.assertAlmostEqual(data['lng'], test_lng, places=4)
        self.assertEqual(data['order_id'], self.order_id)

        customer_client.disconnect()
        driver_client.disconnect()

    # ---------------------------------------------------------
    # TEST 4: Unrelated user cannot subscribe to order room
    # ---------------------------------------------------------
    def test_04_unrelated_user_rejected_from_tracking_room(self):
        flask_unrelated = app.test_client()
        with flask_unrelated.session_transaction() as sess:
            sess['_user_id'] = str(self.unrelated_id)
            sess['_fresh'] = True
        spy_client = socketio.test_client(app, flask_test_client=flask_unrelated)
        spy_client.emit('join_order_room', {'order_id': self.order_id})

        received = spy_client.get_received()
        error_events = [r for r in received if r['name'] == 'error']
        self.assertTrue(len(error_events) > 0)
        self.assertIn("Unauthorized", error_events[0]['args'][0]['message'])
        spy_client.disconnect()

    # ---------------------------------------------------------
    # TEST 5: Two drivers attempt to accept the same delivery
    # ---------------------------------------------------------
    def test_05_two_drivers_accept_delivery_concurrency_cas(self):
        # Driver 1 accepts
        flask_drv1 = app.test_client()
        with flask_drv1.session_transaction() as sess:
            sess['_user_id'] = str(self.driver1_id)
            sess['_fresh'] = True
        resp1 = flask_drv1.post(f'/driver/accept/{self.order_id}', headers={'Content-Type': 'application/json'}, json={})
        self.assertEqual(resp1.status_code, 200)
        json_data1 = resp1.get_json()
        self.assertTrue(json_data1['success'])

        # Driver 2 tries to accept the same order
        flask_drv2 = app.test_client()
        with flask_drv2.session_transaction() as sess:
            sess['_user_id'] = str(self.driver2_id)
            sess['_fresh'] = True
        resp2 = flask_drv2.post(f'/driver/accept/{self.order_id}', headers={'Content-Type': 'application/json'}, json={})
        self.assertEqual(resp2.status_code, 400)
        json_data2 = resp2.get_json()
        self.assertFalse(json_data2['success'])
        self.assertIn("already assigned", json_data2['message'].lower())

        # Ensure order still strictly assigned to driver 1
        order = mongo_db.orders.find_one({'_id': self.order_id})
        self.assertEqual(order['driver_id'], self.driver1_id)
        self.assertEqual(order['status'], OrderStatus.DRIVER_ASSIGNED)

    # ---------------------------------------------------------
    # TEST 6: Driver/Customer disconnects and reconnects safely
    # ---------------------------------------------------------
    def test_06_reconnect_recovers_last_authorized_location(self):
        last_time = time.time() - 10 # 10s ago
        mongo_db.orders.update_one(
            {'_id': self.order_id},
            {'$set': {
                'driver_id': self.driver1_id,
                'status': OrderStatus.PICKED_UP,
                'driver_latitude': 12.9733,
                'driver_longitude': 77.5977,
                'last_location_update': last_time
            }}
        )

        flask_cust = app.test_client()
        with flask_cust.session_transaction() as sess:
            sess['_user_id'] = str(self.customer_id)
            sess['_fresh'] = True

        client = socketio.test_client(app, flask_test_client=flask_cust)
        client.emit('join_order_room', {'order_id': self.order_id})
        _ = client.get_received()

        # Emit reconnect sync request
        client.emit('request_order_location', {'order_id': self.order_id})
        received = client.get_received()
        sync_events = [r for r in received if r['name'] in ('order_location_sync', 'initial_location_state')]
        self.assertTrue(len(sync_events) > 0, "Must return last authorized location sync")
        data = sync_events[0]['args'][0]
        lat = data.get('lat') if data.get('lat') is not None else data.get('driver_latitude')
        lng = data.get('lng') if data.get('lng') is not None else data.get('driver_longitude')
        self.assertAlmostEqual(lat, 12.9733, places=4)
        self.assertAlmostEqual(lng, 77.5977, places=4)
        self.assertFalse(data['is_stale'])
        client.disconnect()

    # ---------------------------------------------------------
    # TEST 7: Routing service failure gracefully degrades (No fake speed!)
    # ---------------------------------------------------------
    def test_07_routing_service_failure_graceful_degradation(self):
        # Test get_road_route when service cannot be reached
        with patch('urllib.request.urlopen', side_effect=Exception("OSRM Connection refused")):
            ROUTE_CACHE.clear()
            res = get_road_route(12.9716, 77.5946, 12.9750, 77.6050)
            self.assertFalse(res['is_road_route'])
            self.assertIsNone(res.get('duration_secs'))
            self.assertIsNone(res.get('eta_mins'))
            self.assertIn("Road ETA unavailable", res['eta_text'])
            # Verify straight line points returned
            self.assertEqual(len(res['geometry']), 2)
            self.assertAlmostEqual(res['geometry'][0][0], 12.9716)
            self.assertAlmostEqual(res['geometry'][1][0], 12.9750)

    # ---------------------------------------------------------
    # TEST 8: Order completed and tracking stops
    # ---------------------------------------------------------
    def test_08_order_completed_and_tracking_stops(self):
        # Set order as DELIVERED
        mongo_db.orders.update_one(
            {'_id': self.order_id},
            {'$set': {
                'driver_id': self.driver1_id,
                'status': OrderStatus.DELIVERED
            }}
        )

        flask_drv = app.test_client()
        with flask_drv.session_transaction() as sess:
            sess['_user_id'] = str(self.driver1_id)
            sess['_fresh'] = True

        driver_client = socketio.test_client(app, flask_test_client=flask_drv)
        driver_client.emit('join_order_room', {'order_id': self.order_id})

        # Attempt to stream coordinates while order is DELIVERED
        driver_client.emit('driver_location_update', {
            'order_id': self.order_id,
            'lat': 12.9800,
            'lng': 77.6000
        })

        received = driver_client.get_received()
        error_events = [r for r in received if r['name'] == 'error' or r['name'] == 'tracking_stopped']
        self.assertTrue(len(error_events) > 0)
        driver_client.disconnect()

    # ---------------------------------------------------------
    # TEST 9: Existing chat, OTP verification and status flow still work
    # ---------------------------------------------------------
    def test_09_existing_chat_otp_and_status_verification(self):
        # 1. Chat between customer and farmer
        flask_cust = app.test_client()
        with flask_cust.session_transaction() as sess:
            sess['_user_id'] = str(self.customer_id)
            sess['_fresh'] = True
        cust_client = socketio.test_client(app, flask_test_client=flask_cust)
        cust_client.emit('join_order_room', {'order_id': self.order_id})

        flask_farmer = app.test_client()
        with flask_farmer.session_transaction() as sess:
            sess['_user_id'] = str(self.farmer_id)
            sess['_fresh'] = True
        farmer_client = socketio.test_client(app, flask_test_client=flask_farmer)
        farmer_client.emit('join_order_room', {'order_id': self.order_id})
        _ = farmer_client.get_received()

        cust_client.emit('send_chat_message', {
            'order_id': self.order_id,
            'message': 'Hello farmer, is the produce packed?'
        })

        farmer_received = farmer_client.get_received()
        chat_events = [r for r in farmer_received if r['name'] == 'new_chat_message']
        self.assertTrue(len(chat_events) > 0)
        self.assertEqual(chat_events[0]['args'][0]['message'], 'Hello farmer, is the produce packed?')

        cust_client.disconnect()
        farmer_client.disconnect()

        # 2. Farmer OTP pickup verification
        p_otp = '789012'
        p_hash = hash_otp(p_otp)
        mongo_db.orders.update_one(
            {'_id': self.order_id},
            {'$set': {
                'driver_id': self.driver1_id,
                'status': OrderStatus.DRIVER_ASSIGNED,
                'pickup_otp': p_otp,
                'pickup_otp_hash': p_hash,
                'pickup_otp_attempts': 0
            }}
        )

        resp = flask_farmer.post(
            f'/farmer/order/verify-pickup/{self.order_id}',
            data={'pickup_otp': '789012'},
            follow_redirects=True
        )
        self.assertEqual(resp.status_code, 200)
        order_after_pickup = mongo_db.orders.find_one({'_id': self.order_id})
        self.assertEqual(order_after_pickup['status'], OrderStatus.PICKED_UP)
        self.assertIsNotNone(order_after_pickup['delivery_otp'])

    # ---------------------------------------------------------
    # TEST 10: Driver Availability Management & Validation
    # ---------------------------------------------------------
    def test_10_driver_availability_validation(self):
        flask_drv = app.test_client()
        with flask_drv.session_transaction() as sess:
            sess['_user_id'] = str(self.driver1_id)
            sess['_fresh'] = True

        # Change to OFFLINE
        resp = flask_drv.post('/driver/availability', json={'availability': 'OFFLINE'})
        self.assertEqual(resp.status_code, 200)
        user = User.get_by_id(self.driver1_id)
        self.assertEqual(user.availability, DriverAvailability.OFFLINE)

        # Give driver an active order
        mongo_db.orders.update_one(
            {'_id': self.order_id},
            {'$set': {'driver_id': self.driver1_id, 'status': OrderStatus.PICKED_UP}}
        )

        # Attempt to set to AVAILABLE while carrying active order
        resp2 = flask_drv.post('/driver/availability', json={'availability': 'AVAILABLE'})
        self.assertEqual(resp2.status_code, 400)
        self.assertIn("active delivery", resp2.get_json()['message'].lower())

    # ---------------------------------------------------------
    # TEST 11: Smart Driver Recommendations Ranking
    # ---------------------------------------------------------
    def test_11_smart_driver_recommendation_ranking(self):
        # Driver 1 is near the farm (12.9720, 77.5950 vs farm 12.9716, 77.5946)
        # Driver 2 is in Chennai (13.0827, 80.2707)
        mongo_db.users.update_one({'_id': self.driver1_id}, {'$set': {'availability': DriverAvailability.AVAILABLE}})
        mongo_db.users.update_one({'_id': self.driver2_id}, {'$set': {'availability': DriverAvailability.AVAILABLE}})
        mongo_db.orders.update_one({'_id': self.order_id}, {'$set': {'driver_id': None, 'status': OrderStatus.READY_FOR_PICKUP}})

        order = Order.get_by_id(self.order_id)
        recs = get_recommended_drivers_for_order(order)
        self.assertTrue(len(recs) >= 2)
        # First recommended must be Driver 1 (closer to farm)
        self.assertEqual(recs[0]['driver_id'], self.driver1_id)
        rec2 = next(r for r in recs if r['driver_id'] == self.driver2_id)
        self.assertTrue(recs[0]['distance_km'] < rec2['distance_km'])
        self.assertTrue(recs[0]['score'] < rec2['score'])
        self.assertTrue("nearest" in recs[0]['reason'].lower() or "available" in recs[0]['reason'].lower())

if __name__ == '__main__':
    unittest.main()
