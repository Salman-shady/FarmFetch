import unittest
import time
from app import app, socketio, mongo_db, User, Order, Farm, ProduceItem, OrderStatus, db

class SimulatorBackendTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        app.config['TESTING'] = True
        app.config['WTF_CSRF_ENABLED'] = False
        cls.app = app
        cls.db = mongo_db

    def setUp(self):
        with app.app_context():
            mongo_db['users'].delete_many({'email': {'$regex': '^test_sim_'}})
            mongo_db['farms'].delete_many({'name': {'$regex': '^Test Sim'}})
            mongo_db['orders'].delete_many({'_id': {'$gte': 77000}})

            self.driver = User(
                email='test_sim_driver@farm.com',
                role='driver',
                name='Sim Driver',
                driver_verification_status='APPROVED'
            )
            self.driver.set_password('pass123')
            db.session.add(self.driver)
            db.session.commit()
            self.driver_id = self.driver.id

            self.customer = User(
                email='test_sim_cust@farm.com',
                role='customer',
                name='Sim Customer'
            )
            self.customer.set_password('pass123')
            db.session.add(self.customer)
            db.session.commit()
            self.customer_id = self.customer.id

            self.farm = Farm(
                name='Test Sim Farm',
                address='123 Farm Way',
                farm_type='Vegetables',
                user_id=self.driver_id,
                latitude=12.9716,
                longitude=77.5946
            )
            db.session.add(self.farm)
            db.session.commit()

            self.order_id = 77001
            order_doc = {
                '_id': self.order_id,
                'customer_id': self.customer_id,
                'customer_name': 'Sim Customer',
                'customer_address': '456 Dropoff Rd',
                'customer_phone': '9876543210',
                'customer_latitude': 12.9800,
                'customer_longitude': 77.6000,
                'farm_id': self.farm.id,
                'driver_id': self.driver_id,
                'driver_name': 'Sim Driver',
                'total_price': 25.0,
                'status': OrderStatus.PICKED_UP,
                'created_at': time.time()
            }
            mongo_db['orders'].insert_one(order_doc)

    def tearDown(self):
        with app.app_context():
            mongo_db['users'].delete_many({'email': {'$regex': '^test_sim_'}})
            mongo_db['farms'].delete_many({'name': {'$regex': '^Test Sim'}})
            mongo_db['orders'].delete_many({'_id': {'$gte': 77000}})

    def test_simulated_gps_updates(self):
        # 1. Connect customer client to order room
        flask_cust = app.test_client()
        with flask_cust.session_transaction() as sess:
            sess['_user_id'] = str(self.customer_id)
            sess['_fresh'] = True
        cust_client = socketio.test_client(app, flask_test_client=flask_cust)
        cust_client.emit('join_order_room', {'order_id': self.order_id})
        _ = cust_client.get_received()

        # 2. Connect driver client
        flask_drv = app.test_client()
        with flask_drv.session_transaction() as sess:
            sess['_user_id'] = str(self.driver_id)
            sess['_fresh'] = True
        drv_client = socketio.test_client(app, flask_test_client=flask_drv)
        drv_client.emit('join_order_room', {'order_id': self.order_id})

        # 3. Emit simulated coordinates
        sim_lat, sim_lng = 12.9750, 77.5980
        drv_client.emit('driver_location_update', {
            'order_id': self.order_id,
            'lat': sim_lat,
            'lng': sim_lng,
            'is_simulated': True
        })

        # 4. Verify customer receives broadcast with is_simulated flag
        received = cust_client.get_received()
        updates = [r for r in received if r['name'] == 'customer_location_update']
        self.assertEqual(len(updates), 1, "Customer should receive 1 customer_location_update")
        payload = updates[0]['args'][0]
        self.assertEqual(payload['order_id'], self.order_id)
        self.assertAlmostEqual(payload['lat'], sim_lat)
        self.assertAlmostEqual(payload['lng'], sim_lng)
        self.assertTrue(payload.get('is_simulated'), "Payload should include is_simulated=True")

        # 5. Check order updated in DB
        order = mongo_db['orders'].find_one({'_id': self.order_id})
        self.assertAlmostEqual(order['driver_latitude'], sim_lat)
        self.assertAlmostEqual(order['driver_longitude'], sim_lng)

        drv_client.disconnect()
        cust_client.disconnect()

if __name__ == '__main__':
    unittest.main()
