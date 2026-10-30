import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from app import app, mongo_db, User, Order, OrderStatus, Farm, db, hash_otp

def test():
    with app.app_context():
        # Ensure order 3 has valid status and OTP hash
        otp = '654321'
        h = hash_otp(otp)
        mongo_db['orders'].update_one(
            {'_id': 3},
            {
                '$set': {
                    'status': OrderStatus.PICKED_UP,
                    'delivery_otp': otp,
                    'delivery_otp_hash': h,
                    'delivery_otp_attempts': 0,
                    'driver_payout_finalized': False,
                    'driver_payout_amount': None
                }
            }
        )

    client = app.test_client()
    client.post('/login', data={'email': 'driver@farm.com', 'password': 'password123'}, follow_redirects=True)
    r = client.post('/driver/complete_delivery/3', data={'delivery_otp': otp}, follow_redirects=True)
    html = r.data.decode('utf-8')

    print("Status code:", r.status_code)
    assert "Driver Payout Finalized" in html, "Flash message should announce Driver Payout Finalized"
    for line in html.split('\n'):
        if "Driver Payout Finalized" in line:
            print("Banner:", line.strip())

    with app.app_context():
        order3 = Order.query.get(3)
        driver = User.query.get(4)
        print(f"Order #3 status: {order3.status}")
        assert order3.status == OrderStatus.DELIVERED, "Order must be DELIVERED"
        assert order3.driver_payout_finalized is True, "Payout must be finalized"
        print(f"Payout amount: ₹{order3.driver_payout_amount:.2f}")
        print(f"Pickup: {order3.driver_pickup_distance_km} km -> ₹{order3.driver_pickup_earning:.2f}")
        print(f"Drop: {order3.driver_drop_distance_km} km -> ₹{order3.driver_drop_earning:.2f}")
        print(f"Driver total earnings: ₹{driver.total_earnings:.2f}")

    print("\nDELIVERY COMPLETION TEST PASSED 100%!")

if __name__ == '__main__':
    test()
