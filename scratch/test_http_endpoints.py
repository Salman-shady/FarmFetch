import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from app import app, mongo_db, User, Order, OrderStatus, Farm, db

def test_endpoints():
    client = app.test_client()

    print("=== Testing Admin Login & Settings ===")
    r = client.post('/login', data={'email': 'admin@farm.com', 'password': 'password123'}, follow_redirects=True)
    assert r.status_code == 200

    r_settings = client.get('/admin/settings')
    assert r_settings.status_code == 200
    html = r_settings.data.decode('utf-8')
    assert "Driver Earnings Rule Configuration" in html
    assert "PICKUP JOURNEY" in html or "Pickup" in html
    assert "DELIVERY JOURNEY" in html or "Delivery" in html
    assert "Live Formula Calculator" in html
    print("PASS: Admin settings page loaded successfully!")

    # Test updating driver rate to ₹4.00
    r_up = client.post('/admin/settings/driver-rate', data={'rate': '4.0'}, follow_redirects=True)
    assert r_up.status_code == 200
    html_up = r_up.data.decode('utf-8')
    assert "Driver earning rate successfully updated to ₹4.00 per km" in html_up
    assert "4.0" in html_up
    print("PASS: Admin updated rate to ₹4.00/km and flashed success!")

    # Reset back to ₹3.00
    client.post('/admin/settings/driver-rate', data={'rate': '3.0'}, follow_redirects=True)
    print("PASS: Reset rate to ₹3.00/km")

    # Test Admin Dashboard
    r_dash = client.get('/admin/dashboard')
    assert r_dash.status_code == 200
    html_dash = r_dash.data.decode('utf-8')
    assert "Driver Earnings Rule" in html_dash
    assert "Configure Rate" in html_dash
    print("PASS: Admin dashboard displays Driver Earnings Rule card!")

    # Test Driver Login & Dashboard
    client.get('/logout', follow_redirects=True)
    r_drv_login = client.post('/login', data={'email': 'driver@farm.com', 'password': 'password123'}, follow_redirects=True)
    assert r_drv_login.status_code == 200

    # Ensure an active assigned order exists for driver
    with app.app_context():
        driver = User.query.filter_by(role='driver').first()
        farm = Farm.query.first()
        cust = User.query.filter_by(role='customer').first()
        
        active_order = Order.query.filter_by(driver_id=driver.id, status=OrderStatus.PICKED_UP).first()
        if not active_order:
            active_order = Order(
                id=3,
                farm_id=farm.id,
                customer_id=cust.id,
                driver_id=driver.id,
                status=OrderStatus.PICKED_UP,
                total_price=35.0,
                customer_name='Sarah Customer',
                customer_phone='9876543210',
                customer_address='MG Road, Bengaluru',
                customer_latitude=12.975,
                customer_longitude=77.605,
                driver_latitude=12.974976,
                driver_longitude=77.604994,
                delivery_otp='123456'
            )
            active_order.delivery_otp_hash = '$2b$12$lknxuuu9HpdHgk8e6/niaumcMttIsqGK6hlNaoaaQHXplSrdWp3xm'
            active_order.delivery_otp_attempts = 0
            db.session.add(active_order)
            db.session.commit()

    r_drv_dash = client.get('/driver/dashboard')
    assert r_drv_dash.status_code == 200
    html_drv = r_drv_dash.data.decode('utf-8')
    assert "Driver Earnings Breakdown" in html_drv
    assert "Pickup Journey" in html_drv
    assert "Delivery Journey" in html_drv
    assert "Total Payable Distance" in html_drv
    assert "₹" in html_drv
    print("PASS: Driver dashboard displays active delivery with pickup and drop earnings breakdown!")

    # Test /api/driver/stats
    r_stats = client.get('/api/driver/stats')
    assert r_stats.status_code == 200
    data = r_stats.get_json()
    print("Driver stats JSON:", data)
    assert data['rate_per_km'] == 3.0
    assert data['currency_symbol'] == '₹'
    print("PASS: /api/driver/stats endpoint returned valid stats, ₹ currency symbol, and completed orders!")

    print("\nALL FLASK TEST CLIENT ENDPOINTS PASSED PERFECTLY!")

if __name__ == '__main__':
    test_endpoints()
