import sys
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')
import os
os.environ['TESTING'] = 'True'
from app import app, mongo_db

client = app.test_client()

def run_tests():
    print("Testing with Flask test_client...")
    
    # 1. Login as Farmer
    resp = client.post('/login', data={'email': 'farmer@farm.com', 'password': 'password123'}, follow_redirects=True)
    assert resp.status_code == 200, "Farmer login failed"
    print("[PASS] Logged in as farmer@farm.com")

    # 2. Check /farmer/produce
    resp = client.get('/farmer/produce')
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert 'Price (₹)' in html, "Price (₹) not found"
    assert 'Unit of Measure' in html, "Unit of Measure not found"
    assert 'chip-unit-select' in html, "Quick unit chips not found"
    assert 'unit-auto-badge' in html, "unit-auto-badge not found"
    print("[PASS] /farmer/produce renders Price (₹), Unit of Measure, auto-badge, and quick chips")

    # 3. Add produce item: Milk without entering unit -> should auto-infer 'per liter'
    resp = client.post('/farmer/produce', data={
        'name': 'Fresh Buffalo Milk',
        'category': 'Dairy',
        'price': '70.00',
        'unit': '',
        'stock': '40',
        'description': 'Rich fresh milk',
        'image_url': ''
    }, follow_redirects=True)
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert 'Fresh Buffalo Milk' in html
    assert 'per liter' in html
    assert '₹70.00' in html
    print("[PASS] 'Fresh Buffalo Milk' automatically assigned unit: 'per liter' and price '₹70.00'")

    # 4. Add produce item: Organic Spinach without entering unit -> should auto-infer 'per bunch'
    resp = client.post('/farmer/produce', data={
        'name': 'Organic Baby Spinach',
        'category': 'Vegetables',
        'price': '25.00',
        'unit': '',
        'stock': '50',
        'description': 'Crisp fresh spinach',
        'image_url': ''
    }, follow_redirects=True)
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert 'Organic Baby Spinach' in html
    assert 'per bunch' in html
    assert '₹25.00' in html
    print("[PASS] 'Organic Baby Spinach' automatically assigned unit: 'per bunch' and price '₹25.00'")

    # 5. Add produce item: Tomatoes without entering unit -> should auto-infer 'per kg'
    resp = client.post('/farmer/produce', data={
        'name': 'Farm Fresh Tomatoes',
        'category': 'Vegetables',
        'price': '40.00',
        'unit': '',
        'stock': '60',
        'description': 'Juicy red tomatoes',
        'image_url': ''
    }, follow_redirects=True)
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert 'Farm Fresh Tomatoes' in html
    assert 'per kg' in html
    assert '₹40.00' in html
    print("[PASS] 'Farm Fresh Tomatoes' automatically assigned unit: 'per kg' and price '₹40.00'")

    # 6. Check Farmer Dashboard
    resp = client.get('/farmer/dashboard')
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert 'Revenue (₹)' in html
    assert 'bi-currency-rupee' in html
    print("[PASS] /farmer/dashboard renders Revenue (₹) and rupee icon")

    # 7. Customer Dashboard
    client.get('/logout')
    client.post('/login', data={'email': 'customer@farm.com', 'password': 'password123'}, follow_redirects=True)
    resp = client.get('/customer/dashboard')
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert '₹' in html
    print("[PASS] /customer/dashboard renders ₹ for order total and stats")

    # 8. Admin Dashboard
    client.get('/logout')
    client.post('/login', data={'email': 'admin@farm.com', 'password': 'password123'}, follow_redirects=True)
    resp = client.get('/admin/dashboard')
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert 'bi-currency-rupee' in html
    assert 'Gross Order Value' in html
    print("[PASS] /admin/dashboard renders ₹ and bi-currency-rupee")

    print("\n=======================================================")
    print(" ALL TESTS AND VERIFICATIONS COMPLETED SUCCESSFULLY! ")
    print("=======================================================")

if __name__ == '__main__':
    run_tests()
