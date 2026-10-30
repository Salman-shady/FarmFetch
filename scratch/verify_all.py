import urllib.request
import urllib.parse
import http.cookiejar
import re
import sys
sys.stdout.reconfigure(encoding='utf-8')

BASE_URL = 'http://localhost:5000'

cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

def post(url, data):
    encoded = urllib.parse.urlencode(data).encode('utf-8')
    req = urllib.request.Request(url, data=encoded)
    return opener.open(req)

def get(url):
    req = urllib.request.Request(url)
    return opener.open(req)

def test_flow():
    # 1. Login as farmer
    res = post(f'{BASE_URL}/login', {
        'email': 'farmer@farm.com',
        'password': 'password123'
    })
    html = res.read().decode('utf-8')
    assert res.status == 200
    print("[PASS] Logged in as farmer@farm.com")

    # 2. Check /farmer/produce
    res = get(f'{BASE_URL}/farmer/produce')
    assert res.status == 200
    html = res.read().decode('utf-8')
    assert 'Price (₹)' in html, "Price (₹) not found in manage_produce.html"
    assert 'Unit of Measure' in html, "Unit of Measure not found"
    assert 'chip-unit-select' in html, "Quick unit chips not found"
    assert 'unit-auto-badge' in html, "Unit auto badge not found"
    print("[PASS] manage_produce.html renders Price (₹), Unit with auto-badge and quick chips")

    # 3. Add produce item: Fresh Cow Milk with empty unit
    res = post(f'{BASE_URL}/farmer/produce', {
        'name': 'Test Fresh Cow Milk',
        'category': 'Dairy',
        'price': '65.00',
        'unit': '',  # Test auto-infer
        'stock': '30',
        'description': 'Pure fresh dairy milk',
        'image_url': 'https://images.unsplash.com/photo-1550583724-b2692b85b150?w=500'
    })
    html = res.read().decode('utf-8')
    assert res.status == 200
    assert 'Produce item added successfully!' in html
    print("[PASS] Produce item 'Test Fresh Cow Milk' added")

    # Verify in HTML that it received 'per liter'
    res = get(f'{BASE_URL}/farmer/produce')
    html = res.read().decode('utf-8')
    assert 'Test Fresh Cow Milk' in html
    assert 'per liter' in html
    assert '₹65.00' in html
    print("[PASS] Milk item auto-inferred unit: 'per liter' and price '₹65.00' confirmed!")

    # 4. Check farmer dashboard for ₹
    res = get(f'{BASE_URL}/farmer/dashboard')
    html = res.read().decode('utf-8')
    assert 'Revenue (₹)' in html
    assert 'bi-currency-rupee' in html
    print("[PASS] farmer_dashboard.html renders Revenue (₹) and bi-currency-rupee")

    # 5. Customer Dashboard
    get(f'{BASE_URL}/logout')
    res = post(f'{BASE_URL}/login', {
        'email': 'customer@farm.com',
        'password': 'password123'
    })
    html = res.read().decode('utf-8')
    res = get(f'{BASE_URL}/customer/dashboard')
    html = res.read().decode('utf-8')
    assert '₹' in html
    print("[PASS] customer_dashboard.html renders in ₹")

    # 6. Admin Dashboard
    get(f'{BASE_URL}/logout')
    res = post(f'{BASE_URL}/login', {
        'email': 'admin@farm.com',
        'password': 'password123'
    })
    html = res.read().decode('utf-8')
    res = get(f'{BASE_URL}/admin/dashboard')
    html = res.read().decode('utf-8')
    assert 'bi-currency-rupee' in html
    assert 'Gross Order Value' in html
    print("[PASS] admin/dashboard.html renders in ₹ with bi-currency-rupee")

    print("\n===========================================")
    print(" ALL VERIFICATION CHECKS PASSED PERFECTLY! ")
    print("===========================================")

if __name__ == '__main__':
    test_flow()
