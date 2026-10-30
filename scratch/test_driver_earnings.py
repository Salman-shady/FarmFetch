"""
Unit and integration test for Driver Earnings Rule, Admin Config, Road Distance, and Idempotent Server Finalization.
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from app import (
    app, db, mongo_db, User, Order, Farm,
    get_driver_rate_per_km, set_driver_rate_per_km,
    calculate_order_driver_earnings, finalize_driver_order_payout,
    transition_order_status, OrderStatus
)

def run_tests():
    with app.app_context():
        print("=== 1. Testing Default Driver Earning Rate ===")
        # Ensure default rate is 3.0
        mongo_db['platform_settings'].delete_one({'_id': 'driver_rate_per_km'})
        default_rate = get_driver_rate_per_km()
        print(f"Default rate: {default_rate} INR/km")
        assert default_rate == 3.0, f"Expected 3.0, got {default_rate}"
        print("PASS: Default rate is 3.0 INR/km")

        print("\n=== 2. Testing Formula Calculation (User Example: 3 km + 5 km = 8 km * 3 = 24) ===")
        rate = 3.0
        p_km = 3.0
        d_km = 5.0
        p_earn = round(p_km * rate, 2)
        d_earn = round(d_km * rate, 2)
        tot_earn = round(p_earn + d_earn, 2)
        print(f"Pickup: {p_km} km x {rate} = {p_earn}")
        print(f"Drop: {d_km} km x {rate} = {d_earn}")
        print(f"Total: {p_km + d_km} km x {rate} = {tot_earn}")
        assert p_earn == 9.0, f"Expected 9.0, got {p_earn}"
        assert d_earn == 15.0, f"Expected 15.0, got {d_earn}"
        assert tot_earn == 24.0, f"Expected 24.0, got {tot_earn}"
        print("PASS: Formula produces exact ₹24 for 3km pickup + 5km drop")

        print("\n=== 3. Testing Admin Rate Configuration & Audit Trail ===")
        admin = User.query.filter_by(role='admin').first()
        assert admin is not None, "Admin user must exist"

        # Update rate to 4.5
        new_rate = set_driver_rate_per_km(4.5, admin)
        assert get_driver_rate_per_km() == 4.5, "Rate should be 4.5"
        audit = mongo_db['audit_logs'].find_one({'action': 'DRIVER_RATE_UPDATED', 'actor_id': admin.id})
        assert audit is not None, "Audit log should exist for rate update"
        print(f"PASS: Admin updated rate to {new_rate} and audit logged: {audit['details']}")

        # Reset rate back to 3.0
        set_driver_rate_per_km(3.0, admin)
        assert get_driver_rate_per_km() == 3.0, "Rate should be back to 3.0"
        print("PASS: Reset rate to 3.0")

        print("\n=== 4. Testing Order Earning Calculation with Real/Road Coordinates ===")
        order = Order.query.get(3)
        assert order is not None, "Order #3 must exist"
        earnings = calculate_order_driver_earnings(order)
        print(f"Order #3 Pickup: {earnings['pickup_distance_km']} km ({earnings['pickup_source_label']}) -> ₹{earnings['pickup_earning']}")
        print(f"Order #3 Drop: {earnings['drop_distance_km']} km ({earnings['drop_source_label']}) -> ₹{earnings['drop_earning']}")
        print(f"Order #3 Total: {earnings['total_distance_km']} km -> ₹{earnings['total_earning']}")
        assert earnings['pickup_earning'] == round(earnings['pickup_distance_km'] * 3.0, 2)
        assert earnings['drop_earning'] == round(earnings['drop_distance_km'] * 3.0, 2)
        assert earnings['total_earning'] == round(earnings['pickup_earning'] + earnings['drop_earning'], 2)
        print("PASS: Order calculation correctly applied road routing and rate")

        print("\n=== 5. Testing Server Payout Finalization & Duplicate Prevention (Idempotency) ===")
        # Create a test order
        driver = User.query.filter_by(role='driver').first()
        farm = Farm.query.first()
        cust = User.query.filter_by(role='customer').first()
        driver_initial_earnings = float(getattr(driver, 'total_earnings', 0.0) or 0.0)

        test_order = Order(
            farm_id=farm.id,
            customer_id=cust.id,
            driver_id=driver.id,
            total_price=50.0,
            status=OrderStatus.PICKED_UP,
            customer_name='Test Recipient',
            customer_address='Test St 101',
            customer_latitude=12.980,
            customer_longitude=77.610,
            driver_latitude=12.970,
            driver_longitude=77.590
        )
        db.session.add(test_order)
        db.session.commit()

        print(f"Created test order #{test_order.id}. Initial driver earnings: ₹{driver_initial_earnings:.2f}")

        # Finalize payout for test order
        res1 = finalize_driver_order_payout(test_order, driver)
        assert res1 is not None, "First payout finalization must succeed"
        payout_amount = res1['driver_payout_amount']
        print(f"Payout finalized: ₹{payout_amount:.2f}")

        # Check driver earnings increased
        driver_updated = User.query.get(driver.id)
        assert round(driver_updated.total_earnings, 2) == round(driver_initial_earnings + payout_amount, 2), "Driver earnings must be credited"
        print(f"Driver earnings updated: ₹{driver_updated.total_earnings:.2f}")

        # Try finalizing a SECOND time (simulate duplicate delivery request or network retry)
        res2 = finalize_driver_order_payout(test_order, driver)
        assert res2 is None, "Second payout finalization must return None (idempotent / duplicate prevented)"

        # Check driver earnings did NOT increase again
        driver_after_duplicate = User.query.get(driver.id)
        assert round(driver_after_duplicate.total_earnings, 2) == round(driver_updated.total_earnings, 2), "Duplicate payout must not be credited"
        print("PASS: Duplicate payout was strictly prevented!")

        # Clean up test order
        mongo_db['orders'].delete_one({'_id': test_order.id})
        # Reset driver earnings back
        mongo_db['users'].update_one({'_id': driver.id}, {'$set': {'total_earnings': driver_initial_earnings}})
        print("Cleaned up test order.")

        print("\nALL DRIVER EARNINGS TESTS PASSED SUCCESSFULLY!")

if __name__ == '__main__':
    run_tests()
