import sys
import os
import unittest
import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import app
from app import (
    app as flask_app, mongo_db, User, Farm, ProduceItem, Order,
    OrderStatus, VerificationStatus, ComplaintStatus, ComplaintCategory,
    Review, Complaint, AuditLog, get_next_id, hash_otp, generate_secure_otp
)

class AdminAndPlatformSecurityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        flask_app.config['TESTING'] = True
        flask_app.config['WTF_CSRF_ENABLED'] = False
        cls.client = flask_app.test_client()

        # Clean test records
        mongo_db.users.delete_many({'email': {'$regex': r'^test_'}})
        mongo_db.farms.delete_many({'name': {'$regex': r'^Test '}})
        mongo_db.produce_items.delete_many({'name': {'$regex': r'^Test '}})
        mongo_db.orders.delete_many({'delivery_address': {'$regex': r'Test'}})
        mongo_db.reviews.delete_many({'comment': {'$regex': r'Test'}})
        mongo_db.complaints.delete_many({'description': {'$regex': r'Test'}})
        mongo_db.audit_logs.delete_many({'details.test_marker': True})

        # Ensure seed admin exists
        cls.admin_user = User.query.filter_by(role='admin').first()
        if not cls.admin_user:
            cls.admin_user = User(
                email='admin@farm.com',
                name='Platform Administrator',
                role='admin',
                is_suspended=False
            )
            cls.admin_user.set_password('password123')
            app.db.session.add(cls.admin_user)
            app.db.session.commit()

        # Create a test customer A
        cls.cust_a = User(
            email='test_cust_a@farm.com',
            name='Customer Alpha',
            role='customer',
            is_suspended=False
        )
        cls.cust_a.set_password('password123')
        app.db.session.add(cls.cust_a)
        app.db.session.commit()

        # Create a test customer B (unrelated user)
        cls.cust_b = User(
            email='test_cust_b@farm.com',
            name='Customer Beta',
            role='customer',
            is_suspended=False
        )
        cls.cust_b.set_password('password123')
        app.db.session.add(cls.cust_b)
        app.db.session.commit()

        # Create a test farmer & farm
        cls.farmer = User(
            email='test_farmer_sec@farm.com',
            name='Farmer Security Test',
            role='farmer',
            is_suspended=False
        )
        cls.farmer.set_password('password123')
        app.db.session.add(cls.farmer)
        app.db.session.commit()

        cls.farm = Farm(
            name='Test Security Farm',
            user_id=cls.farmer.id,
            address='Test Farm Road 101',
            farm_type='Organic Vegetables',
            latitude=13.0,
            longitude=77.0,
            license_number='AGRI-TEST-9988',
            verification_status=VerificationStatus.PENDING
        )
        app.db.session.add(cls.farm)
        app.db.session.commit()

        # Create a test produce item
        cls.produce = ProduceItem(
            farm_id=cls.farm.id,
            name='Test Organic Apples',
            price=150.0,
            unit='kg',
            category='Fruits',
            stock=50
        )
        app.db.session.add(cls.produce)
        app.db.session.commit()

        # Create a test driver (initially PENDING)
        cls.driver = User(
            email='test_driver_sec@farm.com',
            name='Driver Security Test',
            role='driver',
            is_suspended=False,
            driver_verification_status=VerificationStatus.PENDING,
            driver_license_number='DL-TEST-776655',
            driver_vehicle_type='Motorbike',
            availability='AVAILABLE'
        )
        cls.driver.set_password('password123')
        app.db.session.add(cls.driver)
        app.db.session.commit()

    def login_as(self, email, password='password123'):
        self.client.get('/logout')
        return self.client.post('/login', data={'email': email, 'password': password}, follow_redirects=True)

    # --------------------------------------------------------------------------
    # SCENARIO 1: A customer attempts to access an admin URL.
    # --------------------------------------------------------------------------
    def test_01_customer_access_to_admin_url_rejected(self):
        self.login_as(self.cust_a.email)

        # Direct GET requests to admin endpoints
        admin_urls = [
            '/admin/dashboard',
            '/admin/verifications/farms',
            '/admin/verifications/drivers',
            '/admin/users',
            '/admin/complaints',
            '/admin/reviews',
            '/admin/audit-logs'
        ]
        for url in admin_urls:
            resp = self.client.get(url, follow_redirects=False)
            # Regular HTML request should redirect to home or return 403
            self.assertIn(resp.status_code, [302, 403], f"Customer should be denied access to {url}")
            if resp.status_code == 302:
                # Redirect location should be home or login, NOT an admin page
                self.assertNotIn('/admin', resp.headers.get('Location', ''))

        # API check: request expecting json should return HTTP 403 forbidden
        api_resp = self.client.get('/admin/dashboard', headers={'Accept': 'application/json'})
        self.assertIn(api_resp.status_code, [403, 302])
        print("PASS Scenario 1: Customer blocked from accessing administrator URLs.")

    # --------------------------------------------------------------------------
    # SCENARIO 2: A farmer attempts to approve their own verification.
    # --------------------------------------------------------------------------
    def test_02_farmer_cannot_approve_own_verification(self):
        self.login_as(self.farmer.email)

        # Farmer attempts to post directly to admin verification route
        resp = self.client.post(
            f'/admin/verify-farm/{self.farm.id}',
            data={'action': 'approve', 'notes': 'Farmer self-approving'},
            follow_redirects=False
        )
        self.assertIn(resp.status_code, [302, 403], "Farmer POST to admin verify endpoint must be rejected")
        if resp.status_code == 302:
            self.assertNotIn('/admin', resp.headers.get('Location', ''))

        # Verify farm verification status was NOT altered to APPROVED
        farm_doc = mongo_db.farms.find_one({'_id': self.farm.id})
        self.assertEqual(farm_doc.get('verification_status'), VerificationStatus.PENDING,
                         "Farm verification status must remain PENDING after unauthorized attempt")

        # Farmer attempts to inject verification_status via edit_farm_profile
        resp_edit = self.client.post('/farmer/profile/edit', data={
            'name': 'Test Security Farm Modified',
            'address': 'Test Farm Road 101',
            'farm_type': 'Organic Vegetables',
            'verification_status': VerificationStatus.APPROVED  # Tampered field
        }, follow_redirects=True)

        farm_doc2 = mongo_db.farms.find_one({'_id': self.farm.id})
        self.assertNotEqual(farm_doc2.get('verification_status'), VerificationStatus.APPROVED,
                            "Client-submitted verification_status must be ignored by server")
        print("PASS Scenario 2: Farmer cannot approve their own verification or forge approval.")

    # --------------------------------------------------------------------------
    # SCENARIO 3: An unverified driver attempts to accept a delivery.
    # --------------------------------------------------------------------------
    def test_03_unverified_driver_cannot_accept_delivery(self):
        # Create an order ready for pickup
        now = datetime.datetime.now()
        order_id = get_next_id('orders')
        order_doc = {
            '_id': order_id,
            'id': order_id,
            'customer_id': self.cust_a.id,
            'farm_id': self.farm.id,
            'driver_id': None,
            'total_price': 300.0,
            'status': OrderStatus.READY_FOR_PICKUP,
            'items': [{'name': 'Test Organic Apples', 'quantity': 2, 'price': 150.0}],
            'delivery_address': 'Test Customer Address 1',
            'created_at': now,
            'updated_at': now
        }
        mongo_db.orders.insert_one(order_doc)

        # Login as unverified driver (status is PENDING)
        self.login_as(self.driver.email)
        resp = self.client.post(f'/driver/accept/{order_id}', follow_redirects=True)
        self.assertIn(b'Only approved drivers can accept', resp.data)

        # Verify order was NOT assigned to the unverified driver
        check_order = mongo_db.orders.find_one({'_id': order_id})
        self.assertIsNone(check_order.get('driver_id'), "Order must remain unassigned when unverified driver attempts acceptance")
        self.assertEqual(check_order.get('status'), OrderStatus.READY_FOR_PICKUP)
        print("PASS Scenario 3: Unverified driver blocked from accepting delivery assignments.")

    # --------------------------------------------------------------------------
    # SCENARIO 4: An administrator approves or rejects a verification request.
    # --------------------------------------------------------------------------
    def test_04_administrator_approves_and_rejects_verification(self):
        self.login_as('admin@farm.com')

        # Admin approves farm
        resp_farm = self.client.post(
            f'/admin/verify-farm/{self.farm.id}',
            data={'action': 'approve', 'notes': 'All municipal documents verified'},
            follow_redirects=True
        )
        self.assertEqual(resp_farm.status_code, 200)
        farm_doc = mongo_db.farms.find_one({'_id': self.farm.id})
        self.assertEqual(farm_doc.get('verification_status'), VerificationStatus.APPROVED)
        self.assertEqual(farm_doc.get('verified_by'), 'admin@farm.com')

        # Check farm audit log
        audit_farm = mongo_db.audit_logs.find_one({
            'action': f"FARM_VERIFICATION_{VerificationStatus.APPROVED}",
            'target_id': self.farm.id
        })
        self.assertIsNotNone(audit_farm, "Audit log must be recorded for farm approval")

        # Admin rejects driver
        resp_driver = self.client.post(
            f'/admin/verify-driver/{self.driver.id}',
            data={'action': 'reject', 'notes': 'Driver license expired in 2023'},
            follow_redirects=True
        )
        self.assertEqual(resp_driver.status_code, 200)
        driver_doc = mongo_db.users.find_one({'_id': self.driver.id})
        self.assertEqual(driver_doc.get('driver_verification_status'), VerificationStatus.REJECTED)
        self.assertEqual(driver_doc.get('driver_verification_notes'), 'Driver license expired in 2023')

        # Check driver audit log
        audit_driver = mongo_db.audit_logs.find_one({
            'action': f"DRIVER_VERIFICATION_{VerificationStatus.REJECTED}",
            'target_id': self.driver.id
        })
        self.assertIsNotNone(audit_driver, "Audit log must be recorded for driver rejection")

        # Now admin approves driver after resubmission
        resp_driver_app = self.client.post(
            f'/admin/verify-driver/{self.driver.id}',
            data={'action': 'approve', 'notes': 'Updated license submitted and verified'},
            follow_redirects=True
        )
        self.assertEqual(resp_driver_app.status_code, 200)
        driver_doc2 = mongo_db.users.find_one({'_id': self.driver.id})
        self.assertEqual(driver_doc2.get('driver_verification_status'), VerificationStatus.APPROVED)
        print("PASS Scenario 4: Administrator approves/rejects verifications and logs audit entries.")

    # --------------------------------------------------------------------------
    # SCENARIO 5: A customer tries to review an order they did not complete.
    # --------------------------------------------------------------------------
    def test_05_customer_review_uncompleted_order_rejected(self):
        # Create an order in PREPARING status (not DELIVERED)
        now = datetime.datetime.now()
        order_prep_id = get_next_id('orders')
        order_prep_doc = {
            '_id': order_prep_id,
            'id': order_prep_id,
            'customer_id': self.cust_a.id,
            'farm_id': self.farm.id,
            'driver_id': self.driver.id,
            'total_price': 150.0,
            'status': OrderStatus.PREPARING,
            'delivery_address': 'Test Customer Address',
            'created_at': now
        }
        mongo_db.orders.insert_one(order_prep_doc)

        self.login_as(self.cust_a.email)
        # Attempt to review uncompleted order
        resp = self.client.post(f'/order/{order_prep_id}/review', data={
            'farm_rating': 5,
            'farm_comment': 'Test Should Fail - Order not delivered'
        }, follow_redirects=True)
        self.assertIn(b'Reviews can only be submitted for completed', resp.data)

        # Attempt to review someone else's completed order
        order_cust_b_id = get_next_id('orders')
        order_cust_b_doc = {
            '_id': order_cust_b_id,
            'id': order_cust_b_id,
            'customer_id': self.cust_b.id,  # Belongs to Customer B
            'farm_id': self.farm.id,
            'driver_id': self.driver.id,
            'total_price': 150.0,
            'status': OrderStatus.DELIVERED,
            'delivery_address': 'Test Customer B Address',
            'created_at': now
        }
        mongo_db.orders.insert_one(order_cust_b_doc)

        # Customer A tries to review Customer B's order
        resp_other = self.client.post(f'/order/{order_cust_b_id}/review', data={
            'farm_rating': 1,
            'farm_comment': 'Test Unauthorized Review'
        }, follow_redirects=True)
        self.assertIn(b'You can only review your own orders', resp_other.data)

        # Ensure no bogus review was stored
        rev_count = mongo_db.reviews.count_documents({'order_id': order_cust_b_id})
        self.assertEqual(rev_count, 0, "No review should be created for unauthorized user")
        print("PASS Scenario 5: Customer cannot review uncompleted or other users' orders.")

    # --------------------------------------------------------------------------
    # SCENARIO 6: A user submits duplicate reviews.
    # --------------------------------------------------------------------------
    def test_06_duplicate_reviews_prevented(self):
        # Create a delivered order for Customer A
        now = datetime.datetime.now()
        order_deliv_id = get_next_id('orders')
        order_deliv_doc = {
            '_id': order_deliv_id,
            'id': order_deliv_id,
            'customer_id': self.cust_a.id,
            'farm_id': self.farm.id,
            'driver_id': self.driver.id,
            'total_price': 300.0,
            'status': OrderStatus.DELIVERED,
            'delivery_address': 'Test Delivery Address for Reviews',
            'created_at': now
        }
        mongo_db.orders.insert_one(order_deliv_doc)

        self.login_as(self.cust_a.email)

        # First review submission
        resp1 = self.client.post(f'/order/{order_deliv_id}/review', data={
            'farm_rating': 5,
            'farm_comment': 'Test Excellent organic produce!',
            'driver_rating': 5,
            'driver_comment': 'Test Prompt and polite delivery.'
        }, follow_redirects=True)
        self.assertIn(b'submitted successfully', resp1.data)

        # Check exactly 2 reviews were stored (1 for farm, 1 for driver)
        farm_revs = mongo_db.reviews.count_documents({'order_id': order_deliv_id, 'target_type': 'farm'})
        driver_revs = mongo_db.reviews.count_documents({'order_id': order_deliv_id, 'target_type': 'driver'})
        self.assertEqual(farm_revs, 1, "Exactly one farm review must be stored")
        self.assertEqual(driver_revs, 1, "Exactly one driver review must be stored")

        # Second submission attempt for the same order
        resp2 = self.client.post(f'/order/{order_deliv_id}/review', data={
            'farm_rating': 1,
            'farm_comment': 'Test Duplicate malicious submission',
            'driver_rating': 1,
            'driver_comment': 'Test Duplicate malicious submission'
        }, follow_redirects=True)

        # Review count must STILL be exactly 1 each
        farm_revs_after = mongo_db.reviews.count_documents({'order_id': order_deliv_id, 'target_type': 'farm'})
        driver_revs_after = mongo_db.reviews.count_documents({'order_id': order_deliv_id, 'target_type': 'driver'})
        self.assertEqual(farm_revs_after, 1, "Duplicate farm review must be strictly blocked")
        self.assertEqual(driver_revs_after, 1, "Duplicate driver review must be strictly blocked")
        print("PASS Scenario 6: Duplicate customer reviews strictly prevented.")

    # --------------------------------------------------------------------------
    # SCENARIO 7: A customer creates a complaint for an eligible order.
    # --------------------------------------------------------------------------
    def test_07_customer_creates_complaint_and_admin_resolves(self):
        # Create an order for Customer A
        now = datetime.datetime.now()
        order_comp_id = get_next_id('orders')
        order_comp_doc = {
            '_id': order_comp_id,
            'id': order_comp_id,
            'customer_id': self.cust_a.id,
            'farm_id': self.farm.id,
            'driver_id': self.driver.id,
            'total_price': 450.0,
            'status': OrderStatus.DELIVERED,
            'delivery_address': 'Test Complaint Order Address',
            'created_at': now
        }
        mongo_db.orders.insert_one(order_comp_doc)

        self.login_as(self.cust_a.email)

        # File complaint
        resp = self.client.post(f'/order/{order_comp_id}/complaint', data={
            'category': 'Damaged produce',
            'description': 'Test Two cartons of tomatoes arrived bruised and ruptured.'
        }, follow_redirects=True)
        self.assertEqual(resp.status_code, 200)

        # Verify complaint stored in DB
        comp_doc = mongo_db.complaints.find_one({'order_id': order_comp_id})
        self.assertIsNotNone(comp_doc, "Complaint must be saved in complaints collection")
        self.assertEqual(comp_doc['category'], ComplaintCategory.DAMAGED_PRODUCE)
        self.assertEqual(comp_doc['status'], ComplaintStatus.OPEN)

        # Admin logs in to investigate and resolve dispute
        self.login_as('admin@farm.com')
        resp_res = self.client.post(f'/admin/complaints/{comp_doc["_id"]}', data={
            'status': ComplaintStatus.RESOLVED,
            'admin_notes': 'Customer contact made; partial store credit voucher issued by administration.'
        }, follow_redirects=True)
        self.assertEqual(resp_res.status_code, 200)

        # Verify status updated
        comp_after = mongo_db.complaints.find_one({'_id': comp_doc['_id']})
        self.assertEqual(comp_after['status'], ComplaintStatus.RESOLVED)
        self.assertIn('partial store credit', comp_after['admin_notes'])

        # Verify audit log entry
        audit_comp = mongo_db.audit_logs.find_one({
            'action': f"COMPLAINT_STATUS_{ComplaintStatus.RESOLVED}",
            'target_id': comp_doc['_id']
        })
        self.assertIsNotNone(audit_comp, "Audit log must be recorded for complaint resolution")
        print("PASS Scenario 7: Customer files complaint, admin updates status with audit logging.")

    # --------------------------------------------------------------------------
    # SCENARIO 8: An unrelated user attempts to view another customer's complaint.
    # --------------------------------------------------------------------------
    def test_08_unrelated_user_cannot_view_complaint(self):
        # Create a complaint by Customer A
        now = datetime.datetime.now()
        comp_sec_id = get_next_id('complaints')
        comp_sec_doc = {
            '_id': comp_sec_id,
            'id': comp_sec_id,
            'order_id': 99998,
            'customer_id': self.cust_a.id,
            'customer_email': self.cust_a.email,
            'category': 'Missing produce',
            'description': 'Test Confidential complaint details about Order #99998',
            'status': ComplaintStatus.OPEN,
            'created_at': now
        }
        mongo_db.complaints.insert_one(comp_sec_doc)

        # Customer B (unrelated) logs in and attempts to access Customer A's complaint
        self.login_as(self.cust_b.email)
        resp = self.client.get(f'/complaint/{comp_sec_id}')
        self.assertEqual(resp.status_code, 403, "Unrelated user must receive 403 Forbidden when accessing complaint")

        # Customer A (owner) logs in and CAN access
        self.login_as(self.cust_a.email)
        resp_owner = self.client.get(f'/complaint/{comp_sec_id}')
        self.assertEqual(resp_owner.status_code, 200, "Owner customer must have access to their own complaint")
        print("PASS Scenario 8: Unauthorized user receives 403 Forbidden on other customers' complaints.")

    # --------------------------------------------------------------------------
    # SCENARIO 9: A suspended user attempts a restricted action.
    # --------------------------------------------------------------------------
    def test_09_suspended_user_blocked_and_last_admin_lockout_protected(self):
        self.login_as('admin@farm.com')

        # Admin suspends Customer B with a justification reason
        resp_susp = self.client.post(
            f'/admin/user/{self.cust_b.id}/toggle-suspend',
            data={'reason': 'Repeated fraudulent refund claims'},
            follow_redirects=True
        )
        self.assertEqual(resp_susp.status_code, 200)

        cust_b_doc = mongo_db.users.find_one({'_id': self.cust_b.id})
        self.assertTrue(cust_b_doc.get('is_suspended'), "Customer B must be marked as suspended")
        self.assertEqual(cust_b_doc.get('suspension_reason'), 'Repeated fraudulent refund claims')

        # Suspended Customer B attempts to login
        self.client.get('/logout')
        resp_login = self.client.post('/login', data={
            'email': self.cust_b.email,
            'password': 'password123'
        }, follow_redirects=True)
        self.assertIn(b'Account Suspended', resp_login.data)
        self.assertIn(b'Repeated fraudulent refund claims', resp_login.data)

        # Admin lockout protection: Admin attempts to suspend themselves
        self.login_as('admin@farm.com')
        resp_self_susp = self.client.post(
            f'/admin/user/{self.admin_user.id}/toggle-suspend',
            data={'reason': 'Self suspension test'},
            follow_redirects=True
        )
        self.assertIn(b'You cannot suspend your own administrative account', resp_self_susp.data)
        admin_doc = mongo_db.users.find_one({'_id': self.admin_user.id})
        self.assertFalse(admin_doc.get('is_suspended', False), "Admin account must remain active")
        print("PASS Scenario 9: Suspended users blocked; last admin lockout safety enforced.")

    # --------------------------------------------------------------------------
    # SCENARIO 10: Registration security & platform core functionality.
    # --------------------------------------------------------------------------
    def test_10_registration_security_and_platform_regression(self):
        self.client.get('/logout')

        # 1. Attempt to register directly as 'admin'
        resp_fake_admin = self.client.post('/register', data={
            'email': 'hacker_admin@farm.com',
            'password': 'password123',
            'role': 'admin'
        }, follow_redirects=True)
        self.assertIn(b'Invalid account role', resp_fake_admin.data)
        hacker_doc = mongo_db.users.find_one({'email': 'hacker_admin@farm.com'})
        self.assertIsNone(hacker_doc, "Public registration must never create an administrator account")

        # 2. Legitimate registration as customer
        test_new_email = f"test_new_{int(datetime.datetime.now().timestamp())}@farm.com"
        resp_reg = self.client.post('/register', data={
            'email': test_new_email,
            'password': 'password123',
            'role': 'customer'
        }, follow_redirects=True)
        self.assertIn(b'Registration successful', resp_reg.data)
        new_cust = mongo_db.users.find_one({'email': test_new_email})
        self.assertIsNotNone(new_cust)
        self.assertEqual(new_cust.get('role'), 'customer')

        # 3. Admin dashboard financial aggregation verification
        self.login_as('admin@farm.com')
        resp_dash = self.client.get('/admin/dashboard')
        self.assertEqual(resp_dash.status_code, 200)
        self.assertIn(b'Platform Operations Dashboard', resp_dash.data)
        self.assertIn(b'Gross Order Value', resp_dash.data)
        self.assertIn(b'Platform Revenue (5%)', resp_dash.data)
        self.assertIn(b'Farmer Net Payouts (95%)', resp_dash.data)
        print("PASS Scenario 10: Public admin registration blocked and platform aggregation operational.")

if __name__ == '__main__':
    unittest.main()
