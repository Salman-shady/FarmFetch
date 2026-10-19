
"""
Seed script to populate MongoDB with demo accounts and farm produce.
Usage: python seed_data.py
"""
import os
from app import app, db, User, Farm, ProduceItem, Order, OrderItem, mongo_db

def seed():
    with app.app_context():
        print("Clearing existing data in MongoDB...")
        for col in ['users', 'farms', 'produce_items', 'orders', 'order_items', 'counters']:
            mongo_db[col].delete_many({})

        db.create_all()

        print("Creating demo accounts...")
        # 1. Farmer account
        farmer = User(email='farmer@farm.com', role='farmer')
        farmer.set_password('password123')
        db.session.add(farmer)
        db.session.commit()

        # 2. Farm Profile
        farm = Farm(
            name='Green Valley Organic Farm',
            address='Highway 44, Green Meadows',
            farm_type='Organic Fruits & Vegetables',
            user_id=farmer.id,
            latitude=12.9716,
            longitude=77.5946
        )
        db.session.add(farm)
        db.session.commit()

        # 3. Produce items
        items = [
            ProduceItem(farm_id=farm.id, name='Organic Red Tomatoes', description='Vine-ripened, farm-fresh juicy tomatoes.', price=2.99, unit='per kg', category='Vegetables', stock=15, image_url='https://images.unsplash.com/photo-1592924357228-91a4daadcfea?w=500'),
            ProduceItem(farm_id=farm.id, name='Crisp Fuji Apples', description='Sweet, crisp apples harvested straight from the orchard.', price=4.50, unit='per kg', category='Fruits', stock=25, image_url='https://images.unsplash.com/photo-1560806887-1e4cd0b6cbd6?w=500'),
            ProduceItem(farm_id=farm.id, name='Fresh Baby Spinach', description='Tender organic spinach leaves, rich in iron.', price=1.99, unit='per bunch', category='Vegetables', stock=4, image_url='https://images.unsplash.com/photo-1576045057995-568f588f82fb?w=500'),
            ProduceItem(farm_id=farm.id, name='Raw Organic Wildflower Honey', description='100% pure, unpasteurized natural honey.', price=8.50, unit='per 500g jar', category='Other', stock=12, image_url='https://images.unsplash.com/photo-1587049352846-4a222e784d38?w=500'),
            ProduceItem(farm_id=farm.id, name='Farm Fresh Country Eggs', description='Free-range organic brown eggs.', price=4.20, unit='per dozen', category='Dairy', stock=20, image_url='https://images.unsplash.com/photo-1582722872445-44dc5f7e3c8f?w=500'),
        ]
        for it in items:
            db.session.add(it)
        db.session.commit()

        # 4. Customer account
        customer = User(email='customer@farm.com', role='customer')
        customer.set_password('password123')
        db.session.add(customer)

        # 5. Delivery Driver account
        driver = User(email='driver@farm.com', role='driver')
        driver.set_password('password123')
        db.session.add(driver)
        db.session.commit()

        print("\n=============================================")
        print(" Seed data populated successfully in MongoDB!")
        print("=============================================")
        print("Demo Accounts:")
        print("  - Farmer  : farmer@farm.com   | password123")
        print("  - Customer: customer@farm.com | password123")
        print("  - Driver  : driver@farm.com   | password123")
        print("Farm Created: Green Valley Organic Farm (5 produce items)")
        print("=============================================\n")

if __name__ == '__main__':
    seed()
