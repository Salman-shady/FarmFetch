import sys
sys.path.insert(0, '.')
from app import infer_produce_unit

test_cases = [
    ('Fresh Cow Milk', 'Dairy', 'per liter'),
    ('Pure Buffalo Milk', 'Dairy', 'per liter'),
    ('Curd', 'Dairy', 'per liter'),
    ('Organic Red Tomatoes', 'Vegetables', 'per kg'),
    ('Crisp Fuji Apples', 'Fruits', 'per kg'),
    ('Fresh Baby Spinach', 'Vegetables', 'per bunch'),
    ('Coriander Leaves', 'Herbs', 'per bunch'),
    ('Free Range Eggs', 'Dairy', 'per dozen'),
    ('Raw Wildflower Honey', 'Other', 'per 500g jar'),
    ('Fresh Green Coconut', 'Fruits', 'per piece'),
    ('Watermelon', 'Fruits', 'per piece'),
    ('Cabbage', 'Vegetables', 'per piece'),
    ('', 'Dairy', 'per liter'),
    ('', 'Vegetables', 'per kg'),
    ('', 'Herbs', 'per bunch'),
    ('Basmati Rice', 'Grains', 'per kg')
]

all_passed = True
for name, cat, expected in test_cases:
    actual = infer_produce_unit(name, cat)
    passed = (actual == expected)
    if not passed:
        all_passed = False
    print(f"[{'PASS' if passed else 'FAIL'}] name='{name}', cat='{cat}' -> got '{actual}', expected '{expected}'")

print(f"\nAll tests passed: {all_passed}")
assert all_passed, "Unit inference test failed!"
