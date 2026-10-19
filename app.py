import os
import math 
import datetime
from flask import Flask, render_template, redirect, url_for, request, flash, session, jsonify, abort
import pymongo
from flask_login import LoginManager, UserMixin, login_user, logout_user, current_user, login_required
from flask_bcrypt import Bcrypt
from functools import wraps
from flask_socketio import SocketIO, emit, join_room, leave_room 
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env

# --- App Initialization ---

app = Flask(__name__)

# Configuration
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB_NAME = os.environ.get('MONGO_DB_NAME', 'farm2table')
app.config['MONGO_URI'] = MONGO_URI
app.config['MONGO_DB_NAME'] = MONGO_DB_NAME
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', '806f4687241566d2a035411c1e0a9787e8e836cedda573d151a06b6bebe5e3fc')

# Extensions
mongo_client = pymongo.MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB_NAME]

bcrypt = Bcrypt(app)
login_manager = LoginManager(app)
login_manager.login_view = 'login'
login_manager.login_message_category = 'info'
socketio = SocketIO(app)

# --- Haversine Formula Helper ---
def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance in kilometers between two points 
    on the earth (specified in decimal degrees).
    """
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a)) 
    r = 6371 # Radius of earth in kilometers.
    return c * r

def safe_float(value):
    """Converts a value to float, or returns None if it fails."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

# --- MongoDB Sequence Helper ---
def get_next_id(collection_name):
    """Generates auto-incrementing integer IDs for collections."""
    counter = mongo_db['counters'].find_one_and_update(
        {'_id': collection_name},
        {'$inc': {'seq': 1}},
        upsert=True,
        return_document=pymongo.ReturnDocument.AFTER
    )
    return counter['seq']

# --- MongoDB Query & Model Layer ---

class QueryCondition:
    def __init__(self, expr):
        self.expr = expr

    def __or__(self, other):
        e1 = self.expr if isinstance(self, QueryCondition) else self
        e2 = other.expr if isinstance(other, QueryCondition) else other
        return QueryCondition({'$or': [e1, e2]})

    def __and__(self, other):
        e1 = self.expr if isinstance(self, QueryCondition) else self
        e2 = other.expr if isinstance(other, QueryCondition) else other
        return QueryCondition({'$and': [e1, e2]})

    def to_mongo(self):
        return self.expr

class ColumnField:
    """Descriptor allowing model class attributes for queries and instance data access."""
    def __init__(self, name):
        self.name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance._data.get(self.name)

    def __set__(self, instance, value):
        if instance is not None:
            instance._data[self.name] = value
            instance._is_dirty = True

    def __eq__(self, other):
        return QueryCondition({self.name: other})

    def __ne__(self, other):
        return QueryCondition({self.name: {'$ne': other}})

    def desc(self):
        return (self.name, pymongo.DESCENDING)

    def asc(self):
        return (self.name, pymongo.ASCENDING)

class MongoQuery:
    def __init__(self, model_class, filter_doc=None, sort_criteria=None):
        self.model_class = model_class
        self.filter_doc = filter_doc or {}
        self.sort_criteria = sort_criteria or []

    def filter_by(self, **kwargs):
        new_filter = dict(self.filter_doc)
        for k, v in kwargs.items():
            if k == 'id':
                new_filter['$or'] = [{'_id': v}, {'id': v}]
            else:
                new_filter[k] = v
        return MongoQuery(self.model_class, new_filter, self.sort_criteria)

    def filter(self, condition):
        if hasattr(condition, 'to_mongo'):
            cond_dict = condition.to_mongo()
        elif isinstance(condition, dict):
            cond_dict = condition
        else:
            cond_dict = {}
        if not self.filter_doc:
            new_filter = cond_dict
        else:
            new_filter = {'$and': [self.filter_doc, cond_dict]}
        return MongoQuery(self.model_class, new_filter, self.sort_criteria)

    def order_by(self, *criteria):
        new_sort = list(self.sort_criteria)
        for c in criteria:
            if isinstance(c, tuple):
                new_sort.append(c)
            elif hasattr(c, 'desc'):
                new_sort.append(c.desc())
            elif hasattr(c, 'name'):
                new_sort.append((c.name, pymongo.ASCENDING))
            elif isinstance(c, str):
                new_sort.append((c, pymongo.ASCENDING))
        return MongoQuery(self.model_class, self.filter_doc, new_sort)

    def all(self):
        col = self.model_class.get_collection()
        cursor = col.find(self.filter_doc)
        if self.sort_criteria:
            cursor = cursor.sort(self.sort_criteria)
        return [self.model_class.from_doc(d) for d in cursor]

    def first(self):
        col = self.model_class.get_collection()
        cursor = col.find(self.filter_doc)
        if self.sort_criteria:
            cursor = cursor.sort(self.sort_criteria)
        doc = next(cursor, None)
        return self.model_class.from_doc(doc) if doc else None

    def first_or_404(self):
        item = self.first()
        if item is None:
            abort(404)
        return item

    def get(self, ident):
        if ident is None:
            return None
        col = self.model_class.get_collection()
        candidates = [ident]
        if isinstance(ident, str) and ident.isdigit():
            candidates.append(int(ident))
        elif isinstance(ident, int):
            candidates.append(str(ident))
        doc = col.find_one({'$or': [{'_id': {'$in': candidates}}, {'id': {'$in': candidates}}]})
        return self.model_class.from_doc(doc) if doc else None

    def get_or_404(self, ident):
        item = self.get(ident)
        if item is None:
            abort(404)
        return item

class ModelMeta(type):
    @property
    def query(cls):
        return MongoQuery(cls)

class BaseModel(metaclass=ModelMeta):
    __collection__ = ''
    _tracked_instances = []

    def __init__(self, **kwargs):
        self._data = {}
        self._is_dirty = True
        for k, v in kwargs.items():
            self._data[k] = v
        if 'id' in kwargs:
            self.id = kwargs['id']
        elif '_id' in kwargs:
            self.id = kwargs['_id']
        else:
            self.id = None
        BaseModel._tracked_instances.append(self)

    @classmethod
    def get_collection(cls):
        return mongo_db[cls.__collection__]

    @classmethod
    def from_doc(cls, doc):
        if not doc:
            return None
        obj = cls()
        obj._data = dict(doc)
        obj.id = doc.get('id', doc.get('_id'))
        obj._data['id'] = obj.id
        obj._is_dirty = False
        return obj

    def __getattr__(self, name):
        if name in ('_data', '_is_dirty', 'id'):
            return super().__getattribute__(name)
        if '_data' in self.__dict__ and name in self._data:
            return self._data[name]
        # Return None for optional model attributes to safeguard templates
        if name in ('driver_id', 'latitude', 'longitude', 'customer_latitude', 'customer_longitude', 'unit', 'description', 'created_at', 'stock', 'image_url', 'category'):
            return None
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        if name in ('_data', '_is_dirty', 'id'):
            super().__setattr__(name, value)
        else:
            if '_data' in self.__dict__:
                self._data[name] = value
                self._is_dirty = True
            else:
                super().__setattr__(name, value)

    def save(self):
        col = self.get_collection()
        if self.id is None:
            self.id = get_next_id(self.__collection__)
            self._data['id'] = self.id
            self._data['_id'] = self.id
        else:
            self._data['id'] = self.id
            self._data['_id'] = self.id
        col.update_one({'_id': self.id}, {'$set': self._data}, upsert=True)
        self._is_dirty = False

    def delete_from_db(self):
        if self.id is not None:
            self.get_collection().delete_one({'_id': self.id})

class MongoSession:
    def __init__(self):
        self.pending_adds = []
        self.pending_deletes = []

    def add(self, obj):
        if obj not in self.pending_adds:
            self.pending_adds.append(obj)
            if getattr(obj, 'id', None) is None:
                obj.id = get_next_id(obj.__collection__)
                obj._data['id'] = obj.id
                obj._data['_id'] = obj.id

    def delete(self, obj):
        if obj not in self.pending_deletes:
            self.pending_deletes.append(obj)

    def commit(self):
        for obj in self.pending_adds:
            obj.save()
        self.pending_adds.clear()

        for obj in self.pending_deletes:
            obj.delete_from_db()
        self.pending_deletes.clear()

        for obj in list(BaseModel._tracked_instances):
            if getattr(obj, '_is_dirty', False):
                obj.save()

        # Keep memory clean
        if len(BaseModel._tracked_instances) > 200:
            BaseModel._tracked_instances = [o for o in BaseModel._tracked_instances if getattr(o, '_is_dirty', False)]

class DB:
    def __init__(self):
        self.session = MongoSession()

    def create_all(self):
        """Initializes collections and indexes in MongoDB."""
        try:
            mongo_db['users'].create_index('email', unique=True)
            mongo_db['farms'].create_index('user_id', unique=True)
            mongo_db['produce_items'].create_index('farm_id')
            mongo_db['orders'].create_index('farm_id')
            mongo_db['orders'].create_index('customer_id')
            mongo_db['orders'].create_index('driver_id')
            mongo_db['order_items'].create_index('order_id')
        except Exception as e:
            print("MongoDB index initialization:", e)

db = DB()

# --- Database Models ---

class User(BaseModel, UserMixin):
    __collection__ = 'users'

    email = ColumnField('email')
    role = ColumnField('role')

    def set_password(self, password):
        self.password_hash = bcrypt.generate_password_hash(password).decode('utf-8')
        self._is_dirty = True

    def check_password(self, password):
        stored_hash = getattr(self, 'password_hash', None)
        if not stored_hash:
            return False
        return bcrypt.check_password_hash(stored_hash, password)

    @property
    def farm(self):
        return Farm.query.filter_by(user_id=self.id).first()

    @property
    def orders(self):
        return Order.query.filter_by(customer_id=self.id).all()


class Farm(BaseModel):
    __collection__ = 'farms'

    user_id = ColumnField('user_id')
    name = ColumnField('name')
    farm_type = ColumnField('farm_type')

    @property
    def user(self):
        return User.query.get(self.user_id)

    @property
    def produce_items(self):
        return ProduceItem.query.filter_by(farm_id=self.id).all()

    @property
    def orders(self):
        return Order.query.filter_by(farm_id=self.id).all()

    def to_dict(self):
        return {
            'id': self.id,
            'name': getattr(self, 'name', ''),
            'address': getattr(self, 'address', ''),
            'farm_type': getattr(self, 'farm_type', ''),
            'latitude': getattr(self, 'latitude', None),
            'longitude': getattr(self, 'longitude', None),
        }


class ProduceItem(BaseModel):
    __collection__ = 'produce_items'

    farm_id = ColumnField('farm_id')
    name = ColumnField('name')
    price = ColumnField('price')

    @property
    def farm(self):
        return Farm.query.get(self.farm_id)


class Order(BaseModel):
    __collection__ = 'orders'

    status = ColumnField('status')
    driver_id = ColumnField('driver_id')
    farm_id = ColumnField('farm_id')
    customer_id = ColumnField('customer_id')
    created_at = ColumnField('created_at')

    def __init__(self, **kwargs):
        if 'created_at' not in kwargs:
            kwargs['created_at'] = datetime.datetime.now()
        if 'status' not in kwargs:
            kwargs['status'] = 'Placed'
        super().__init__(**kwargs)

    @property
    def customer(self):
        return User.query.get(self.customer_id)

    @property
    def driver(self):
        if getattr(self, 'driver_id', None):
            return User.query.get(self.driver_id)
        return None

    @property
    def farm(self):
        return Farm.query.get(self.farm_id)

    @property
    def items(self):
        return OrderItem.query.filter_by(order_id=self.id).all()


class OrderItem(BaseModel):
    __collection__ = 'order_items'

    order_id = ColumnField('order_id')
    produce_item_id = ColumnField('produce_item_id')

    @property
    def produce_item(self):
        item = ProduceItem.query.get(self.produce_item_id)
        if not item:
            class DummyItem:
                name = "Produce Item"
                unit = "unit"
                price = getattr(self, 'price_per_item', 0.0)
            return DummyItem()
        return item

    @property
    def order(self):
        return Order.query.get(self.order_id)


# --- Flask-Login User Loader ---

@login_manager.user_loader
def load_user(user_id):
    try:
        return User.query.get(int(user_id))
    except (TypeError, ValueError):
        return User.query.get(user_id)

# --- Custom Decorators ---

def farmer_required(f):
    """Decorator to ensure user is logged in and is a farmer."""
    @wraps(f)
    @login_required
    def decorated_function(*args, **kwargs):
        if current_user.role != 'farmer':
            flash('You do not have permission to access this page.', 'danger')
            return redirect(url_for('home'))
        return f(*args, **kwargs)
    return decorated_function

def driver_required(f):
    """Decorator to ensure user is logged in and is a delivery driver."""
    @wraps(f)
    @login_required
    def decorated_function(*args, **kwargs):
        if current_user.role != 'driver':
            flash('You do not have permission to access this page.', 'danger')
            return redirect(url_for('home'))
        return f(*args, **kwargs)
    return decorated_function

# --- Helper Functions ---
def get_cart_details():
    """
    Retrieves cart from session and calculates total price.
    """
    cart_items = []
    total_price = 0
    cart = session.get('cart', {})
    
    if not cart:
        return [], 0

    invalid_item_ids = []
    for item_id, quantity in cart.items():
        try:
            item = ProduceItem.query.get(int(item_id))
        except (TypeError, ValueError):
            invalid_item_ids.append(item_id)
            continue
        if item:
            cart_items.append({'item': item, 'quantity': quantity})
            total_price += item.price * quantity

    for item_id in invalid_item_ids:
        del cart[item_id]

    if invalid_item_ids:
        session['cart'] = cart
            
    return cart_items, total_price

# --- Customer Dashboard Route ---

@app.route('/customer/dashboard')
@login_required
def customer_dashboard():
    """Customer personal dashboard - order history, active orders, cart summary."""
    if current_user.role != 'customer':
        flash('Access denied.', 'danger')
        return redirect(url_for('home'))
    
    all_orders = Order.query.filter_by(customer_id=current_user.id).order_by(Order.created_at.desc()).all()
    
    active_statuses = ['Placed', 'Preparing', 'Ready for Pickup', 'Picked Up']
    active_orders = [o for o in all_orders if o.status in active_statuses]
    completed_orders = [o for o in all_orders if o.status == 'Delivered']
    recent_orders = all_orders[:5]
    
    total_spent = sum(o.total_price for o in completed_orders if o.total_price)
    cart_count = sum(session.get('cart', {}).values()) if session.get('cart') else 0
    
    return render_template(
        'customer_dashboard.html',
        all_orders=all_orders,
        active_orders=active_orders,
        completed_orders=completed_orders,
        recent_orders=recent_orders,
        total_spent=total_spent,
        cart_count=cart_count
    )

@app.route('/api/driver/stats')
@login_required
def api_driver_stats():
    """Returns JSON stats for the driver dashboard."""
    if current_user.role != 'driver':
        return jsonify({'error': 'Forbidden'}), 403
    
    all_driver_orders = Order.query.filter_by(driver_id=current_user.id).all()
    completed = [o for o in all_driver_orders if o.status == 'Delivered']
    
    return jsonify({
        'total_delivered': len(completed),
        'total_earnings': round(sum(o.total_price * 0.1 for o in completed if o.total_price), 2)
    })

@app.route('/api/farmer/sales-chart')
@farmer_required
def api_farmer_sales_chart():
    """Returns last 7-day daily revenue data for Chart.js."""
    farm = Farm.query.filter_by(user_id=current_user.id).first()
    if not farm:
        return jsonify({'error': 'No farm found'}), 404
    
    today = datetime.datetime.now()
    labels = []
    data = []
    
    for i in range(6, -1, -1):
        day = today - datetime.timedelta(days=i)
        day_start = day.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day.replace(hour=23, minute=59, second=59, microsecond=999999)
        label = day.strftime('%a %d')
        labels.append(label)
        
        day_orders = mongo_db['orders'].find({
            'farm_id': farm.id,
            'status': {'$ne': 'Rejected'},
            'created_at': {'$gte': day_start, '$lte': day_end}
        })
        revenue = sum(o.get('total_price', 0) or 0 for o in day_orders)
        data.append(round(revenue, 2))
    
    return jsonify({'labels': labels, 'data': data})

@app.route('/api/customer/products')
def api_customer_products():
    """Returns a list of all produce items for browsing. Supports search and category filter."""
    search = request.args.get('q', '').lower().strip()
    category = request.args.get('category', '').strip()
    sort = request.args.get('sort', 'name')  # name, price_asc, price_desc
    
    all_items = ProduceItem.query.all()
    results = []
    
    for item in all_items:
        farm = Farm.query.get(item.farm_id)
        if not farm:
            continue
        
        item_category = getattr(item, 'category', 'Other') or 'Other'
        item_name = getattr(item, 'name', '') or ''
        item_desc = getattr(item, 'description', '') or ''
        
        # Filter by search
        if search and search not in item_name.lower() and search not in item_desc.lower() and search not in farm.name.lower():
            continue
        
        # Filter by category
        if category and item_category.lower() != category.lower():
            continue
        
        results.append({
            'id': item.id,
            'name': item_name,
            'description': item_desc,
            'price': item.price,
            'unit': getattr(item, 'unit', 'unit') or 'unit',
            'category': item_category,
            'stock': getattr(item, 'stock', None),
            'image_url': getattr(item, 'image_url', '') or '',
            'farm_id': farm.id,
            'farm_name': farm.name,
            'farm_address': getattr(farm, 'address', '') or '',
        })
    
    # Sort
    if sort == 'price_asc':
        results.sort(key=lambda x: x['price'])
    elif sort == 'price_desc':
        results.sort(key=lambda x: x['price'], reverse=True)
    else:
        results.sort(key=lambda x: x['name'].lower())
    
    return jsonify(results)

@app.route('/api/geocode')
def api_geocode():
    """
    Geocode an address, town, city, or state name (e.g., Tumkur, Mysore, Bangalore)
    using OpenStreetMap Nominatim with built-in regional fallback.
    """
    import urllib.request
    import urllib.parse
    import json
    
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify({'error': 'No location query provided'}), 400
    
    FALLBACK_CITIES = {
        'tumkur': {'lat': 13.3401, 'lon': 77.1006, 'display_name': 'Tumakuru, Karnataka, India'},
        'tumakuru': {'lat': 13.3401, 'lon': 77.1006, 'display_name': 'Tumakuru, Karnataka, India'},
        'mysore': {'lat': 12.3052, 'lon': 76.6554, 'display_name': 'Mysuru, Karnataka, India'},
        'mysuru': {'lat': 12.3052, 'lon': 76.6554, 'display_name': 'Mysuru, Karnataka, India'},
        'bangalore': {'lat': 12.9716, 'lon': 77.5946, 'display_name': 'Bengaluru, Karnataka, India'},
        'bengaluru': {'lat': 12.9716, 'lon': 77.5946, 'display_name': 'Bengaluru, Karnataka, India'},
        'mangalore': {'lat': 12.9141, 'lon': 74.8560, 'display_name': 'Mangaluru, Karnataka, India'},
        'mangaluru': {'lat': 12.9141, 'lon': 74.8560, 'display_name': 'Mangaluru, Karnataka, India'},
        'hubli': {'lat': 15.3647, 'lon': 75.1240, 'display_name': 'Hubballi, Karnataka, India'},
        'hubballi': {'lat': 15.3647, 'lon': 75.1240, 'display_name': 'Hubballi, Karnataka, India'},
        'belgaum': {'lat': 15.8497, 'lon': 74.4977, 'display_name': 'Belagavi, Karnataka, India'},
        'belagavi': {'lat': 15.8497, 'lon': 74.4977, 'display_name': 'Belagavi, Karnataka, India'},
        'hassan': {'lat': 13.0033, 'lon': 76.1004, 'display_name': 'Hassan, Karnataka, India'},
        'mandya': {'lat': 12.5223, 'lon': 76.8973, 'display_name': 'Mandya, Karnataka, India'},
        'shimoga': {'lat': 13.9299, 'lon': 75.5681, 'display_name': 'Shivamogga, Karnataka, India'},
        'shivamogga': {'lat': 13.9299, 'lon': 75.5681, 'display_name': 'Shivamogga, Karnataka, India'},
        'davanagere': {'lat': 14.4644, 'lon': 75.9218, 'display_name': 'Davanagere, Karnataka, India'},
        'delhi': {'lat': 28.6139, 'lon': 77.2090, 'display_name': 'New Delhi, Delhi, India'},
        'mumbai': {'lat': 19.0760, 'lon': 72.8777, 'display_name': 'Mumbai, Maharashtra, India'},
        'chennai': {'lat': 13.0827, 'lon': 80.2707, 'display_name': 'Chennai, Tamil Nadu, India'},
        'hyderabad': {'lat': 17.3850, 'lon': 78.4867, 'display_name': 'Hyderabad, Telangana, India'},
        'kerala': {'lat': 10.8505, 'lon': 76.2711, 'display_name': 'Kerala, India'},
        'karnataka': {'lat': 15.3173, 'lon': 75.7139, 'display_name': 'Karnataka, India'},
    }
    
    try:
        url = f"https://nominatim.openstreetmap.org/search?format=json&q={urllib.parse.quote(query)}&limit=5&countrycodes=in"
        req = urllib.request.Request(url, headers={'User-Agent': 'FarmFetchPlatform/1.0'})
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            if data and len(data) > 0:
                results = []
                for item in data:
                    results.append({
                        'lat': round(float(item['lat']), 6),
                        'lon': round(float(item['lon']), 6),
                        'display_name': item.get('display_name', query)
                    })
                return jsonify(results)
    except Exception as e:
        print("Geocoding lookup notice:", e)
        
    q_lower = query.lower().strip()
    for key, val in FALLBACK_CITIES.items():
        if key in q_lower or q_lower in key:
            return jsonify([val])
            
    return jsonify([{
        'lat': 12.9716,
        'lon': 77.5946,
        'display_name': f"{query.title()}, India"
    }])

# --- Authentication Routes ---

@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        role = request.form.get('role') # 'customer', 'farmer', or 'driver'

        existing_user = User.query.filter_by(email=email).first()
        if existing_user:
            flash('Email already registered. Please login.', 'warning')
            return redirect(url_for('login'))
        
        user = User(email=email, role=role)
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        
        flash('Registration successful! Please login.', 'success')
        return redirect(url_for('login'))
        
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('home'))
        
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        user = User.query.filter_by(email=email).first()
        
        if user and user.check_password(password):
            login_user(user, remember=True)
            flash(f'Logged in successfully as {user.email}!', 'success')
            
            if user.role == 'farmer':
                return redirect(url_for('farmer_dashboard'))
            elif user.role == 'driver':
                return redirect(url_for('driver_dashboard'))
            else:
                return redirect(url_for('customer_dashboard'))
        else:
            flash('Login unsuccessful. Please check email and password.', 'danger')
            
    return render_template('login.html')

@app.route('/quick-login/<role>')
def quick_login(role):
    """
    1-Click instant login for development & testing.
    Switches/logs in to farmer@farm.com, customer@farm.com, or driver@farm.com without typing.
    """
    role_email_map = {
        'farmer': 'farmer@farm.com',
        'customer': 'customer@farm.com',
        'driver': 'driver@farm.com'
    }
    target_email = role_email_map.get(role.lower())
    if not target_email:
        flash('Unknown demo role.', 'warning')
        return redirect(url_for('login'))
        
    user = User.query.filter_by(email=target_email).first()
    if not user:
        user = User(email=target_email, role=role.lower())
        user.set_password('password123')
        db.session.add(user)
        db.session.commit()
        
    if current_user.is_authenticated:
        logout_user()
    login_user(user, remember=True)
    flash(f'Logged in as {role.title()} ({user.email})', 'success')
    
    if user.role == 'farmer':
        return redirect(url_for('farmer_dashboard'))
    elif user.role == 'driver':
        return redirect(url_for('driver_dashboard'))
    else:
        return redirect(url_for('customer_dashboard'))

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('home'))

# --- Customer Browsing Routes ---

@app.route('/')
@app.route('/home')
def home():
    """
    Renders the homepage. 
    JS will make an API call to populate nearby farms.
    """
    return render_template('home.html')

@app.route('/api/nearby-farms')
def api_nearby_farms():
    """
    Returns a JSON list of nearby farms based on user's lat/lon.
    """
    try:
        user_lat = float(request.args.get('lat'))
        user_lon = float(request.args.get('lon'))
    except (TypeError, ValueError):
        return jsonify({'error': 'Invalid location data'}), 400

    all_farms = Farm.query.all()
    nearby_farms = []
    
    for farm in all_farms:
        if farm.latitude and farm.longitude:
            distance = haversine(user_lat, user_lon, farm.latitude, farm.longitude)
            
            if distance < 500: 
                farm_data = farm.to_dict()
                farm_data['distance'] = round(distance, 2)
                nearby_farms.append(farm_data)
                
    nearby_farms.sort(key=lambda x: x['distance'])
    
    return jsonify(nearby_farms)

@app.route('/farm/<int:farm_id>')
def farm_produce(farm_id):
    """Customer-facing page to view a specific farm's produce."""
    farm = Farm.query.get_or_404(farm_id)
    produce_items = farm.produce_items
    return render_template('farm_produce.html', farm=farm, produce_items=produce_items)

# --- Farmer Management Routes ---

@app.route('/farmer/dashboard')
@farmer_required
def farmer_dashboard():
    """
    Main dashboard for farmers.
    Calculates real KPI metrics from database and renders farmer_dashboard.html.
    """
    farm = Farm.query.filter_by(user_id=current_user.id).first()
    if not farm:
        flash('Welcome! Please create your farm profile to get started.', 'info')
        return redirect(url_for('create_farm_profile'))
    
    orders = Order.query.filter_by(farm_id=farm.id).all()
    produce_items = ProduceItem.query.filter_by(farm_id=farm.id).all()
    
    total_orders = len(orders)
    pending_orders = len([o for o in orders if o.status in ['Placed', 'Preparing']])
    ready_orders = len([o for o in orders if o.status == 'Ready for Pickup'])
    completed_orders = len([o for o in orders if o.status == 'Delivered'])
    total_revenue = sum(o.total_price for o in orders if o.total_price and o.status != 'Rejected')
    recent_orders = Order.query.filter_by(farm_id=farm.id).order_by(Order.created_at.desc()).all()[:10]
    
    # Low stock items: stock is set and <= 5
    low_stock_items = [item for item in produce_items if getattr(item, 'stock', None) is not None and item.stock <= 5]
    
    return render_template(
        'farmer_dashboard.html',
        farm=farm,
        total_orders=total_orders,
        pending_orders=pending_orders,
        ready_orders=ready_orders,
        completed_orders=completed_orders,
        total_revenue=total_revenue,
        produce_count=len(produce_items),
        produce_items=produce_items,
        recent_orders=recent_orders,
        low_stock_items=low_stock_items
    )

@app.route('/farmer/profile', methods=['GET', 'POST'])
@farmer_required
def create_farm_profile():
    """Route for farmers to create their profile."""
    if Farm.query.filter_by(user_id=current_user.id).first():
        return redirect(url_for('edit_farm_profile'))
        
    if request.method == 'POST':
        name = request.form.get('name')
        address = request.form.get('address')
        farm_type = request.form.get('farm_type')
        latitude = safe_float(request.form.get('latitude'))
        longitude = safe_float(request.form.get('longitude'))
        
        new_farm = Farm(
            name=name,
            address=address,
            farm_type=farm_type,
            user_id=current_user.id, 
            latitude=latitude, 
            longitude=longitude
        )
        db.session.add(new_farm)
        db.session.commit()
        
        flash('Farm profile created successfully!', 'success')
        return redirect(url_for('farmer_dashboard'))
        
    return render_template('create_farm_profile.html')

@app.route('/farmer/profile/edit', methods=['GET', 'POST'])
@farmer_required
def edit_farm_profile():
    """Route for farmers to edit their profile."""
    farm = Farm.query.filter_by(user_id=current_user.id).first_or_404()
    
    if request.method == 'POST':
        farm.name = request.form.get('name')
        farm.address = request.form.get('address')
        farm.farm_type = request.form.get('farm_type')
        farm.latitude = safe_float(request.form.get('latitude'))
        farm.longitude = safe_float(request.form.get('longitude'))

        db.session.commit()
        flash('Profile updated successfully!', 'success')
        return redirect(url_for('manage_produce'))
        
    return render_template('edit_farm_profile.html', farm=farm)

@app.route('/farmer/produce', methods=['GET', 'POST'])
@farmer_required
def manage_produce():
    """
    CRUD hub for Produce Items.
    """
    farm = Farm.query.filter_by(user_id=current_user.id).first_or_404()
    
    if request.method == 'POST':
        name = request.form.get('name')
        description = request.form.get('description')
        price = float(request.form.get('price'))
        unit = request.form.get('unit', 'per kg')
        category = request.form.get('category', 'Other')
        image_url = request.form.get('image_url', '').strip()
        try:
            stock = int(request.form.get('stock', 0))
        except (TypeError, ValueError):
            stock = 0
        
        new_item = ProduceItem(
            name=name,
            description=description,
            price=price,
            unit=unit,
            category=category,
            stock=stock,
            image_url=image_url,
            farm_id=farm.id
        )
        db.session.add(new_item)
        db.session.commit()
        flash('Produce item added successfully!', 'success')
        return redirect(url_for('manage_produce'))
    
    produce_items = ProduceItem.query.filter_by(farm_id=farm.id).all()
    return render_template('manage_produce.html', farm=farm, produce_items=produce_items)

@app.route('/farmer/produce/edit/<int:item_id>', methods=['GET', 'POST'])
@farmer_required
def edit_produce_item(item_id):
    """Edit an existing produce item."""
    item = ProduceItem.query.get_or_404(item_id)
    if item.farm.user_id != current_user.id:
        flash('You do not have permission to edit this item.', 'danger')
        return redirect(url_for('manage_produce'))
        
    if request.method == 'POST':
        item.name = request.form.get('name')
        item.description = request.form.get('description')
        item.price = float(request.form.get('price'))
        item.unit = request.form.get('unit')
        item.category = request.form.get('category', getattr(item, 'category', 'Other') or 'Other')
        item.image_url = request.form.get('image_url', '').strip()
        try:
            item.stock = int(request.form.get('stock', getattr(item, 'stock', 0) or 0))
        except (TypeError, ValueError):
            item.stock = 0
        db.session.commit()
        flash('Item updated successfully!', 'success')
        return redirect(url_for('manage_produce'))
    
    return render_template('edit_produce_item.html', item=item)

@app.route('/farmer/produce/delete/<int:item_id>', methods=['POST'])
@farmer_required
def delete_produce_item(item_id):
    """Delete a produce item."""
    item = ProduceItem.query.get_or_404(item_id)
    if item.farm.user_id != current_user.id:
        flash('You do not have permission to delete this item.', 'danger')
        return redirect(url_for('manage_produce'))
        
    db.session.delete(item)
    db.session.commit()
    flash('Item deleted successfully!', 'success')
    return redirect(url_for('manage_produce'))

# --- Shopping Cart & Order Routes ---

@app.route('/cart/add/<int:item_id>', methods=['POST'])
@login_required
def add_to_cart(item_id):
    if current_user.role != 'customer':
        flash('Only customers can add items to a cart.', 'danger')
        return redirect(url_for('home'))

    cart = session.get('cart', {})
    item = ProduceItem.query.get_or_404(item_id)
    
    # Check if cart is empty or if item is from the same farm
    if cart:
        first_item_id = next(iter(cart))
        try:
            first_item = ProduceItem.query.get(int(first_item_id))
        except (TypeError, ValueError):
            del cart[first_item_id]
            first_item = None

        if first_item is None:
            session['cart'] = cart
            cart = {}

    if cart and first_item:
        if first_item.farm_id != item.farm_id:
            flash('You can only order from one farm at a time. Clear your cart to add this item.', 'warning')
            return redirect(url_for('farm_produce', farm_id=item.farm_id))
    
    item_id_str = str(item_id)
    quantity = cart.get(item_id_str, 0)
    cart[item_id_str] = quantity + 1
    
    session['cart'] = cart
    flash(f'"{item.name}" added to your cart.', 'success')
    return redirect(url_for('farm_produce', farm_id=item.farm_id))

@app.route('/cart')
@login_required
def view_cart():
    cart_items, total_price = get_cart_details()
    return render_template('cart.html', cart_items=cart_items, total_price=total_price)

@app.route('/cart/update/<int:item_id>', methods=['POST'])
@login_required
def update_cart_item(item_id):
    cart = session.get('cart', {})
    item_id_str = str(item_id)
    
    if item_id_str in cart:
        try:
            quantity = int(request.form.get('quantity'))
            if quantity > 0:
                cart[item_id_str] = quantity
            elif quantity == 0:
                del cart[item_id_str]
            session['cart'] = cart
        except ValueError:
            flash('Invalid quantity.', 'danger')
    
    return redirect(url_for('view_cart'))

@app.route('/cart/remove/<int:item_id>', methods=['POST'])
@login_required
def remove_from_cart(item_id):
    cart = session.get('cart', {})
    item_id_str = str(item_id)
    
    if item_id_str in cart:
        del cart[item_id_str]
        session['cart'] = cart
        flash('Item removed from cart.', 'success')
        
    return redirect(url_for('view_cart'))

@app.route('/checkout', methods=['GET', 'POST'])
@login_required
def checkout():
    cart_items, total_price = get_cart_details()
    
    if not cart_items:
        flash('Your cart is empty. Add items to checkout.', 'warning')
        return redirect(url_for('home'))
        
    if request.method == 'POST':
        name = request.form.get('name')
        address = request.form.get('address')
        phone = request.form.get('phone')
        try:
            cust_lat = float(request.form.get('customer_latitude'))
            cust_lon = float(request.form.get('customer_longitude'))
        except (TypeError, ValueError):
            cust_lat = None
            cust_lon = None
        
        if not name or not address or not phone:
            flash('Please fill out all delivery details.', 'danger')
            return redirect(url_for('checkout'))

        first_item_in_cart = cart_items[0]['item']
        farm_id = first_item_in_cart.farm_id
        
        new_order = Order(
            customer_id=current_user.id,
            farm_id=farm_id,
            customer_name=name,
            customer_address=address,
            customer_phone=phone,
            total_price=total_price,
            status='Placed',
            customer_latitude=cust_lat,
            customer_longitude=cust_lon
        )
        db.session.add(new_order)
        db.session.commit()
        
        for item_data in cart_items:
            item = item_data['item']
            quantity = item_data['quantity']
            
            order_item = OrderItem(
                order_id=new_order.id,
                produce_item_id=item.id,
                quantity=quantity,
                price_per_item=item.price
            )
            db.session.add(order_item)
            
        db.session.commit()
        session.pop('cart', None)

        # Emit real-time event to the farmer
        farm_room = f"farm_{farm_id}"
        socketio.emit('new_order', {
            'order_id': new_order.id,
            'customer_name': new_order.customer_name,
            'total': new_order.total_price
        }, to=farm_room)
        
        flash('Order placed successfully!', 'success')
        return redirect(url_for('order_details', order_id=new_order.id))

    return render_template('checkout.html', cart_items=cart_items, total_price=total_price)

@app.route('/order/<int:order_id>')
@login_required
def order_details(order_id):
    order = Order.query.get_or_404(order_id)
    
    # Security check: User is customer, farmer, or driver for this order
    is_customer = current_user.id == order.customer_id
    is_farmer = current_user.role == 'farmer' and current_user.farm and current_user.farm.id == order.farm_id
    is_driver = current_user.role == 'driver' and current_user.id == order.driver_id
    
    if not (is_customer or is_farmer or is_driver):
        flash('You do not have permission to view this order.', 'danger')
        return redirect(url_for('home'))
        
    return render_template('order_details.html', order=order)

@app.route('/my-orders')
@login_required
def my_orders():
    """Customer order history page."""
    if current_user.role != 'customer':
        flash('Only customers can view this page.', 'warning')
        return redirect(url_for('home'))
    orders = Order.query.filter_by(customer_id=current_user.id).order_by(Order.created_at.desc()).all()
    return render_template('customer_orders.html', orders=orders)
    
@app.route('/farmer/orders')
@farmer_required
def farm_orders():
    """Farmer dashboard to see and manage incoming orders."""
    farm = current_user.farm
    orders = Order.query.filter_by(farm_id=farm.id)\
                        .order_by(Order.created_at.desc())\
                        .all()
                        
    return render_template('farm_orders.html', orders=orders)

@app.route('/farmer/order/update/<int:order_id>', methods=['POST'])
@farmer_required
def update_order_status(order_id):
    order = Order.query.get_or_404(order_id)
    
    if order.farm_id != current_user.farm.id:
        flash('You do not have permission to update this order.', 'danger')
        return redirect(url_for('farm_orders'))
        
    new_status = request.form.get('status')
    if new_status in ['Preparing', 'Ready for Pickup', 'Rejected']:
        order.status = new_status
        db.session.commit()

        order_room = f"order_{order_id}"
        socketio.emit('status_update', {
            'status': new_status
        }, to=order_room)

        flash(f'Order #{order.id} status updated to "{new_status}".', 'success')
              
    return redirect(url_for('farm_orders'))

# --- Delivery Driver Routes ---

@app.route('/driver/dashboard')
@driver_required
def driver_dashboard():
    """Show driver all orders that are 'Ready for Pickup' OR assigned to them."""
    orders = Order.query.filter(
        (Order.status == 'Ready for Pickup') | (Order.driver_id == current_user.id)
    ).order_by(Order.created_at.asc()).all()

    return render_template('driver_dashboard.html', orders=orders)

@app.route('/driver/accept/<int:order_id>', methods=['POST'])
@driver_required
def driver_accept_order(order_id):
    order = Order.query.get_or_404(order_id)
    
    if order.status == 'Ready for Pickup':
        order.status = 'Picked Up'
        order.driver_id = current_user.id
        db.session.commit()
        
        order_room = f"order_{order_id}"
        socketio.emit('status_update', {
            'status': 'Picked Up',
            'driver_name': current_user.email.split('@')[0]
        }, to=order_room)
        
        flash(f'You have accepted order #{order.id}.', 'success')
        return redirect(url_for('driver_delivery', order_id=order.id))
    else:
        flash(f'Order #{order.id} is no longer available.', 'warning')
        return redirect(url_for('driver_dashboard'))
    
@app.route('/driver/delivery/<int:order_id>')
@driver_required
def driver_delivery(order_id):
    order = Order.query.get_or_404(order_id)
    
    if order.driver_id != current_user.id:
        flash('You are not assigned to this delivery.', 'danger')
        return redirect(url_for('driver_dashboard'))
        
    return render_template('driver_delivery.html', order=order)

@app.route('/driver/complete_delivery/<int:order_id>', methods=['POST'])
@driver_required
def driver_complete_delivery(order_id):
    order = Order.query.get_or_404(order_id)
    
    if order.driver_id != current_user.id or order.status != 'Picked Up':
        flash('Cannot confirm delivery.', 'danger')
        return redirect(url_for('driver_dashboard'))
        
    order.status = 'Delivered'
    db.session.commit()
    
    order_room = f"order_{order_id}"
    socketio.emit('status_update', {
        'status': 'Delivered',
        'message': 'Your order has been successfully delivered!'
    }, to=order_room)
    
    flash(f'Order #{order.id} marked as Delivered! Thank you.', 'success')
    return redirect(url_for('driver_dashboard'))

# --- SocketIO Event Handlers ---

@socketio.on('join_order_room')
def handle_join_order_room(data):
    """Called by customer OR farmer JS when they load an order page."""
    order_id = data['order_id']
    room = f"order_{order_id}"
    join_room(room)
    print(f'Client joined room: {room}')

@socketio.on('join_farm_room')
def handle_join_farm_room(data):
    """Called by farmer JS when they load their order dashboard."""
    farm_id = data['farm_id']
    room = f"farm_{farm_id}"
    join_room(room)
    print(f'Farmer joined room: {room}')

@socketio.on('driver_location_update')
def handle_driver_location_update(data):
    """
    Received from the driver's browser.
    Broadcasts the location to the customer's room.
    """
    order_id = data['order_id']
    location = { 'lat': data['lat'], 'lng': data['lng'] }
    
    customer_room = f"order_{order_id}"
    emit('customer_location_update', location, to=customer_room)  

# --- Real-time Chat Handler ---
@socketio.on('send_chat_message')
@login_required
def handle_send_chat_message(data):
    """
    Called when a user sends a chat message from the order_details page.
    Broadcasts the message to the order room, skipping the sender.
    """
    order_id = data['order_id']
    message = data['message']
    
    sender_name = current_user.email.split('@')[0]
    sender_sid = request.sid 
    order_room = f"order_{order_id}"
    
    socketio.emit('new_chat_message', {
        'message': message,
        'sender': sender_name
    }, to=order_room, skip_sid=sender_sid)
    
    print(f"Message broadcast in {order_room} by {sender_name} (SID: {sender_sid})")

@socketio.on('start_typing')
@login_required
def handle_start_typing(data):
    """
    Called when a user starts typing in the chat.
    Broadcasts to the room, skipping the sender.
    """
    order_id = data['order_id']
    sender_name = current_user.email.split('@')[0]
    order_room = f"order_{order_id}"
    
    socketio.emit('user_is_typing', {
        'sender': sender_name
    }, to=order_room, skip_sid=request.sid)

@socketio.on('stop_typing')
@login_required
def handle_stop_typing(data):
    """
    Called when a user stops typing.
    Broadcasts to the room, skipping the sender.
    """
    order_id = data['order_id']
    order_room = f"order_{order_id}"
    
    socketio.emit('user_stopped_typing', to=order_room, skip_sid=request.sid)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    print("MongoDB Connected successfully! Running server on http://localhost:5000 ...")
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)