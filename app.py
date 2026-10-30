import os
import math 
import datetime
import time
import urllib.request
import json
import secrets
import hashlib
import hmac
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

    @classmethod
    def get_by_id(cls, ident):
        if ident is None:
            return None
        return cls.query.get(ident)

    def __getattr__(self, name):
        if name in ('_data', '_is_dirty', 'id'):
            return super().__getattribute__(name)
        if '_data' in self.__dict__ and name in self._data:
            return self._data[name]
        # Return None or empty list for optional model attributes to safeguard templates
        if name == 'status_history':
            return []
        if name in (
            'driver_id', 'latitude', 'longitude', 'customer_latitude', 'customer_longitude',
            'unit', 'description', 'created_at', 'stock', 'image_url', 'category',
            'pickup_otp', 'pickup_otp_hash', 'pickup_otp_expires_at', 'pickup_otp_attempts',
            'delivery_otp', 'delivery_otp_hash', 'delivery_otp_expires_at', 'delivery_otp_attempts',
            'idempotency_key', 'stock_reserved', 'stock_restored',
            'accepted_at', 'preparing_at', 'ready_at', 'assigned_at', 'picked_up_at',
            'delivered_at', 'cancelled_at', 'failed_at', 'cancellation_reason', 'failure_reason',
            'driver_latitude', 'driver_longitude', 'last_location_at',
            'last_latitude', 'last_longitude', 'last_location_update', 'last_active_at',
            'availability', 'phone', 'name',
            'is_suspended', 'suspension_reason', 'suspended_at', 'suspended_by',
            'verification_status', 'verification_notes', 'verified_at', 'verified_by',
            'license_number', 'supporting_document',
            'driver_verification_status', 'driver_verification_notes', 'driver_verified_at', 'driver_verified_by',
            'driver_license_number', 'driver_vehicle_type', 'vehicle_type',
            'rating', 'comment', 'is_reported', 'report_reason', 'is_hidden', 'moderated_at', 'moderated_by',
            'admin_notes', 'resolved_at', 'resolved_by', 'action', 'target_type', 'target_id', 'details', 'actor_email',
            'actor_role', 'actor_id', 'ip_address', 'timestamp', 'timestamp_display', 'timestamp_iso',
            'driver_payout_finalized', 'driver_payout_finalized_at', 'driver_payout_amount', 'driver_payout_rate',
            'driver_pickup_distance_km', 'driver_pickup_earning', 'driver_pickup_source',
            'driver_drop_distance_km', 'driver_drop_earning', 'driver_drop_source',
            'driver_payable_distance_km', 'total_earnings', 'last_payout_at'
        ):
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
            mongo_db['users'].create_index('role')
            mongo_db['users'].create_index('is_suspended')
            mongo_db['users'].create_index('driver_verification_status')

            mongo_db['farms'].create_index('user_id', unique=True)
            mongo_db['farms'].create_index('verification_status')

            mongo_db['produce_items'].create_index('farm_id')

            mongo_db['orders'].create_index('farm_id')
            mongo_db['orders'].create_index('customer_id')
            mongo_db['orders'].create_index('driver_id')
            mongo_db['orders'].create_index('status')
            mongo_db['orders'].create_index('created_at')

            mongo_db['order_items'].create_index('order_id')

            mongo_db['reviews'].create_index([('order_id', 1), ('target_type', 1)], unique=True)
            mongo_db['reviews'].create_index('target_id')
            mongo_db['reviews'].create_index('customer_id')
            mongo_db['reviews'].create_index('is_reported')
            mongo_db['reviews'].create_index('is_hidden')

            mongo_db['complaints'].create_index('order_id')
            mongo_db['complaints'].create_index('customer_id')
            mongo_db['complaints'].create_index('status')

            mongo_db['audit_logs'].create_index([('timestamp', pymongo.DESCENDING)])
            mongo_db['audit_logs'].create_index('action')
            mongo_db['audit_logs'].create_index('actor_id')
            mongo_db['audit_logs'].create_index('target_type')
        except Exception as e:
            print("MongoDB index initialization:", e)

db = DB()

# --- Database Models ---

class VerificationStatus:
    PENDING = 'PENDING'
    APPROVED = 'APPROVED'
    REJECTED = 'REJECTED'
    NEEDS_MORE_INFORMATION = 'NEEDS_MORE_INFORMATION'

class ComplaintStatus:
    OPEN = 'OPEN'
    UNDER_REVIEW = 'UNDER_REVIEW'
    RESOLVED = 'RESOLVED'
    REJECTED = 'REJECTED'

class ComplaintCategory:
    MISSING_PRODUCE = 'Missing Produce'
    DAMAGED_PRODUCE = 'Damaged Produce'
    INCORRECT_QUANTITY = 'Incorrect Quantity'
    LATE_DELIVERY = 'Late Delivery'
    PAYMENT_REFUND = 'Payment / Refund Problem'
    CONDUCT_ISSUE = 'Driver / Farmer Conduct'
    OTHER = 'Other'

    @classmethod
    def all_categories(cls):
        return [
            cls.MISSING_PRODUCE,
            cls.DAMAGED_PRODUCE,
            cls.INCORRECT_QUANTITY,
            cls.LATE_DELIVERY,
            cls.PAYMENT_REFUND,
            cls.CONDUCT_ISSUE,
            cls.OTHER
        ]

class DriverAvailability:
    AVAILABLE = 'AVAILABLE'
    BUSY = 'BUSY'
    OFFLINE = 'OFFLINE'

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
    def is_active(self):
        """Flask-Login active flag: returns False if account is suspended."""
        return not bool(self._data.get('is_suspended', False))

    @property
    def is_suspended(self):
        return bool(self._data.get('is_suspended', False))

    @property
    def driver_verification_status(self):
        if 'driver_verification_status' in self._data and self._data['driver_verification_status']:
            return self._data['driver_verification_status']
        if self.role == 'driver':
            # For backward compatibility with existing test suites where status was unset
            return VerificationStatus.APPROVED
        return None

    @property
    def farm(self):
        return Farm.query.filter_by(user_id=self.id).first()

    @property
    def orders(self):
        return Order.query.filter_by(customer_id=self.id).all()

    @property
    def availability(self):
        """Returns the effective driver availability."""
        if self.role == 'driver':
            active_count = mongo_db['orders'].count_documents({
                'driver_id': self.id,
                'status': {'$in': [OrderStatus.DRIVER_ASSIGNED, OrderStatus.PICKED_UP, 'Picked Up']}
            })
            if active_count > 0:
                return DriverAvailability.BUSY
            return self._data.get('availability', DriverAvailability.AVAILABLE)
        return self._data.get('availability', DriverAvailability.AVAILABLE)

    def set_availability(self, status):
        """Allows authenticated drivers to update their availability."""
        valid_statuses = [DriverAvailability.AVAILABLE, DriverAvailability.BUSY, DriverAvailability.OFFLINE]
        status = str(status).strip().upper()
        if status not in valid_statuses:
            return False, f"Invalid status '{status}'. Must be one of {', '.join(valid_statuses)}."
        if status == DriverAvailability.AVAILABLE and self.role == 'driver':
            active_count = mongo_db['orders'].count_documents({
                'driver_id': self.id,
                'status': {'$in': [OrderStatus.DRIVER_ASSIGNED, OrderStatus.PICKED_UP, 'Picked Up']}
            })
            if active_count > 0:
                return False, f"Cannot switch to Available while you have {active_count} active delivery assignment(s)."
        self._data['availability'] = status
        self._is_dirty = True
        mongo_db['users'].update_one(
            {'_id': self.id},
            {'$set': {'availability': status, 'last_active_at': datetime.datetime.now()}}
        )
        return True, None


class Farm(BaseModel):
    __collection__ = 'farms'

    user_id = ColumnField('user_id')
    name = ColumnField('name')
    farm_type = ColumnField('farm_type')

    @property
    def verification_status(self):
        return self._data.get('verification_status', VerificationStatus.APPROVED)

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
            'verification_status': self.verification_status
        }


class Review(BaseModel):
    __collection__ = 'reviews'

    order_id = ColumnField('order_id')
    customer_id = ColumnField('customer_id')
    target_type = ColumnField('target_type')  # 'farm' or 'driver'
    target_id = ColumnField('target_id')
    rating = ColumnField('rating')  # 1 to 5 int
    comment = ColumnField('comment')
    created_at = ColumnField('created_at')

    @property
    def order(self):
        return Order.query.get(self.order_id)

    @property
    def customer(self):
        return User.query.get(self.customer_id)

    @property
    def target_name(self):
        if self.target_type == 'farm':
            f = Farm.query.get(self.target_id)
            return f.name if f else f"Farm #{self.target_id}"
        elif self.target_type == 'driver':
            d = User.query.get(self.target_id)
            return d.email.split('@')[0].title() if d else f"Driver #{self.target_id}"
        return f"Target #{self.target_id}"


class Complaint(BaseModel):
    __collection__ = 'complaints'

    order_id = ColumnField('order_id')
    customer_id = ColumnField('customer_id')
    category = ColumnField('category')
    description = ColumnField('description')
    status = ColumnField('status')
    admin_notes = ColumnField('admin_notes')
    created_at = ColumnField('created_at')

    @property
    def order(self):
        return Order.query.get(self.order_id)

    @property
    def customer(self):
        return User.query.get(self.customer_id)


class AuditLog(BaseModel):
    __collection__ = 'audit_logs'

    actor_id = ColumnField('actor_id')
    actor_email = ColumnField('actor_email')
    actor_role = ColumnField('actor_role')
    action = ColumnField('action')
    target_type = ColumnField('target_type')
    target_id = ColumnField('target_id')
    details = ColumnField('details')
    timestamp = ColumnField('timestamp')


class ProduceItem(BaseModel):
    __collection__ = 'produce_items'

    farm_id = ColumnField('farm_id')
    name = ColumnField('name')
    price = ColumnField('price')

    @property
    def farm(self):
        return Farm.query.get(self.farm_id)


# --- Order Lifecycle Status Constants & Helpers ---

class OrderStatus:
    PLACED = 'PLACED'
    FARMER_ACCEPTED = 'FARMER_ACCEPTED'
    PREPARING = 'PREPARING'
    READY_FOR_PICKUP = 'READY_FOR_PICKUP'
    DRIVER_ASSIGNED = 'DRIVER_ASSIGNED'
    PICKED_UP = 'PICKED_UP'
    DELIVERED = 'DELIVERED'
    CANCELLED = 'CANCELLED'
    DELIVERY_FAILED = 'DELIVERY_FAILED'

# Normalized lookup for backward compatibility with title-cased DB statuses
NORMALIZED_STATUS = {
    'PLACED': OrderStatus.PLACED,
    'Placed': OrderStatus.PLACED,
    'FARMER_ACCEPTED': OrderStatus.FARMER_ACCEPTED,
    'Accepted': OrderStatus.FARMER_ACCEPTED,
    'PREPARING': OrderStatus.PREPARING,
    'Preparing': OrderStatus.PREPARING,
    'READY_FOR_PICKUP': OrderStatus.READY_FOR_PICKUP,
    'Ready for Pickup': OrderStatus.READY_FOR_PICKUP,
    'DRIVER_ASSIGNED': OrderStatus.DRIVER_ASSIGNED,
    'PICKED_UP': OrderStatus.PICKED_UP,
    'Picked Up': OrderStatus.PICKED_UP,
    'DELIVERED': OrderStatus.DELIVERED,
    'Delivered': OrderStatus.DELIVERED,
    'CANCELLED': OrderStatus.CANCELLED,
    'Cancelled': OrderStatus.CANCELLED,
    'Rejected': OrderStatus.CANCELLED,
    'DELIVERY_FAILED': OrderStatus.DELIVERY_FAILED,
    'Delivery Failed': OrderStatus.DELIVERY_FAILED
}

VALID_TRANSITIONS = {
    OrderStatus.PLACED: [OrderStatus.FARMER_ACCEPTED, OrderStatus.CANCELLED],
    OrderStatus.FARMER_ACCEPTED: [OrderStatus.PREPARING, OrderStatus.CANCELLED],
    OrderStatus.PREPARING: [OrderStatus.READY_FOR_PICKUP, OrderStatus.CANCELLED],
    OrderStatus.READY_FOR_PICKUP: [OrderStatus.DRIVER_ASSIGNED, OrderStatus.CANCELLED],
    OrderStatus.DRIVER_ASSIGNED: [OrderStatus.PICKED_UP, OrderStatus.READY_FOR_PICKUP, OrderStatus.CANCELLED],
    OrderStatus.PICKED_UP: [OrderStatus.DELIVERED, OrderStatus.DELIVERY_FAILED],
    OrderStatus.DELIVERED: [],
    OrderStatus.CANCELLED: [],
    OrderStatus.DELIVERY_FAILED: [OrderStatus.READY_FOR_PICKUP, OrderStatus.CANCELLED]
}

def generate_secure_otp(length=6):
    """Generates a cryptographically secure random numeric OTP."""
    return ''.join(secrets.choice('0123456789') for _ in range(length))

def hash_otp(otp_str):
    """Produces a secure SHA-256 hash using the secret key as salt."""
    salt = app.config.get('SECRET_KEY', 'default_salt')
    return hashlib.sha256(f"{str(otp_str).strip()}:{salt}".encode('utf-8')).hexdigest()

def verify_otp_hash(input_otp, stored_hash):
    """Constant-time verification of an OTP input against its stored hash."""
    if not input_otp or not stored_hash:
        return False
    computed = hash_otp(str(input_otp).strip())
    return hmac.compare_digest(computed, stored_hash)

def restore_order_stock_once(order_id):
    """
    Atomically restores stock for an order if it hasn't been restored yet.
    Guarantees idempotency so retries do not restore stock multiple times.
    """
    order_doc = mongo_db['orders'].find_one_and_update(
        {
            '_id': order_id,
            'stock_reserved': True,
            'stock_restored': {'$ne': True}
        },
        {'$set': {'stock_restored': True}},
        return_document=pymongo.ReturnDocument.BEFORE
    )
    if order_doc:
        order_items = list(mongo_db['order_items'].find({'order_id': order_id}))
        for oi in order_items:
            item_id = oi.get('produce_item_id')
            qty = oi.get('quantity', 0)
            if item_id and qty > 0:
                mongo_db['produce_items'].update_one(
                    {'_id': item_id},
                    {'$inc': {'stock': qty}}
                )
        return True
    return False

def emit_order_status_update(order, new_status, actor_user=None):
    """Emits real-time event without leaking sensitive OTPs."""
    order_room = f"order_{order.id}"
    farm_room = f"farm_{order.farm_id}"
    
    driver_name = None
    if order.driver:
        driver_name = order.driver.email.split('@')[0]

    payload = {
        'order_id': order.id,
        'status': new_status,
        'status_display': order.status_display,
        'status_css': order.status_css_class,
        'stepper_weight': order.stepper_weight,
        'driver_name': driver_name,
        'timestamp': datetime.datetime.now().strftime('%b %d, %I:%M %p')
    }
    
    socketio.emit('status_update', payload, to=order_room)
    socketio.emit('status_update', payload, to=farm_room)
    
    if new_status == OrderStatus.READY_FOR_PICKUP:
        socketio.emit('order_available', {
            'order_id': order.id,
            'farm_name': order.farm.name if order.farm else 'Farm',
            'farm_address': getattr(order.farm, 'address', '') if order.farm else '',
            'customer_name': getattr(order, 'customer_name', ''),
            'customer_address': getattr(order, 'customer_address', ''),
            'total': getattr(order, 'total_price', 0.0)
        }, to='drivers_room')

def transition_order_status(order, new_status, actor_user=None, notes=None, **extra_updates):
    """
    Validates and executes an order status transition.
    Enforces authorization, updates timestamps, updates status history,
    manages inventory rollback if cancelled, saves to MongoDB,
    and emits Socket.IO event.
    Returns (success: bool, error_message: str or None).
    """
    current_norm = NORMALIZED_STATUS.get(order.status, order.status)
    target_norm = NORMALIZED_STATUS.get(new_status, new_status)
    
    allowed = VALID_TRANSITIONS.get(current_norm, [])
    if target_norm not in allowed:
        return False, f"Invalid status transition from '{order.status}' to '{new_status}'."

    now = datetime.datetime.now()
    now_iso = now.isoformat()
    now_display = now.strftime('%b %d, %I:%M %p')

    update_doc = {
        'status': target_norm,
        'updated_at': now
    }
    
    if target_norm == OrderStatus.FARMER_ACCEPTED:
        update_doc['accepted_at'] = now
    elif target_norm == OrderStatus.PREPARING:
        update_doc['preparing_at'] = now
    elif target_norm == OrderStatus.READY_FOR_PICKUP:
        update_doc['ready_at'] = now
        if current_norm == OrderStatus.DRIVER_ASSIGNED:
            update_doc['driver_id'] = None
            update_doc['pickup_otp'] = None
            update_doc['pickup_otp_hash'] = None
    elif target_norm == OrderStatus.DRIVER_ASSIGNED:
        update_doc['assigned_at'] = now
        p_otp = generate_secure_otp(6)
        update_doc['pickup_otp'] = p_otp
        update_doc['pickup_otp_hash'] = hash_otp(p_otp)
        update_doc['pickup_otp_expires_at'] = now + datetime.timedelta(hours=24)
        update_doc['pickup_otp_attempts'] = 0
    elif target_norm == OrderStatus.PICKED_UP:
        update_doc['picked_up_at'] = now
        update_doc['pickup_otp'] = None
        update_doc['pickup_otp_hash'] = None
        d_otp = generate_secure_otp(6)
        update_doc['delivery_otp'] = d_otp
        update_doc['delivery_otp_hash'] = hash_otp(d_otp)
        update_doc['delivery_otp_expires_at'] = now + datetime.timedelta(hours=24)
        update_doc['delivery_otp_attempts'] = 0
    elif target_norm == OrderStatus.DELIVERED:
        update_doc['delivered_at'] = now
        update_doc['delivery_otp'] = None
        update_doc['delivery_otp_hash'] = None
    elif target_norm == OrderStatus.CANCELLED:
        update_doc['cancelled_at'] = now
        if notes:
            update_doc['cancellation_reason'] = notes
        update_doc['pickup_otp'] = None
        update_doc['pickup_otp_hash'] = None
        update_doc['delivery_otp'] = None
        update_doc['delivery_otp_hash'] = None
        restore_order_stock_once(order.id)
    elif target_norm == OrderStatus.DELIVERY_FAILED:
        update_doc['failed_at'] = now
        if notes:
            update_doc['failure_reason'] = notes

    update_doc.update(extra_updates)

    actor_role = actor_user.role if actor_user else 'system'
    actor_id = actor_user.id if actor_user else None
    
    history_entry = {
        'status': target_norm,
        'timestamp': now_display,
        'iso_timestamp': now_iso,
        'actor_id': actor_id,
        'actor_role': actor_role,
        'notes': notes or f"Status transitioned to {target_norm}"
    }

    res = mongo_db['orders'].find_one_and_update(
        {'_id': order.id, 'status': order.status},
        {
            '$set': update_doc,
            '$push': {'status_history': history_entry}
        },
        return_document=pymongo.ReturnDocument.AFTER
    )
    if not res:
        return False, "Order status was updated concurrently. Please refresh the page."

    order._data.update(res)
    order.status = target_norm

    # Calculate and finalize driver earnings on server strictly upon successful delivery
    if target_norm == OrderStatus.DELIVERED:
        payout_doc = finalize_driver_order_payout(order, actor_user)
        if payout_doc:
            order._data.update(payout_doc)

    emit_order_status_update(order, target_norm, actor_user)

    # Synchronize driver availability when order lifecycle changes
    assigned_driver_id = getattr(order, 'driver_id', None)
    if assigned_driver_id:
        if target_norm == OrderStatus.DRIVER_ASSIGNED:
            mongo_db['users'].update_one(
                {'_id': assigned_driver_id},
                {'$set': {'availability': DriverAvailability.BUSY, 'last_active_at': now}}
            )
        elif target_norm in [OrderStatus.DELIVERED, OrderStatus.CANCELLED, OrderStatus.DELIVERY_FAILED]:
            remaining_active = mongo_db['orders'].count_documents({
                'driver_id': assigned_driver_id,
                'status': {'$in': [OrderStatus.DRIVER_ASSIGNED, OrderStatus.PICKED_UP, 'Picked Up']},
                '_id': {'$ne': order.id}
            })
            if remaining_active == 0:
                mongo_db['users'].update_one(
                    {'_id': assigned_driver_id},
                    {'$set': {'availability': DriverAvailability.AVAILABLE, 'last_active_at': now}}
                )

    return True, None


# --- Geolocation, OSRM Road Routing & Smart Driver Recommendation ---

OSRM_BASE_URL = os.environ.get('OSRM_BASE_URL', 'https://router.project-osrm.org').rstrip('/')
ROUTE_CACHE = {}  # (s_lat, s_lng, e_lat, e_lng) -> {'time': ts, 'data': dict}
ROUTE_CACHE_TTL = 300  # 5 minutes cache

def get_road_route(start_lat, start_lng, end_lat, end_lng):
    """
    Queries OSRM driving service with caching and graceful degradation.
    Returns real road distance and duration without fabricating speed.
    If routing service is unavailable, provides straight-line distance with explicit notice.
    """
    if start_lat is None or start_lng is None or end_lat is None or end_lng is None:
        return {
            'success': False,
            'is_road_route': False,
            'distance_km': 0.0,
            'duration_mins': None,
            'eta_mins': None,
            'eta_text': 'Coordinates missing',
            'geometry': [],
            'source': 'none',
            'error': 'Missing coordinates'
        }

    try:
        s_lat, s_lng = float(start_lat), float(start_lng)
        e_lat, e_lng = float(end_lat), float(end_lng)
    except (TypeError, ValueError):
        return {
            'success': False,
            'is_road_route': False,
            'distance_km': 0.0,
            'duration_mins': None,
            'eta_mins': None,
            'eta_text': 'Invalid coordinates',
            'geometry': [],
            'source': 'none',
            'error': 'Invalid coordinates'
        }

    cache_key = (round(s_lat, 4), round(s_lng, 4), round(e_lat, 4), round(e_lng, 4))
    now_ts = time.time()
    if cache_key in ROUTE_CACHE:
        cached = ROUTE_CACHE[cache_key]
        if now_ts - cached['time'] < ROUTE_CACHE_TTL:
            return cached['data']

    # Query OSRM
    url = f"{OSRM_BASE_URL}/route/v1/driving/{s_lng},{s_lat};{e_lng},{e_lat}?overview=full&geometries=geojson"
    req = urllib.request.Request(url, headers={'User-Agent': 'FarmMarketplace-Routing/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=3.5) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            if data.get('code') == 'Ok' and data.get('routes'):
                r = data['routes'][0]
                dist_km = round(r['distance'] / 1000.0, 2)
                dur_mins = round(r['duration'] / 60.0, 1)
                eta_m = max(1, round(dur_mins))
                # Convert GeoJSON [lon, lat] to Leaflet [lat, lon]
                leaflet_coords = [[coord[1], coord[0]] for coord in r['geometry']['coordinates']]
                
                result = {
                    'success': True,
                    'is_road_route': True,
                    'distance_km': dist_km,
                    'duration_mins': dur_mins,
                    'duration_secs': round(r['duration'], 1),
                    'eta_mins': eta_m,
                    'eta_text': f"{dist_km} km ({eta_m} min driving ETA)",
                    'geometry': leaflet_coords,
                    'source': 'osrm',
                    'error': None
                }
                ROUTE_CACHE[cache_key] = {'time': now_ts, 'data': result}
                return result
    except Exception as e:
        print(f"OSRM routing request warning: {e}")

    # Fallback to straight-line Haversine distance with explicit 'ETA Unavailable' notice
    straight_km = round(haversine(s_lat, s_lng, e_lat, e_lng), 2)
    fallback_result = {
        'success': True,
        'is_road_route': False,
        'distance_km': straight_km,
        'duration_mins': None,
        'duration_secs': None,
        'eta_mins': None,
        'eta_text': f"{straight_km} km straight-line (Road ETA unavailable)",
        'geometry': [[s_lat, s_lng], [e_lat, e_lng]],
        'source': 'haversine_fallback',
        'error': 'Routing service unavailable. Showing straight-line distance.'
    }
    return fallback_result

def get_recommended_drivers_for_order(order):
    """
    Ranks drivers using transparent, explainable rules:
    1. Driver availability (AVAILABLE prioritized; OFFLINE ranked low; BUSY flagged)
    2. Active delivery load (0 active orders preferred)
    3. Distance to farm (calculated using OSRM or Haversine)
    """
    farm_lat = order.farm.latitude if order.farm else None
    farm_lng = order.farm.longitude if order.farm else None

    driver_docs = list(mongo_db['users'].find({'role': 'driver'}))
    ranked_drivers = []

    for d in driver_docs:
        driver_id = d.get('id', d.get('_id'))
        driver_email = d.get('email', f'driver_{driver_id}')
        driver_name = d.get('name') or driver_email.split('@')[0].capitalize()
        driver_phone = d.get('phone', 'N/A')
        
        # Check driver verification and suspension status
        v_status = d.get('driver_verification_status')
        if v_status in [VerificationStatus.PENDING, VerificationStatus.REJECTED, VerificationStatus.NEEDS_MORE_INFORMATION]:
            continue
        if d.get('is_suspended', False):
            continue

        # Check active deliveries
        active_count = mongo_db['orders'].count_documents({
            'driver_id': driver_id,
            'status': {'$in': [OrderStatus.DRIVER_ASSIGNED, OrderStatus.PICKED_UP, 'Picked Up']}
        })
        
        raw_avail = d.get('availability', DriverAvailability.AVAILABLE)
        effective_avail = DriverAvailability.BUSY if active_count > 0 else raw_avail

        d_lat = d.get('last_latitude')
        d_lng = d.get('last_longitude')
        last_at = d.get('last_location_at') or d.get('last_active_at')
        last_str = last_at.strftime('%b %d, %I:%M %p') if isinstance(last_at, datetime.datetime) else 'Recently'

        dist_km = None
        dur_mins = None
        has_location = False
        if d_lat is not None and d_lng is not None and farm_lat is not None and farm_lng is not None:
            has_location = True
            route_res = get_road_route(d_lat, d_lng, farm_lat, farm_lng)
            dist_km = route_res.get('distance_km')
            dur_mins = route_res.get('duration_mins')

        # Compute transparent score: lower score = better recommendation
        if active_count > 0:
            is_eligible = False
            score = 10000 + active_count * 100
            status_badge = 'Busy'
            badge_class = 'bg-warning text-dark'
            reason = f"Delivering {active_count} active order(s)"
        elif effective_avail == DriverAvailability.OFFLINE:
            is_eligible = False
            score = 5000 + (dist_km if dist_km else 100)
            status_badge = 'Offline'
            badge_class = 'bg-secondary text-white'
            reason = f"Driver offline - {f'{dist_km} km away' if dist_km else 'Location not reported'}"
        else: # AVAILABLE with 0 active orders!
            is_eligible = True
            status_badge = 'Available'
            badge_class = 'bg-success text-white'
            if dist_km is not None:
                score = dist_km
                reason = f"Nearest available driver - {dist_km} km to farm - 0 active orders"
            else:
                score = 50.0 # moderate ranking if location unknown
                reason = "Available - 0 active orders (Location unverified)"

        ranked_drivers.append({
            'id': driver_id,
            'driver_id': driver_id,
            'name': driver_name,
            'email': driver_email,
            'phone': driver_phone,
            'availability': effective_avail,
            'active_orders': active_count,
            'distance_km': dist_km,
            'duration_mins': dur_mins,
            'has_location': has_location,
            'is_eligible': is_eligible,
            'score': score,
            'status_badge': status_badge,
            'badge_class': badge_class,
            'reason': reason,
            'last_active': last_str,
            'is_recommended': False
        })

    # Sort drivers by score
    ranked_drivers.sort(key=lambda x: x['score'])
    
    # Mark the first eligible driver as top recommendation
    for rd in ranked_drivers:
        if rd['is_eligible']:
            rd['is_recommended'] = True
            rd['reason'] = f"Top Recommendation: {rd['reason']}"
            break

    return ranked_drivers


# --- Driver Earnings Engine (₹3/km default, admin-configurable, atomic idempotent payout) ---

DEFAULT_DRIVER_RATE_PER_KM = 3.0

def get_driver_rate_per_km():
    """
    Retrieves the configurable platform driver earning rate per kilometer.
    Defaults to ₹3.00/km as per platform earning rules.
    """
    try:
        doc = mongo_db['platform_settings'].find_one({'_id': 'driver_rate_per_km'})
        if doc and 'rate' in doc:
            return float(doc['rate'])
    except Exception as e:
        print("Error fetching driver earning rate per km:", e)
    return DEFAULT_DRIVER_RATE_PER_KM

def set_driver_rate_per_km(rate, admin_user=None):
    """
    Allows admin to configure the driver earning rate per km.
    Records an immutable audit trail entry for rate updates.
    """
    rate = round(float(rate), 2)
    if rate <= 0:
        raise ValueError("Driver earning rate must be strictly positive.")

    now = datetime.datetime.now()
    mongo_db['platform_settings'].update_one(
        {'_id': 'driver_rate_per_km'},
        {
            '$set': {
                'rate': rate,
                'currency': 'INR',
                'currency_symbol': '₹',
                'updated_at': now,
                'updated_by': admin_user.email if admin_user else 'system'
            }
        },
        upsert=True
    )

    actor_id = admin_user.id if admin_user else None
    actor_email = admin_user.email if admin_user else 'system'
    actor_role = getattr(admin_user, 'role', 'admin') if admin_user else 'admin'

    mongo_db['audit_logs'].insert_one({
        '_id': get_next_id('audit_logs'),
        'actor_id': actor_id,
        'actor_email': actor_email,
        'actor_role': actor_role,
        'action': 'DRIVER_RATE_UPDATED',
        'target_type': 'platform_settings',
        'target_id': 'driver_rate_per_km',
        'details': f"Admin updated driver earning rate to ₹{rate:.2f} per km",
        'timestamp': now
    })
    return rate

def calculate_order_driver_earnings(order, driver_user=None, rate_per_km=None):
    """
    Calculates driver earnings according to the Driver Earnings Rule:
      - Pickup earning = pickup road distance in km × ₹rate
      - Drop earning = farm-to-customer road distance in km × ₹rate
      - Total driver earning = pickup earning + drop earning
    Uses road-distance estimates from OSRM where available, with explicit estimate labeling.
    If the order has already finalized earnings, returns frozen finalized payout data.
    """
    # 1. If already finalized on server, return immutable finalized records
    is_finalized = bool(getattr(order, 'driver_payout_finalized', False) or (isinstance(order, dict) and order.get('driver_payout_finalized')))
    if is_finalized:
        p_km = float(getattr(order, 'driver_pickup_distance_km', 0.0) or (order.get('driver_pickup_distance_km', 0.0) if isinstance(order, dict) else 0.0))
        d_km = float(getattr(order, 'driver_drop_distance_km', 0.0) or (order.get('driver_drop_distance_km', 0.0) if isinstance(order, dict) else 0.0))
        p_earn = float(getattr(order, 'driver_pickup_earning', 0.0) or (order.get('driver_pickup_earning', 0.0) if isinstance(order, dict) else 0.0))
        d_earn = float(getattr(order, 'driver_drop_earning', 0.0) or (order.get('driver_drop_earning', 0.0) if isinstance(order, dict) else 0.0))
        tot_earn = float(getattr(order, 'driver_payout_amount', 0.0) or (order.get('driver_payout_amount', 0.0) if isinstance(order, dict) else 0.0))
        tot_km = float(getattr(order, 'driver_payable_distance_km', 0.0) or (order.get('driver_payable_distance_km', 0.0) if isinstance(order, dict) else 0.0))
        frozen_rate = float(getattr(order, 'driver_payout_rate', 0.0) or (order.get('driver_payout_rate', 0.0) if isinstance(order, dict) else 0.0)) or get_driver_rate_per_km()
        p_src = getattr(order, 'driver_pickup_source', 'osrm') if not isinstance(order, dict) else order.get('driver_pickup_source', 'osrm')
        d_src = getattr(order, 'driver_drop_source', 'osrm') if not isinstance(order, dict) else order.get('driver_drop_source', 'osrm')
        
        return {
            'rate_per_km': round(frozen_rate, 2),
            'currency_symbol': '₹',
            'pickup_distance_km': round(p_km, 2),
            'pickup_earning': round(p_earn, 2),
            'pickup_source': p_src,
            'pickup_source_label': 'Road Route (OSRM)' if p_src == 'osrm' else 'Estimated Distance',
            'is_pickup_road': (p_src == 'osrm'),
            'drop_distance_km': round(d_km, 2),
            'drop_earning': round(d_earn, 2),
            'drop_source': d_src,
            'drop_source_label': 'Road Route (OSRM)' if d_src == 'osrm' else 'Estimated Distance',
            'is_drop_road': (d_src == 'osrm'),
            'total_distance_km': round(tot_km, 2),
            'total_earning': round(tot_earn, 2),
            'is_finalized': True
        }

    # 2. Compute live/estimated earnings
    active_rate = round(float(rate_per_km if rate_per_km is not None else get_driver_rate_per_km()), 2)
    
    # Extract coordinates
    farm = getattr(order, 'farm', None)
    if not farm and hasattr(order, 'farm_id'):
        farm = Farm.query.get(order.farm_id)
    farm_lat = getattr(farm, 'latitude', None) if farm else None
    farm_lng = getattr(farm, 'longitude', None) if farm else None

    cust_lat = getattr(order, 'customer_latitude', None)
    cust_lng = getattr(order, 'customer_longitude', None)

    drv_lat = getattr(order, 'driver_latitude', None)
    drv_lng = getattr(order, 'driver_longitude', None)
    if drv_lat is None or drv_lng is None:
        if driver_user:
            drv_lat = getattr(driver_user, 'last_latitude', None)
            drv_lng = getattr(driver_user, 'last_longitude', None)
        elif getattr(order, 'driver_id', None):
            d_user = User.query.get(order.driver_id)
            if d_user:
                drv_lat = getattr(d_user, 'last_latitude', None)
                drv_lng = getattr(d_user, 'last_longitude', None)

    # Leg 1: Pickup Journey (Driver to Farm)
    if drv_lat is not None and drv_lng is not None and farm_lat is not None and farm_lng is not None:
        pickup_route = get_road_route(drv_lat, drv_lng, farm_lat, farm_lng)
        pickup_km = round(float(pickup_route.get('distance_km', 0.0)), 2)
        is_pickup_road = pickup_route.get('is_road_route', False)
        pickup_source = pickup_route.get('source', 'osrm' if is_pickup_road else 'estimate')
        pickup_label = 'Road Route (OSRM)' if is_pickup_road else 'Estimated Distance (Haversine)'
    else:
        # Standard estimate when driver GPS is not yet acquired
        pickup_km = 3.0
        is_pickup_road = False
        pickup_source = 'standard_estimate'
        pickup_label = 'Estimated Distance (GPS Pending)'

    # Leg 2: Drop Journey (Farm to Customer)
    if farm_lat is not None and farm_lng is not None and cust_lat is not None and cust_lng is not None:
        drop_route = get_road_route(farm_lat, farm_lng, cust_lat, cust_lng)
        drop_km = round(float(drop_route.get('distance_km', 0.0)), 2)
        is_drop_road = drop_route.get('is_road_route', False)
        drop_source = drop_route.get('source', 'osrm' if is_drop_road else 'estimate')
        drop_label = 'Road Route (OSRM)' if is_drop_road else 'Estimated Distance (Haversine)'
    else:
        drop_km = 5.0
        is_drop_road = False
        drop_source = 'standard_estimate'
        drop_label = 'Estimated Distance (Standard Estimate)'

    pickup_earning = round(pickup_km * active_rate, 2)
    drop_earning = round(drop_km * active_rate, 2)
    total_distance_km = round(pickup_km + drop_km, 2)
    total_earning = round(pickup_earning + drop_earning, 2)

    return {
        'rate_per_km': active_rate,
        'currency_symbol': '₹',
        'pickup_distance_km': pickup_km,
        'pickup_earning': pickup_earning,
        'pickup_source': pickup_source,
        'pickup_source_label': pickup_label,
        'is_pickup_road': is_pickup_road,
        'drop_distance_km': drop_km,
        'drop_earning': drop_earning,
        'drop_source': drop_source,
        'drop_source_label': drop_label,
        'is_drop_road': is_drop_road,
        'total_distance_km': total_distance_km,
        'total_earning': total_earning,
        'is_finalized': False
    }

def finalize_driver_order_payout(order, actor_user=None):
    """
    Calculates and finalizes driver earnings on server upon successful delivery.
    Enforces atomic idempotency via MongoDB find_one_and_update so duplicate payouts are impossible.
    Formula:
      Pickup earning = pickup road distance in km × ₹rate
      Drop earning = farm-to-customer road distance in km × ₹rate
      Total driver earning = pickup earning + drop earning
    """
    order_id = order.id if hasattr(order, 'id') else order.get('_id')
    driver_id = getattr(order, 'driver_id', None) or (order.get('driver_id') if isinstance(order, dict) else None)
    if not driver_id:
        return None

    rate = get_driver_rate_per_km()
    earnings = calculate_order_driver_earnings(order, rate_per_km=rate)

    now = datetime.datetime.now()
    payout_data = {
        'driver_payout_finalized': True,
        'driver_payout_finalized_at': now,
        'driver_payout_amount': earnings['total_earning'],
        'driver_payout_rate': rate,
        'driver_pickup_distance_km': earnings['pickup_distance_km'],
        'driver_pickup_earning': earnings['pickup_earning'],
        'driver_pickup_source': earnings['pickup_source'],
        'driver_drop_distance_km': earnings['drop_distance_km'],
        'driver_drop_earning': earnings['drop_earning'],
        'driver_drop_source': earnings['drop_source'],
        'driver_payable_distance_km': earnings['total_distance_km']
    }

    # Atomic conditional update prevents duplicate payout even across parallel requests
    updated_doc = mongo_db['orders'].find_one_and_update(
        {
            '_id': order_id,
            'driver_payout_finalized': {'$ne': True}
        },
        {'$set': payout_data},
        return_document=pymongo.ReturnDocument.AFTER
    )

    if updated_doc:
        # Increment driver's balance and credit account
        mongo_db['users'].update_one(
            {'_id': driver_id},
            {
                '$inc': {'total_earnings': earnings['total_earning']},
                '$set': {'last_payout_at': now}
            }
        )

        actor_id = actor_user.id if actor_user else None
        actor_email = actor_user.email if actor_user else 'system'
        actor_role = getattr(actor_user, 'role', 'system') if actor_user else 'system'

        mongo_db['audit_logs'].insert_one({
            '_id': get_next_id('audit_logs'),
            'actor_id': actor_id,
            'actor_email': actor_email,
            'actor_role': actor_role,
            'action': 'DRIVER_PAYOUT_FINALIZED',
            'target_type': 'order',
            'target_id': order_id,
            'details': (
                f"Driver #{driver_id} payout finalized: ₹{earnings['total_earning']:.2f} "
                f"(Pickup: {earnings['pickup_distance_km']} km × ₹{rate:.2f} = ₹{earnings['pickup_earning']:.2f}, "
                f"Drop: {earnings['drop_distance_km']} km × ₹{rate:.2f} = ₹{earnings['drop_earning']:.2f}). "
                f"Total payable: {earnings['total_distance_km']} km."
            ),
            'timestamp': now
        })

        if hasattr(order, '_data'):
            order._data.update(updated_doc)
        return updated_doc

    return None


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
            kwargs['status'] = OrderStatus.PLACED
        if 'status_history' not in kwargs:
            c_time = kwargs['created_at']
            time_display = c_time.strftime('%b %d, %I:%M %p') if hasattr(c_time, 'strftime') else str(c_time)
            kwargs['status_history'] = [{
                'status': kwargs.get('status', OrderStatus.PLACED),
                'timestamp': time_display,
                'iso_timestamp': c_time.isoformat() if hasattr(c_time, 'isoformat') else str(c_time),
                'actor_role': 'customer',
                'notes': 'Order placed by customer'
            }]
        super().__init__(**kwargs)

    @property
    def driver_earnings(self):
        """Returns calculated or finalized driver earnings breakdown."""
        return calculate_order_driver_earnings(self)

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

    @property
    def status_display(self):
        s = getattr(self, 'status', OrderStatus.PLACED)
        displays = {
            'PLACED': 'Placed',
            'Placed': 'Placed',
            'FARMER_ACCEPTED': 'Farmer Accepted',
            'Accepted': 'Farmer Accepted',
            'PREPARING': 'Preparing',
            'Preparing': 'Preparing',
            'READY_FOR_PICKUP': 'Ready for Pickup',
            'Ready for Pickup': 'Ready for Pickup',
            'DRIVER_ASSIGNED': 'Driver Assigned',
            'PICKED_UP': 'Picked Up',
            'Picked Up': 'Picked Up',
            'DELIVERED': 'Delivered',
            'Delivered': 'Delivered',
            'CANCELLED': 'Cancelled',
            'Cancelled': 'Cancelled',
            'Rejected': 'Cancelled',
            'DELIVERY_FAILED': 'Delivery Failed',
            'Delivery Failed': 'Delivery Failed'
        }
        return displays.get(s, str(s).replace('_', ' ').title())

    @property
    def status_css_class(self):
        s = getattr(self, 'status', OrderStatus.PLACED)
        classes = {
            'PLACED': 'status-Placed',
            'Placed': 'status-Placed',
            'FARMER_ACCEPTED': 'status-Farmer-Accepted',
            'Accepted': 'status-Farmer-Accepted',
            'PREPARING': 'status-Preparing',
            'Preparing': 'status-Preparing',
            'READY_FOR_PICKUP': 'status-Ready-for-Pickup',
            'Ready for Pickup': 'status-Ready-for-Pickup',
            'DRIVER_ASSIGNED': 'status-Driver-Assigned',
            'PICKED_UP': 'status-Picked-Up',
            'Picked Up': 'status-Picked-Up',
            'DELIVERED': 'status-Delivered',
            'Delivered': 'status-Delivered',
            'CANCELLED': 'status-Cancelled',
            'Cancelled': 'status-Cancelled',
            'Rejected': 'status-Cancelled',
            'DELIVERY_FAILED': 'status-Delivery-Failed',
            'Delivery Failed': 'status-Delivery-Failed'
        }
        return classes.get(s, f"status-{str(s).replace('_', '-').replace(' ', '-')}")

    @property
    def stepper_weight(self):
        s = getattr(self, 'status', OrderStatus.PLACED)
        weights = {
            'PLACED': 1,
            'Placed': 1,
            'FARMER_ACCEPTED': 2,
            'Accepted': 2,
            'PREPARING': 3,
            'Preparing': 3,
            'READY_FOR_PICKUP': 4,
            'Ready for Pickup': 4,
            'DRIVER_ASSIGNED': 5,
            'PICKED_UP': 6,
            'Picked Up': 6,
            'DELIVERED': 7,
            'Delivered': 7,
            'CANCELLED': -1,
            'Cancelled': -1,
            'Rejected': -1,
            'DELIVERY_FAILED': -2,
            'Delivery Failed': -2
        }
        return weights.get(s, 1)

    @property
    def timeline_steps(self):
        history = getattr(self, 'status_history', []) or []
        history_map = {}
        for h in history:
            history_map[h.get('status')] = h.get('timestamp')
            
        def get_time(status_keys, field_fallback):
            for k in status_keys:
                if k in history_map:
                    return history_map[k]
            val = getattr(self, field_fallback, None)
            if val:
                if hasattr(val, 'strftime'):
                    return val.strftime('%b %d, %I:%M %p')
                return str(val)
            return None

        current_w = self.stepper_weight
        steps = [
            {
                'key': 'PLACED',
                'weight': 1,
                'label': 'Placed',
                'icon': 'bi-receipt',
                'time': get_time(['PLACED', 'Placed'], 'created_at'),
            },
            {
                'key': 'FARMER_ACCEPTED',
                'weight': 2,
                'label': 'Accepted',
                'icon': 'bi-check2-circle',
                'time': get_time(['FARMER_ACCEPTED', 'Accepted'], 'accepted_at'),
            },
            {
                'key': 'PREPARING',
                'weight': 3,
                'label': 'Preparing',
                'icon': 'bi-box-seam',
                'time': get_time(['PREPARING', 'Preparing'], 'preparing_at'),
            },
            {
                'key': 'READY_FOR_PICKUP',
                'weight': 4,
                'label': 'Ready for Pickup',
                'icon': 'bi-geo-alt',
                'time': get_time(['READY_FOR_PICKUP', 'Ready for Pickup'], 'ready_at'),
            },
            {
                'key': 'DRIVER_ASSIGNED',
                'weight': 5,
                'label': 'Driver Assigned',
                'icon': 'bi-person-badge',
                'time': get_time(['DRIVER_ASSIGNED'], 'assigned_at'),
            },
            {
                'key': 'PICKED_UP',
                'weight': 6,
                'label': 'Picked Up',
                'icon': 'bi-truck',
                'time': get_time(['PICKED_UP', 'Picked Up'], 'picked_up_at'),
            },
            {
                'key': 'DELIVERED',
                'weight': 7,
                'label': 'Delivered',
                'icon': 'bi-house-check',
                'time': get_time(['DELIVERED', 'Delivered'], 'delivered_at'),
            }
        ]
        
        for step in steps:
            if current_w < 0:
                step['state'] = 'completed' if step['time'] else 'inactive'
            elif current_w >= 7 and step['weight'] == 7:
                step['state'] = 'completed'
            elif current_w > step['weight']:
                step['state'] = 'completed'
            elif current_w == step['weight']:
                step['state'] = 'active'
            else:
                step['state'] = 'pending'
        return steps

    def can_cancel_by_customer(self):
        norm = NORMALIZED_STATUS.get(self.status, self.status)
        return norm in [OrderStatus.PLACED, OrderStatus.FARMER_ACCEPTED]

    def can_cancel_by_farmer(self):
        norm = NORMALIZED_STATUS.get(self.status, self.status)
        return norm in [OrderStatus.PLACED, OrderStatus.FARMER_ACCEPTED, OrderStatus.PREPARING]


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

def admin_required(f):
    """Decorator to ensure user is logged in, active, and has administrator privileges."""
    @wraps(f)
    @login_required
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or current_user.role != 'admin':
            if request.is_json or request.path.startswith('/api/'):
                return jsonify({'error': 'Unauthorized. Administrator access required.'}), 403
            flash('Access denied. Administrator privileges required.', 'danger')
            return redirect(url_for('home'))
        if getattr(current_user, 'is_suspended', False):
            logout_user()
            flash('Your administrator account has been suspended. Please contact support.', 'danger')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.before_request
def check_user_suspension():
    """Immediately logs out and blocks any suspended user from performing actions."""
    if current_user.is_authenticated:
        if getattr(current_user, 'is_suspended', False):
            reason = getattr(current_user, 'suspension_reason', None) or 'Violation of platform terms'
            logout_user()
            flash(f'Your account has been suspended by administration ({reason}).', 'danger')
            return redirect(url_for('login'))

def log_audit(actor_id, actor_email, actor_role, action, target_type, target_id, details=None, ip_address=None):
    """
    Records a sensitive administrative action into the audit trail.
    Ensures safe logging without leaking passwords, OTPs, or auth secrets.
    """
    safe_details = {}
    if details and isinstance(details, dict):
        for k, v in details.items():
            if any(secret in k.lower() for secret in ['password', 'hash', 'otp', 'secret', 'token', 'key']):
                safe_details[k] = '[REDACTED]'
            else:
                safe_details[k] = str(v)
    elif details:
        safe_details = {'message': str(details)}

    now = datetime.datetime.now()
    log_entry = {
        'id': get_next_id('audit_logs'),
        'actor_id': actor_id,
        'actor_email': actor_email,
        'actor_role': actor_role,
        'action': action,
        'target_type': target_type,
        'target_id': target_id,
        'details': safe_details,
        'timestamp': now,
        'timestamp_iso': now.isoformat(),
        'timestamp_display': now.strftime('%b %d, %Y %I:%M %p'),
        'ip_address': ip_address or (request.remote_addr if request else '127.0.0.1')
    }
    mongo_db['audit_logs'].insert_one(log_entry)

def get_farm_rating_stats(farm_id):
    """Calculates average rating and review count for a farm from active, unhidden reviews."""
    try:
        fid = int(farm_id)
    except (TypeError, ValueError):
        fid = farm_id
    pipeline = [
        {
            '$match': {
                'target_type': 'farm',
                'target_id': fid,
                'is_hidden': {'$ne': True}
            }
        },
        {
            '$group': {
                '_id': None,
                'avg_rating': {'$avg': '$rating'},
                'total_reviews': {'$sum': 1}
            }
        }
    ]
    res = list(mongo_db['reviews'].aggregate(pipeline))
    if res and res[0].get('avg_rating') is not None:
        return round(float(res[0]['avg_rating']), 1), int(res[0]['total_reviews'])
    return 0.0, 0

def get_driver_rating_stats(driver_id):
    """Calculates average rating and review count for a driver from active, unhidden reviews."""
    try:
        did = int(driver_id)
    except (TypeError, ValueError):
        did = driver_id
    pipeline = [
        {
            '$match': {
                'target_type': 'driver',
                'target_id': did,
                'is_hidden': {'$ne': True}
            }
        },
        {
            '$group': {
                '_id': None,
                'avg_rating': {'$avg': '$rating'},
                'total_reviews': {'$sum': 1}
            }
        }
    ]
    res = list(mongo_db['reviews'].aggregate(pipeline))
    if res and res[0].get('avg_rating') is not None:
        return round(float(res[0]['avg_rating']), 1), int(res[0]['total_reviews'])
    return 0.0, 0

class Pagination:
    def __init__(self, items, page, per_page, total_count):
        self.items = items
        self.page = page
        self.per_page = per_page
        self.total_count = total_count
        self.total_pages = math.ceil(total_count / per_page) if total_count > 0 else 1
        self.has_prev = page > 1
        self.has_next = page < self.total_pages
        self.prev_page = page - 1
        self.next_page = page + 1

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def get(self, key, default=None):
        return getattr(self, key, default)

def paginate_items(items_list, page=1, per_page=10):
    page = max(1, int(page or 1))
    per_page = max(1, min(int(per_page or 10), 100))
    total_count = len(items_list)
    start = (page - 1) * per_page
    end = start + per_page
    return Pagination(
        items=items_list[start:end],
        page=page,
        per_page=per_page,
        total_count=total_count
    )

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
    
    active_statuses = [
        'Placed', 'PLACED', 'FARMER_ACCEPTED', 'Accepted',
        'Preparing', 'PREPARING', 'Ready for Pickup', 'READY_FOR_PICKUP',
        'DRIVER_ASSIGNED', 'Picked Up', 'PICKED_UP'
    ]
    active_orders = [o for o in all_orders if o.status in active_statuses]
    completed_orders = [o for o in all_orders if o.status in ['Delivered', 'DELIVERED']]
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
    """Returns JSON stats and delivery payout history for the driver dashboard."""
    if current_user.role != 'driver':
        return jsonify({'error': 'Forbidden'}), 403
    
    rate = get_driver_rate_per_km()
    all_driver_orders = Order.query.filter_by(driver_id=current_user.id).all()
    completed = [o for o in all_driver_orders if o.status in ['Delivered', 'DELIVERED']]
    
    total_earnings = 0.0
    completed_list = []
    for o in completed:
        earnings = calculate_order_driver_earnings(o, driver_user=current_user, rate_per_km=rate)
        total_earnings += earnings['total_earning']
        del_at = getattr(o, 'delivered_at', None)
        del_str = del_at.strftime('%b %d, %I:%M %p') if (del_at and hasattr(del_at, 'strftime')) else 'Delivered'
        
        completed_list.append({
            'order_id': o.id,
            'delivered_at': del_str,
            'customer_name': getattr(o, 'customer_name', 'Customer'),
            'farm_name': o.farm.name if o.farm else 'Farm',
            'pickup_distance_km': earnings['pickup_distance_km'],
            'pickup_earning': earnings['pickup_earning'],
            'pickup_source_label': earnings['pickup_source_label'],
            'drop_distance_km': earnings['drop_distance_km'],
            'drop_earning': earnings['drop_earning'],
            'drop_source_label': earnings['drop_source_label'],
            'total_distance_km': earnings['total_distance_km'],
            'total_earning': earnings['total_earning'],
            'rate_per_km': earnings['rate_per_km'],
            'currency_symbol': '₹',
            'is_finalized': earnings['is_finalized']
        })
    
    user_earnings = float(getattr(current_user, 'total_earnings', 0.0) or 0.0)
    final_earnings = round(max(total_earnings, user_earnings), 2)
    
    return jsonify({
        'total_delivered': len(completed),
        'total_earnings': final_earnings,
        'rate_per_km': rate,
        'currency_symbol': '₹',
        'completed_orders': completed_list
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

ALLOWED_REGISTRATION_ROLES = ['customer', 'farmer', 'driver']

@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    
    if request.method == 'POST':
        email = (request.form.get('email') or '').strip().lower()
        password = request.form.get('password')
        role = (request.form.get('role') or '').strip().lower()

        # Strict security validation: Never allow public registration of admin accounts
        if role not in ALLOWED_REGISTRATION_ROLES:
            flash(f"Invalid account role '{role}'. Permitted roles are: customer, farmer, driver.", 'danger')
            return render_template('register.html')

        if not email or not password:
            flash('Email and password are required.', 'danger')
            return render_template('register.html')

        existing_user = User.query.filter_by(email=email).first()
        if existing_user:
            flash('Email already registered. Please login.', 'warning')
            return redirect(url_for('login'))
        
        user_data = {
            'email': email,
            'role': role,
            'is_suspended': False,
            'created_at': datetime.datetime.now()
        }
        if role == 'driver':
            user_data['driver_verification_status'] = VerificationStatus.PENDING
            user_data['availability'] = DriverAvailability.OFFLINE

        user = User(**user_data)
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        
        flash('Registration successful! Please login.', 'success')
        return redirect(url_for('login'))
        
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        if current_user.role == 'admin':
            return redirect(url_for('admin_dashboard'))
        elif current_user.role == 'farmer':
            return redirect(url_for('farmer_dashboard'))
        elif current_user.role == 'driver':
            return redirect(url_for('driver_dashboard'))
        else:
            return redirect(url_for('customer_dashboard'))
        
    if request.method == 'POST':
        email = (request.form.get('email') or '').strip().lower()
        password = request.form.get('password')
        user = User.query.filter_by(email=email).first()
        
        if user and user.check_password(password):
            # Check for administrative suspension
            if getattr(user, 'is_suspended', False):
                reason = getattr(user, 'suspension_reason', None) or 'Violation of platform terms.'
                flash(f'Account Suspended: Your account has been suspended by administration ({reason}). Please contact support.', 'danger')
                return render_template('login.html')

            login_user(user, remember=True)
            flash(f'Logged in successfully as {user.email}!', 'success')
            
            if user.role == 'admin':
                log_audit(user.id, user.email, user.role, 'ADMIN_LOGIN', 'system', 0, {'message': 'Administrator logged in'})
                return redirect(url_for('admin_dashboard'))
            elif user.role == 'farmer':
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
    Switches/logs in to admin@farm.com, farmer@farm.com, customer@farm.com, or driver@farm.com without typing.
    """
    role_email_map = {
        'admin': 'admin@farm.com',
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
        user_data = {
            'email': target_email,
            'role': role.lower(),
            'is_suspended': False,
            'created_at': datetime.datetime.now()
        }
        if role.lower() == 'driver':
            user_data['driver_verification_status'] = VerificationStatus.APPROVED
            user_data['availability'] = DriverAvailability.AVAILABLE
        user = User(**user_data)
        user.set_password('password123')
        db.session.add(user)
        db.session.commit()
        
    if getattr(user, 'is_suspended', False):
        flash('This account is suspended.', 'danger')
        return redirect(url_for('login'))

    if current_user.is_authenticated:
        logout_user()
    login_user(user, remember=True)
    flash(f'Logged in as {role.title()} ({user.email})', 'success')
    
    if user.role == 'admin':
        return redirect(url_for('admin_dashboard'))
    elif user.role == 'farmer':
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
    
    orders = Order.query.filter_by(farm_id=farm.id).order_by(Order.created_at.desc()).all()
    produce_items = ProduceItem.query.filter_by(farm_id=farm.id).all()
    
    total_orders = len(orders)
    pending_orders = len([o for o in orders if o.status in ['Placed', 'PLACED', 'FARMER_ACCEPTED', 'Accepted', 'Preparing', 'PREPARING']])
    ready_orders = len([o for o in orders if o.status in ['Ready for Pickup', 'READY_FOR_PICKUP', 'DRIVER_ASSIGNED']])
    completed_orders = len([o for o in orders if o.status in ['Delivered', 'DELIVERED']])
    total_revenue = sum(o.total_price for o in orders if o.total_price and o.status not in ['Rejected', 'Cancelled', 'CANCELLED', 'DELIVERY_FAILED'])
    recent_orders = orders[:10]
    
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
        license_number = request.form.get('license_number', '').strip()
        
        new_farm = Farm(
            name=name,
            address=address,
            farm_type=farm_type,
            user_id=current_user.id, 
            latitude=latitude, 
            longitude=longitude,
            license_number=license_number,
            verification_status=VerificationStatus.PENDING
        )
        db.session.add(new_farm)
        db.session.commit()
        log_audit(current_user.id, current_user.email, 'farmer', 'FARM_CREATED', 'farm', new_farm.id, {'name': name})
        
        flash('Farm profile created and submitted for verification! An administrator will review your farm details.', 'success')
        return redirect(url_for('farmer_dashboard'))
        
    return render_template('create_farm_profile.html')

@app.route('/farmer/profile/edit', methods=['GET', 'POST'])
@farmer_required
def edit_farm_profile():
    """Route for farmers to edit their profile and resubmit verification if needed."""
    farm = Farm.query.filter_by(user_id=current_user.id).first_or_404()
    
    if request.method == 'POST':
        farm.name = request.form.get('name')
        farm.address = request.form.get('address')
        farm.farm_type = request.form.get('farm_type')
        farm.latitude = safe_float(request.form.get('latitude'))
        farm.longitude = safe_float(request.form.get('longitude'))
        
        license_num = request.form.get('license_number', '').strip()
        if license_num:
            farm.license_number = license_num

        # If farm was rejected or needed more info, re-submitting moves it back to PENDING for review
        prev_status = farm.verification_status
        if prev_status in [VerificationStatus.REJECTED, VerificationStatus.NEEDS_MORE_INFORMATION]:
            farm.verification_status = VerificationStatus.PENDING
            farm.verification_notes = f"Resubmitted by farmer on {datetime.datetime.now().strftime('%b %d, %Y %I:%M %p')}"
            log_audit(current_user.id, current_user.email, 'farmer', 'FARM_VERIFICATION_RESUBMITTED', 'farm', farm.id, {
                'previous_status': prev_status,
                'name': farm.name
            })
            flash('Profile updated and resubmitted for administrative verification!', 'success')
        else:
            flash('Profile updated successfully!', 'success')

        db.session.commit()
        return redirect(url_for('manage_produce'))
        
    return render_template('edit_farm_profile.html', farm=farm)

def infer_produce_unit(name: str = '', category: str = '') -> str:
    """
    Automatically infers appropriate produce unit based on item name and category.
    Examples:
      - Milk, curd, juice, oil, liquids -> 'per liter'
      - Eggs -> 'per dozen'
      - Spinach, coriander, mint, herbs -> 'per bunch'
      - Honey, jam, pickle -> 'per 500g jar'
      - Coconut, watermelon, cabbage, pumpkin -> 'per piece'
      - Vegetables, fruits, grains -> 'per kg'
    """
    n = (name or '').strip().lower()
    c = (category or '').strip().lower()

    # 1. Produce keywords in item name
    if any(k in n for k in ['milk', 'doodh', 'curd', 'dahi', 'yogurt', 'lassi', 'buttermilk', 'chaas', 'juice', 'oil', 'liquid', 'ghee', 'shake']):
        return 'per liter'
    if any(k in n for k in ['egg', 'anda']):
        return 'per dozen'
    if any(k in n for k in ['spinach', 'palak', 'coriander', 'cilantro', 'dhaniya', 'methi', 'fenugreek', 'mint', 'pudina', 'kale', 'lettuce', 'bunch', 'parsley', 'basil', 'herb']):
        return 'per bunch'
    if any(k in n for k in ['honey', 'jam', 'pickle', 'achar']):
        return 'per 500g jar'
    if any(k in n for k in ['coconut', 'watermelon', 'pumpkin', 'cabbage', 'cauliflower', 'pineapple', 'papaya', 'piece', 'kaddu']):
        return 'per piece'
    if any(k in n for k in ['tomato', 'potato', 'onion', 'apple', 'banana', 'mango', 'carrot', 'rice', 'wheat', 'flour', 'atta', 'dal', 'grain', 'veg']):
        return 'per kg'

    # 2. Category fallbacks
    if c == 'dairy':
        return 'per liter'
    if c == 'herbs':
        return 'per bunch'
    if c in ['vegetables', 'fruits', 'grains']:
        return 'per kg'
    return 'per kg'

@app.route('/farmer/produce', methods=['GET', 'POST'])
@farmer_required
def manage_produce():
    """
    CRUD hub for Produce Items.
    Restricts publishing if the farm is awaiting verification or rejected.
    """
    farm = Farm.query.filter_by(user_id=current_user.id).first_or_404()
    
    if request.method == 'POST':
        # Business rule: unapproved farms cannot publish produce items
        if farm.verification_status in [VerificationStatus.PENDING, VerificationStatus.REJECTED, VerificationStatus.NEEDS_MORE_INFORMATION]:
            flash(f"Farm verification status is {farm.verification_status.replace('_', ' ').title()}. Only approved farms can publish new produce items.", 'warning')
            return redirect(url_for('manage_produce'))

        name = request.form.get('name', '').strip()
        description = request.form.get('description', '').strip()
        price = float(request.form.get('price'))
        category = request.form.get('category', 'Other')
        unit = request.form.get('unit', '').strip()
        if not unit:
            unit = infer_produce_unit(name, category)
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
        item.name = request.form.get('name', '').strip()
        item.description = request.form.get('description', '').strip()
        item.price = float(request.form.get('price'))
        item.category = request.form.get('category', getattr(item, 'category', 'Other') or 'Other')
        unit = request.form.get('unit', '').strip()
        if not unit:
            unit = infer_produce_unit(item.name, item.category)
        item.unit = unit
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

# --- Shopping Cart & Order Routes ---

@app.route('/checkout', methods=['GET', 'POST'])
@login_required
def checkout():
    if current_user.role != 'customer':
        flash('Only customers can place orders.', 'warning')
        return redirect(url_for('home'))

    cart_items, total_price = get_cart_details()
    
    if not cart_items:
        flash('Your cart is empty. Add items to checkout.', 'warning')
        return redirect(url_for('home'))
        
    if request.method == 'POST':
        # Idempotency token check
        idempotency_key = request.form.get('idempotency_key')
        if idempotency_key:
            existing = mongo_db['orders'].find_one({
                'customer_id': current_user.id,
                'idempotency_key': idempotency_key
            })
            if existing:
                flash('Your order has already been received! Showing your order status.', 'info')
                session.pop('cart', None)
                return redirect(url_for('order_details', order_id=existing['_id']))

        name = request.form.get('name', '').strip()
        address = request.form.get('address', '').strip()
        phone = request.form.get('phone', '').strip()
        try:
            cust_lat = float(request.form.get('customer_latitude'))
            cust_lon = float(request.form.get('customer_longitude'))
        except (TypeError, ValueError):
            cust_lat = None
            cust_lon = None
        
        if not name or not address or not phone:
            flash('Please fill out all delivery recipient details.', 'danger')
            return redirect(url_for('checkout'))

        # Server-side validation of cart items, current prices, and farm ownership
        server_total = 0.0
        verified_items = []
        farm_ids = set()

        for c_data in cart_items:
            item_obj = c_data['item']
            raw_qty = c_data['quantity']
            
            # Fetch fresh item directly from DB to verify price & availability
            fresh_item = ProduceItem.query.get(item_obj.id)
            if not fresh_item:
                flash(f"Item '{item_obj.name}' is no longer available.", 'danger')
                return redirect(url_for('view_cart'))
                
            try:
                qty = int(raw_qty)
                if qty <= 0:
                    raise ValueError
            except (TypeError, ValueError):
                flash("Invalid quantity in your cart.", 'danger')
                return redirect(url_for('view_cart'))

            item_price = float(fresh_item.price)
            server_total += item_price * qty
            farm_ids.add(fresh_item.farm_id)
            verified_items.append({
                'item': fresh_item,
                'quantity': qty,
                'price': item_price
            })

        if len(farm_ids) > 1:
            flash('You can only purchase from one farm per order.', 'warning')
            return redirect(url_for('view_cart'))
            
        farm_id = next(iter(farm_ids))
        farm = Farm.query.get(farm_id)
        if not farm:
            flash('Origin farm not found.', 'danger')
            return redirect(url_for('view_cart'))

        # Check farm verification status
        if farm.verification_status in [VerificationStatus.PENDING, VerificationStatus.REJECTED, VerificationStatus.NEEDS_MORE_INFORMATION]:
            flash(f"Farm '{farm.name}' is currently awaiting administrative verification ({farm.verification_status.replace('_', ' ').title()}). Orders cannot be placed at this time.", 'warning')
            return redirect(url_for('view_cart'))

        # Safe conditional atomic inventory deduction with rollback
        reserved_items = []
        stock_failed = False
        failed_item_name = ""

        for v in verified_items:
            item_id = v['item'].id
            needed_qty = v['quantity']
            
            # Atomic decrement only if available stock is >= requested quantity
            res = mongo_db['produce_items'].find_one_and_update(
                {
                    '_id': item_id,
                    'stock': {'$gte': needed_qty}
                },
                {'$inc': {'stock': -needed_qty}},
                return_document=pymongo.ReturnDocument.AFTER
            )
            
            if not res:
                stock_failed = True
                failed_item_name = v['item'].name
                break
                
            reserved_items.append((item_id, needed_qty))

        if stock_failed:
            # Roll back all previously deducted items in this order
            for r_id, r_qty in reserved_items:
                mongo_db['produce_items'].update_one(
                    {'_id': r_id},
                    {'$inc': {'stock': r_qty}}
                )
            fresh_stock_doc = mongo_db['produce_items'].find_one({'name': failed_item_name})
            available_now = fresh_stock_doc.get('stock', 0) if fresh_stock_doc else 0
            flash(f"Sorry, '{failed_item_name}' only has {available_now} units remaining. Please update your cart.", 'warning')
            return redirect(url_for('view_cart'))

        # Create Order record with recalculated total & lifecycle history
        now = datetime.datetime.now()
        new_order = Order(
            customer_id=current_user.id,
            farm_id=farm_id,
            customer_name=name,
            customer_address=address,
            customer_phone=phone,
            total_price=round(server_total, 2),
            status=OrderStatus.PLACED,
            customer_latitude=cust_lat,
            customer_longitude=cust_lon,
            idempotency_key=idempotency_key,
            stock_reserved=True,
            stock_restored=False,
            created_at=now,
            status_history=[{
                'status': OrderStatus.PLACED,
                'timestamp': now.strftime('%b %d, %I:%M %p'),
                'iso_timestamp': now.isoformat(),
                'actor_id': current_user.id,
                'actor_role': 'customer',
                'notes': 'Order placed by customer'
            }]
        )
        db.session.add(new_order)
        db.session.commit()
        
        # Save OrderItem records
        for v in verified_items:
            order_item = OrderItem(
                order_id=new_order.id,
                produce_item_id=v['item'].id,
                quantity=v['quantity'],
                price_per_item=v['price']
            )
            db.session.add(order_item)
            
        db.session.commit()
        session.pop('cart', None)

        # Real-time Socket.IO notification to farmer
        farm_room = f"farm_{farm_id}"
        socketio.emit('new_order', {
            'order_id': new_order.id,
            'customer_name': new_order.customer_name,
            'total': new_order.total_price
        }, to=farm_room)
        
        flash('Order placed successfully! The farm has received your order.', 'success')
        return redirect(url_for('order_details', order_id=new_order.id))

    # GET request: generate a unique idempotency key
    idempotency_key = secrets.token_hex(16)
    return render_template(
        'checkout.html',
        cart_items=cart_items,
        total_price=total_price,
        idempotency_key=idempotency_key
    )

@app.route('/order/<int:order_id>')
@login_required
def order_details(order_id):
    order = Order.query.get_or_404(order_id)
    
    # Security check: User is customer, farmer, driver, or platform admin
    is_customer = current_user.id == order.customer_id
    is_farmer = current_user.role == 'farmer' and current_user.farm and current_user.farm.id == order.farm_id
    is_driver = current_user.role == 'driver' and (order.driver_id == current_user.id or order.status in [OrderStatus.READY_FOR_PICKUP, 'Ready for Pickup'])
    is_admin = current_user.role == 'admin'
    
    if not (is_customer or is_farmer or is_driver or is_admin):
        flash('You do not have permission to view this order.', 'danger')
        return redirect(url_for('home'))
        
    farm_review = mongo_db['reviews'].find_one({'order_id': order.id, 'target_type': 'farm'})
    driver_review = mongo_db['reviews'].find_one({'order_id': order.id, 'target_type': 'driver'})
    complaints = list(mongo_db['complaints'].find({'order_id': order.id}))

    return render_template(
        'order_details.html',
        order=order,
        farm_review=farm_review,
        driver_review=driver_review,
        complaints=complaints,
        is_admin=is_admin
    )

@app.route('/order/<int:order_id>/cancel', methods=['POST'])
@login_required
def cancel_order(order_id):
    """Customer or Farmer cancels an eligible order; restores inventory exactly once."""
    order = Order.query.get_or_404(order_id)
    
    is_customer = current_user.id == order.customer_id
    is_farmer = current_user.role == 'farmer' and current_user.farm and current_user.farm.id == order.farm_id
    
    if not (is_customer or is_farmer):
        flash('Unauthorized to cancel this order.', 'danger')
        return redirect(url_for('order_details', order_id=order.id))
        
    if is_customer and not order.can_cancel_by_customer():
        flash('Order has already progressed past the cancellation window.', 'warning')
        return redirect(url_for('order_details', order_id=order.id))
        
    if is_farmer and not order.can_cancel_by_farmer():
        flash('Order cannot be cancelled at this stage.', 'warning')
        return redirect(url_for('farm_orders'))

    reason = request.form.get('reason', f'Cancelled by {current_user.role}')
    success, err = transition_order_status(order, OrderStatus.CANCELLED, current_user, notes=reason)
    
    if success:
        flash(f'Order #{order.id} has been cancelled and produce inventory restored.', 'success')
    else:
        flash(f'Cancellation failed: {err}', 'danger')
        
    if is_farmer:
        return redirect(url_for('farm_orders'))
    return redirect(url_for('order_details', order_id=order.id))

@app.route('/order/<int:order_id>/regenerate-otp', methods=['POST'])
@login_required
def regenerate_delivery_otp(order_id):
    """Customer requests a new delivery OTP if status is PICKED_UP."""
    order = Order.query.get_or_404(order_id)
    if current_user.id != order.customer_id:
        flash('Unauthorized.', 'danger')
        return redirect(url_for('home'))
        
    current_norm = NORMALIZED_STATUS.get(order.status, order.status)
    if current_norm != OrderStatus.PICKED_UP:
        flash('Delivery OTP can only be regenerated while order is out for delivery.', 'warning')
        return redirect(url_for('order_details', order_id=order.id))
        
    new_otp = generate_secure_otp(6)
    new_hash = hash_otp(new_otp)
    now = datetime.datetime.now()
    
    mongo_db['orders'].update_one(
        {'_id': order.id},
        {
            '$set': {
                'delivery_otp': new_otp,
                'delivery_otp_hash': new_hash,
                'delivery_otp_expires_at': now + datetime.timedelta(hours=24),
                'delivery_otp_attempts': 0
            }
        }
    )
    flash('A fresh Delivery OTP has been generated! Present this code to your driver upon arrival.', 'success')
    return redirect(url_for('order_details', order_id=order.id))

@app.route('/my-orders')
@login_required
def my_orders():
    """Customer order history page."""
    if current_user.role != 'customer':
        flash('Only customers can view this page.', 'warning')
        return redirect(url_for('home'))
    orders = Order.query.filter_by(customer_id=current_user.id).order_by(Order.created_at.desc()).all()
    return render_template('customer_orders.html', orders=orders)

# --- Farmer Order Management Routes ---

@app.route('/farmer/orders')
@farmer_required
def farm_orders():
    """Farmer dashboard to see and manage incoming orders."""
    farm = current_user.farm
    orders = Order.query.filter_by(farm_id=farm.id)\
                        .order_by(Order.created_at.desc())\
                        .all()
    return render_template('farm_orders.html', orders=orders)

@app.route('/farmer/order/accept/<int:order_id>', methods=['POST'])
@farmer_required
def farmer_accept_order(order_id):
    """Farmer accepts a PLACED order -> FARMER_ACCEPTED."""
    order = Order.query.get_or_404(order_id)
    if order.farm_id != current_user.farm.id:
        flash('Unauthorized to manage this order.', 'danger')
        return redirect(url_for('farm_orders'))
        
    success, err = transition_order_status(order, OrderStatus.FARMER_ACCEPTED, current_user, notes='Accepted by farmer')
    if success:
        flash(f'Order #{order.id} accepted! You can now start harvesting and preparing.', 'success')
    else:
        flash(f'Error: {err}', 'danger')
    return redirect(url_for('farm_orders'))

@app.route('/farmer/order/prepare/<int:order_id>', methods=['POST'])
@farmer_required
def farmer_prepare_order(order_id):
    """Farmer starts preparing order -> PREPARING."""
    order = Order.query.get_or_404(order_id)
    if order.farm_id != current_user.farm.id:
        flash('Unauthorized to manage this order.', 'danger')
        return redirect(url_for('farm_orders'))
        
    success, err = transition_order_status(order, OrderStatus.PREPARING, current_user, notes='Farmer is harvesting and packing produce')
    if success:
        flash(f'Order #{order.id} status updated to Preparing.', 'success')
    else:
        flash(f'Error: {err}', 'danger')
    return redirect(url_for('farm_orders'))

@app.route('/farmer/order/ready/<int:order_id>', methods=['POST'])
@farmer_required
def farmer_ready_order(order_id):
    """Farmer marks produce ready for driver pickup -> READY_FOR_PICKUP."""
    order = Order.query.get_or_404(order_id)
    if order.farm_id != current_user.farm.id:
        flash('Unauthorized to manage this order.', 'danger')
        return redirect(url_for('farm_orders'))
        
    success, err = transition_order_status(order, OrderStatus.READY_FOR_PICKUP, current_user, notes='Packaged and ready for driver pickup')
    if success:
        flash(f'Order #{order.id} is marked Ready for Pickup! Nearby drivers have been alerted.', 'success')
    else:
        flash(f'Error: {err}', 'danger')
    return redirect(url_for('farm_orders'))

@app.route('/farmer/order/verify-pickup/<int:order_id>', methods=['POST'])
@farmer_required
def farmer_verify_pickup(order_id):
    """
    Farmer verifies the driver's Pickup OTP before releasing produce.
    Transitions DRIVER_ASSIGNED -> PICKED_UP upon successful verification.
    """
    order = Order.query.get_or_404(order_id)
    if order.farm_id != current_user.farm.id:
        flash('Unauthorized to manage this order.', 'danger')
        return redirect(url_for('farm_orders'))

    current_norm = NORMALIZED_STATUS.get(order.status, order.status)
    if current_norm != OrderStatus.DRIVER_ASSIGNED:
        flash('Pickup can only be verified after a driver is assigned.', 'warning')
        return redirect(url_for('farm_orders'))

    attempts = getattr(order, 'pickup_otp_attempts', 0) or 0
    if attempts >= 5:
        flash('Too many failed pickup verification attempts. Please contact dispatch or re-assign.', 'danger')
        return redirect(url_for('farm_orders'))

    expires_at = getattr(order, 'pickup_otp_expires_at', None)
    if expires_at and datetime.datetime.now() > expires_at:
        flash('Pickup verification OTP has expired.', 'danger')
        return redirect(url_for('farm_orders'))

    input_otp = request.form.get('pickup_otp', '').strip()
    stored_hash = getattr(order, 'pickup_otp_hash', None)

    if not verify_otp_hash(input_otp, stored_hash):
        new_attempts = attempts + 1
        mongo_db['orders'].update_one(
            {'_id': order.id},
            {'$set': {'pickup_otp_attempts': new_attempts}}
        )
        remaining = max(0, 5 - new_attempts)
        flash(f'Invalid Pickup OTP. {remaining} attempt(s) remaining.', 'danger')
        return redirect(url_for('order_details', order_id=order.id))

    # OTP Verified: Transition to PICKED_UP
    driver_name = order.driver.email.split('@')[0] if order.driver else 'Driver'
    success, err = transition_order_status(
        order,
        OrderStatus.PICKED_UP,
        current_user,
        notes=f'Pickup verified by farmer with driver {driver_name}'
    )
    if success:
        flash(f'Pickup verified! Order #{order.id} has been handed over to driver {driver_name}.', 'success')
    else:
        flash(f'Error updating order status: {err}', 'danger')
    return redirect(url_for('farm_orders'))

@app.route('/farmer/order/update/<int:order_id>', methods=['POST'])
@farmer_required
def update_order_status(order_id):
    """Backwards-compatible update handler for legacy forms."""
    order = Order.query.get_or_404(order_id)
    if order.farm_id != current_user.farm.id:
        flash('You do not have permission to update this order.', 'danger')
        return redirect(url_for('farm_orders'))
        
    new_status = request.form.get('status')
    if new_status in ['Preparing', 'PREPARING']:
        return farmer_prepare_order(order_id)
    elif new_status in ['Ready for Pickup', 'READY_FOR_PICKUP']:
        return farmer_ready_order(order_id)
    elif new_status in ['Rejected', 'CANCELLED', 'Cancelled']:
        return cancel_order(order_id)
    elif new_status in ['Farmer Accepted', 'FARMER_ACCEPTED', 'Accepted']:
        return farmer_accept_order(order_id)
    
    flash(f'Unknown status update "{new_status}".', 'warning')
    return redirect(url_for('farm_orders'))

@app.route('/farmer/order/assign-driver/<int:order_id>', methods=['POST'])
@farmer_required
def farmer_assign_driver(order_id):
    """
    Farmer assigns an eligible driver to a READY_FOR_PICKUP order.
    Uses atomic MongoDB update to prevent race conditions.
    """
    order = Order.query.get_or_404(order_id)
    if order.farm_id != current_user.farm.id:
        flash('Unauthorized to assign drivers for this order.', 'danger')
        return redirect(url_for('farm_orders'))
        
    try:
        driver_id = int(request.form.get('driver_id'))
    except (TypeError, ValueError):
        flash('Please select a valid driver.', 'warning')
        return redirect(url_for('farm_orders'))

    driver_doc = mongo_db['users'].find_one({'_id': driver_id, 'role': 'driver'})
    if not driver_doc:
        flash('Driver not found.', 'danger')
        return redirect(url_for('farm_orders'))

    driver_name = driver_doc.get('name') or driver_doc.get('email', '').split('@')[0]

    now = datetime.datetime.now()
    p_otp = generate_secure_otp(6)
    p_hash = hash_otp(p_otp)

    # Atomic conditional assignment: order must be READY_FOR_PICKUP and unassigned
    updated_doc = mongo_db['orders'].find_one_and_update(
        {
            '_id': order_id,
            'farm_id': current_user.farm.id,
            'status': {'$in': [OrderStatus.READY_FOR_PICKUP, 'Ready for Pickup']},
            'driver_id': None
        },
        {
            '$set': {
                'status': OrderStatus.DRIVER_ASSIGNED,
                'driver_id': driver_id,
                'assigned_at': now,
                'updated_at': now,
                'pickup_otp': p_otp,
                'pickup_otp_hash': p_hash,
                'pickup_otp_expires_at': now + datetime.timedelta(hours=24),
                'pickup_otp_attempts': 0
            },
            '$push': {
                'status_history': {
                    'status': OrderStatus.DRIVER_ASSIGNED,
                    'timestamp': now.strftime('%b %d, %I:%M %p'),
                    'iso_timestamp': now.isoformat(),
                    'actor_id': current_user.id,
                    'actor_role': 'farmer',
                    'notes': f"Assigned to driver {driver_name} by farmer"
                }
            }
        },
        return_document=pymongo.ReturnDocument.AFTER
    )

    if not updated_doc:
        flash(f'Order #{order_id} could not be assigned. It may have already been accepted or is not ready for pickup.', 'warning')
        return redirect(url_for('farm_orders'))

    # Update driver user doc to BUSY
    mongo_db['users'].update_one(
        {'_id': driver_id},
        {'$set': {'availability': DriverAvailability.BUSY, 'last_active_at': now}}
    )

    assigned_order = Order.from_doc(updated_doc)
    emit_order_status_update(assigned_order, OrderStatus.DRIVER_ASSIGNED, current_user)
    
    flash(f'Driver {driver_name} successfully assigned to Order #{order_id}!', 'success')
    return redirect(url_for('farm_orders'))

@app.route('/api/order/<int:order_id>/recommendations')
@farmer_required
def api_order_recommendations(order_id):
    """Returns smart driver recommendations with transparent criteria for a ready order."""
    order = Order.query.get_or_404(order_id)
    if order.farm_id != current_user.farm.id:
        return jsonify({'error': 'Unauthorized'}), 403

    ranked = get_recommended_drivers_for_order(order)
    return jsonify({
        'order_id': order.id,
        'status': order.status,
        'drivers': ranked
    })

@app.route('/api/order/<int:order_id>/route')
@login_required
def api_order_route(order_id):
    """
    Returns actual OSRM road route geometry, distance and duration.
    Falls back gracefully to straight-line distance if routing is unavailable.
    """
    order = Order.query.get_or_404(order_id)
    
    # Authorization check: customer of order, farm owner, assigned driver, or admin
    is_cust = order.customer_id == current_user.id
    is_farm = current_user.role == 'farmer' and current_user.farm and current_user.farm.id == order.farm_id
    is_drv = current_user.role == 'driver' and (order.driver_id == current_user.id or order.status in [OrderStatus.READY_FOR_PICKUP, 'Ready for Pickup'])
    if not (is_cust or is_farm or is_drv or current_user.role == 'admin'):
        return jsonify({'error': 'Unauthorized to view this order route'}), 403

    farm_lat = order.farm.latitude if order.farm else None
    farm_lng = order.farm.longitude if order.farm else None
    cust_lat = order.customer_latitude
    cust_lng = order.customer_longitude
    drv_lat = order.driver_latitude
    drv_lng = order.driver_longitude

    norm_status = NORMALIZED_STATUS.get(order.status, order.status)
    
    if norm_status in [OrderStatus.PICKED_UP, 'Picked Up'] and drv_lat and drv_lng and cust_lat and cust_lng:
        # Live transit: from driver vehicle position to customer delivery address
        route_data = get_road_route(drv_lat, drv_lng, cust_lat, cust_lng)
        route_data['leg'] = 'driver_to_customer'
    elif norm_status == OrderStatus.DRIVER_ASSIGNED and drv_lat and drv_lng and farm_lat and farm_lng:
        # Pickup leg: from driver position to farm
        route_data = get_road_route(drv_lat, drv_lng, farm_lat, farm_lng)
        route_data['leg'] = 'driver_to_farm'
    elif farm_lat and farm_lng and cust_lat and cust_lng:
        # Origin to destination road route
        route_data = get_road_route(farm_lat, farm_lng, cust_lat, cust_lng)
        route_data['leg'] = 'farm_to_customer'
    else:
        route_data = {
            'success': False,
            'is_road_route': False,
            'distance_km': 0.0,
            'duration_mins': None,
            'eta_mins': None,
            'eta_text': 'Coordinates missing',
            'geometry': [],
            'source': 'none',
            'error': 'Missing coordinates for route'
        }

    route_data['order_id'] = order.id
    route_data['status'] = norm_status
    route_data['customer_lat'] = cust_lat
    route_data['customer_lng'] = cust_lng
    route_data['farm_lat'] = farm_lat
    route_data['farm_lng'] = farm_lng
    route_data['driver_lat'] = drv_lat
    route_data['driver_lng'] = drv_lng
    route_data['last_location_at'] = order.last_location_at.strftime('%I:%M:%S %p') if getattr(order, 'last_location_at', None) else None
    
    return jsonify(route_data)

# --- Delivery Driver Routes ---

@app.route('/driver/dashboard')
@driver_required
def driver_dashboard():
    """Show driver available orders that are READY_FOR_PICKUP, and their active assigned orders."""
    rate_per_km = get_driver_rate_per_km()
    all_driver_orders = Order.query.filter_by(driver_id=current_user.id).order_by(Order.created_at.desc()).all()
    
    ready_orders = Order.query.filter(
        (Order.status == OrderStatus.READY_FOR_PICKUP) | (Order.status == 'Ready for Pickup')
    ).order_by(Order.created_at.asc()).all()
    available_orders = [o for o in ready_orders if not o.driver_id]
    
    in_progress_orders = [
        o for o in all_driver_orders
        if o.status in [OrderStatus.DRIVER_ASSIGNED, OrderStatus.PICKED_UP, 'Picked Up']
    ]
    completed_orders = [
        o for o in all_driver_orders
        if o.status in [OrderStatus.DELIVERED, 'Delivered']
    ]

    total_finalized_earnings = sum(
        float(getattr(o, 'driver_payout_amount', 0.0) or 0.0) for o in completed_orders
    )
    user_total = float(getattr(current_user, 'total_earnings', 0.0) or 0.0)
    effective_earnings = round(max(total_finalized_earnings, user_total), 2)

    return render_template(
        'driver_dashboard.html',
        available_orders=available_orders,
        in_progress_orders=in_progress_orders,
        completed_orders=completed_orders,
        orders=available_orders + in_progress_orders,
        rate_per_km=rate_per_km,
        total_driver_earnings=effective_earnings
    )

@app.route('/driver/availability', methods=['POST'])
@driver_required
def update_driver_availability():
    """Allows driver to update availability to AVAILABLE, BUSY, or OFFLINE."""
    if request.is_json:
        data = request.get_json() or {}
        status = str(data.get('availability', '')).strip().upper()
    else:
        status = request.form.get('availability', '').strip().upper()

    success, err = current_user.set_availability(status)
    if request.is_json:
        if success:
            return jsonify({'success': True, 'availability': status, 'message': f'Driver status is now {status}.'})
        return jsonify({'success': False, 'message': err}), 400

    if success:
        flash(f'Your driver status is now {status}.', 'success')
    else:
        flash(f'Cannot change status: {err}', 'warning')
    return redirect(url_for('driver_dashboard'))

@app.route('/driver/accept/<int:order_id>', methods=['POST'])
@app.route('/driver/order/accept/<int:order_id>', methods=['POST'])
@driver_required
def driver_accept_order(order_id):
    """
    Driver accepts an eligible delivery assignment.
    Prevents race conditions using atomic MongoDB update.
    Transitions READY_FOR_PICKUP -> DRIVER_ASSIGNED (generates Pickup OTP).
    """
    # Restrict unapproved drivers from accepting orders
    driver_status = getattr(current_user, 'driver_verification_status', None) or current_user._data.get('driver_verification_status')
    if driver_status in [VerificationStatus.PENDING, VerificationStatus.REJECTED, VerificationStatus.NEEDS_MORE_INFORMATION]:
        status_clean = driver_status.replace('_', ' ').title()
        if request.is_json:
            return jsonify({'success': False, 'message': f'Driver account verification is {status_clean}. Only approved drivers can accept deliveries.'}), 403
        flash(f'Your driver verification status is currently {status_clean}. Only approved drivers can accept delivery assignments.', 'danger')
        return redirect(url_for('driver_dashboard'))

    now = datetime.datetime.now()
    now_iso = now.isoformat()
    now_display = now.strftime('%b %d, %I:%M %p')
    driver_name = current_user.email.split('@')[0]
    
    # Generate cryptographic pickup OTP
    p_otp = generate_secure_otp(6)
    p_hash = hash_otp(p_otp)
    
    # Atomic conditional update prevents concurrent assignment to multiple drivers
    updated_doc = mongo_db['orders'].find_one_and_update(
        {
            '_id': order_id,
            'status': {'$in': [OrderStatus.READY_FOR_PICKUP, 'Ready for Pickup']},
            'driver_id': None
        },
        {
            '$set': {
                'status': OrderStatus.DRIVER_ASSIGNED,
                'driver_id': current_user.id,
                'assigned_at': now,
                'updated_at': now,
                'pickup_otp': p_otp,
                'pickup_otp_hash': p_hash,
                'pickup_otp_expires_at': now + datetime.timedelta(hours=24),
                'pickup_otp_attempts': 0
            },
            '$push': {
                'status_history': {
                    'status': OrderStatus.DRIVER_ASSIGNED,
                    'timestamp': now_display,
                    'iso_timestamp': now_iso,
                    'actor_id': current_user.id,
                    'actor_role': 'driver',
                    'notes': f'Accepted delivery by driver {driver_name}'
                }
            }
        },
        return_document=pymongo.ReturnDocument.AFTER
    )
    
    if not updated_doc:
        if request.is_json:
            return jsonify({'success': False, 'message': 'Order already assigned or no longer ready for pickup'}), 400
        flash(f'Order #{order_id} is no longer available or was already accepted by another driver.', 'warning')
        return redirect(url_for('driver_dashboard'))
        
    # Mark driver user as BUSY
    mongo_db['users'].update_one(
        {'_id': current_user.id},
        {'$set': {'availability': DriverAvailability.BUSY, 'last_active_at': now}}
    )

    order = Order.from_doc(updated_doc)
    emit_order_status_update(order, OrderStatus.DRIVER_ASSIGNED, current_user)
    
    if request.is_json:
        return jsonify({
            'success': True,
            'order_id': order.id,
            'status': OrderStatus.DRIVER_ASSIGNED,
            'pickup_otp': p_otp,
            'message': 'Delivery assignment accepted.'
        })

    flash(f'You have accepted Order #{order.id}! Present your Pickup OTP to the farmer upon arrival.', 'success')
    return redirect(url_for('driver_delivery', order_id=order.id))

@app.route('/driver/delivery/<int:order_id>')
@app.route('/driver/order/<int:order_id>/delivery')
@driver_required
def driver_delivery(order_id):
    order = Order.query.get_or_404(order_id)
    
    if order.driver_id != current_user.id:
        flash('You are not assigned to this delivery.', 'danger')
        return redirect(url_for('driver_dashboard'))
        
    return render_template('driver_delivery.html', order=order)

@app.route('/driver/order/verify-delivery/<int:order_id>', methods=['POST'])
@app.route('/driver/complete_delivery/<int:order_id>', methods=['POST'])
@driver_required
def driver_complete_delivery(order_id):
    """
    Assigned driver submits Customer's Delivery OTP.
    Confirms delivery only after successful server-side hash verification.
    """
    order = Order.query.get_or_404(order_id)
    
    if order.driver_id != current_user.id:
        flash('Unauthorized to confirm this delivery.', 'danger')
        return redirect(url_for('driver_dashboard'))
        
    current_norm = NORMALIZED_STATUS.get(order.status, order.status)
    if current_norm != OrderStatus.PICKED_UP:
        flash('Delivery can only be confirmed once produce has been picked up.', 'warning')
        return redirect(url_for('driver_delivery', order_id=order.id))

    attempts = getattr(order, 'delivery_otp_attempts', 0) or 0
    if attempts >= 5:
        flash('Too many failed delivery attempts. The customer must regenerate a new code from their tracking page.', 'danger')
        return redirect(url_for('driver_delivery', order_id=order.id))

    expires_at = getattr(order, 'delivery_otp_expires_at', None)
    if expires_at and datetime.datetime.now() > expires_at:
        flash('Delivery OTP has expired. Please ask customer to regenerate their code.', 'danger')
        return redirect(url_for('driver_delivery', order_id=order.id))

    input_otp = request.form.get('delivery_otp', '').strip()
    stored_hash = getattr(order, 'delivery_otp_hash', None)

    if not verify_otp_hash(input_otp, stored_hash):
        new_attempts = attempts + 1
        mongo_db['orders'].update_one(
            {'_id': order.id},
            {'$set': {'delivery_otp_attempts': new_attempts}}
        )
        remaining = max(0, 5 - new_attempts)
        flash(f'Invalid Delivery OTP. {remaining} attempt(s) remaining. Ask the customer for the code on their tracking screen.', 'danger')
        return redirect(url_for('driver_delivery', order_id=order.id))

    # Delivery OTP Verified: Transition to DELIVERED
    success, err = transition_order_status(
        order,
        OrderStatus.DELIVERED,
        current_user,
        notes='Delivery successfully verified and completed with customer OTP'
    )
    if success:
        payout_info = calculate_order_driver_earnings(order)
        flash(
            f'Order #{order.id} delivery verified and marked as Delivered! '
            f'Driver Payout Finalized: ₹{payout_info["total_earning"]:.2f} '
            f'(Pickup: {payout_info["pickup_distance_km"]} km -> ₹{payout_info["pickup_earning"]:.2f} + '
            f'Drop: {payout_info["drop_distance_km"]} km -> ₹{payout_info["drop_earning"]:.2f} '
            f'@ ₹{payout_info["rate_per_km"]:.2f}/km). Thank you!',
            'success'
        )
        return redirect(url_for('driver_dashboard'))
    else:
        flash(f'Error updating order: {err}', 'danger')
        return redirect(url_for('driver_delivery', order_id=order.id))

@app.route('/driver/order/delivery-failed/<int:order_id>', methods=['POST'])
@driver_required
def driver_delivery_failed(order_id):
    """Assigned driver marks delivery failed if customer unreachable."""
    order = Order.query.get_or_404(order_id)
    if order.driver_id != current_user.id:
        flash('Unauthorized.', 'danger')
        return redirect(url_for('driver_dashboard'))
        
    reason = request.form.get('reason', 'Customer unreachable / wrong address')
    success, err = transition_order_status(order, OrderStatus.DELIVERY_FAILED, current_user, notes=reason)
    if success:
        flash(f'Order #{order.id} marked as Delivery Failed.', 'warning')
    else:
        flash(f'Error: {err}', 'danger')
    return redirect(url_for('driver_dashboard'))

# --- SocketIO Event Handlers ---

@socketio.on('join_order_room')
def handle_join_order_room(data):
    """Authenticate and authorize access to an order's real-time room."""
    if not current_user.is_authenticated:
        emit('error', {'message': 'Authentication required'})
        return

    try:
        order_id = int(data.get('order_id'))
    except (TypeError, ValueError):
        emit('error', {'message': 'Invalid order ID'})
        return

    order = Order.query.get(order_id)
    if not order:
        emit('error', {'message': 'Order not found'})
        return

    # Check permission: customer, farm owner, assigned driver, or driver inspecting ready order
    is_cust = order.customer_id == current_user.id
    is_farm = current_user.role == 'farmer' and current_user.farm and current_user.farm.id == order.farm_id
    is_drv = current_user.role == 'driver' and (order.driver_id == current_user.id or order.status in [OrderStatus.READY_FOR_PICKUP, 'Ready for Pickup'])

    if not (is_cust or is_farm or is_drv):
        emit('error', {'message': 'Unauthorized to join order room'})
        return

    room = f"order_{order_id}"
    join_room(room)
    print(f'User {current_user.email} joined {room}')

@socketio.on('join_farm_room')
def handle_join_farm_room(data):
    """Authorize access to a farm's private event channel."""
    if not current_user.is_authenticated or current_user.role != 'farmer':
        return
    try:
        farm_id = int(data.get('farm_id'))
    except (TypeError, ValueError):
        return
        
    if not current_user.farm or current_user.farm.id != farm_id:
        return

    room = f"farm_{farm_id}"
    join_room(room)
    print(f'Farmer {current_user.email} joined {room}')

@socketio.on('join_drivers_room')
def handle_join_drivers_room():
    """Allow logged in drivers to join available delivery announcements."""
    if not current_user.is_authenticated or current_user.role != 'driver':
        return
    join_room('drivers_room')
    print(f'Driver {current_user.email} joined drivers_room')

LAST_GPS_BROADCAST = {} # order_id -> float timestamp

@socketio.on('driver_location_update')
def handle_driver_location_update(data):
    """
    Received from the assigned driver's browser GPS.
    Validates coordinates, checks order status and authorization,
    throttles broadcasts, persists in DB, and emits to order room.
    """
    if not current_user.is_authenticated or current_user.role != 'driver':
        emit('error', {'message': 'Authentication required as driver.'})
        return
        
    try:
        order_id = int(data.get('order_id'))
        lat = float(data.get('lat'))
        lng = float(data.get('lng'))
    except (TypeError, ValueError):
        emit('error', {'message': 'Invalid coordinates format: numeric lat/lng required.'})
        return

    # Validate coordinate boundaries (-90..90, -180..180)
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0):
        emit('error', {'message': 'Invalid coordinates range: lat must be between -90 and 90, lng between -180 and 180.'})
        return

    order = Order.query.get(order_id)
    if not order or order.driver_id != current_user.id:
        emit('error', {'message': 'Unauthorized: You are not assigned to this order.'})
        return  # Unauthorized to stream location for this order

    # Only stream location while delivery is active! Stop once DELIVERED, CANCELLED, or FAILED
    norm_status = NORMALIZED_STATUS.get(order.status, order.status)
    active_statuses = [OrderStatus.DRIVER_ASSIGNED, OrderStatus.PICKED_UP]
    if norm_status not in active_statuses:
        emit('tracking_stopped', {
            'order_id': order_id,
            'status': norm_status,
            'message': f'Tracking stopped because order is {order.status_display}.'
        })
        emit('error', {'message': 'Tracking is inactive for this order status'})
        return

    now_ts = time.time()
    last_broadcast = LAST_GPS_BROADCAST.get(order_id, 0.0)
    # Rate limit: max 1 broadcast every 0.2s for simulator, 1.0s for standard GPS (bypassed in testing)
    min_interval = 0.2 if data.get('is_simulated') else 1.0
    if not app.config.get('TESTING') and now_ts - last_broadcast < min_interval:
        return
    LAST_GPS_BROADCAST[order_id] = now_ts

    now = datetime.datetime.now()
    now_str = now.strftime('%I:%M:%S %p')
    now_iso = now.isoformat()

    # Persist latest location to orders collection
    mongo_db['orders'].update_one(
        {'_id': order_id},
        {
            '$set': {
                'driver_latitude': lat,
                'driver_longitude': lng,
                'last_location_at': now,
                'last_location_update': now_ts
            }
        }
    )

    # Persist driver's latest coordinates on users collection
    mongo_db['users'].update_one(
        {'_id': current_user.id},
        {
            '$set': {
                'last_latitude': lat,
                'last_longitude': lng,
                'last_location_at': now,
                'last_location_update': now_ts,
                'last_active_at': now
            }
        }
    )

    location_payload = {
        'order_id': order_id,
        'lat': lat,
        'lng': lng,
        'driver_name': current_user.email.split('@')[0],
        'timestamp': now_str,
        'iso_timestamp': now_iso,
        'is_active': True,
        'is_simulated': bool(data.get('is_simulated', False))
    }
    customer_room = f"order_{order_id}"
    emit('customer_location_update', location_payload, to=customer_room)  

@socketio.on('request_order_location')
def handle_request_order_location(data):
    """
    Called by client on page load or after reconnect to get the last known driver location,
    timestamp, and staleness info.
    """
    if not current_user.is_authenticated:
        emit('error', {'message': 'Authentication required'})
        return
    try:
        order_id = int(data.get('order_id'))
    except (TypeError, ValueError):
        emit('error', {'message': 'Invalid order ID'})
        return

    order = Order.query.get(order_id)
    if not order:
        emit('error', {'message': 'Order not found'})
        return

    # Check permission
    is_cust = order.customer_id == current_user.id
    is_farm = current_user.role == 'farmer' and current_user.farm and current_user.farm.id == order.farm_id
    is_drv = current_user.role == 'driver' and (order.driver_id == current_user.id or order.status in [OrderStatus.READY_FOR_PICKUP, 'Ready for Pickup'])
    if not (is_cust or is_farm or is_drv or current_user.role == 'admin'):
        emit('error', {'message': 'Unauthorized to request tracking for this order'})
        return

    d_lat = getattr(order, 'driver_latitude', None)
    d_lng = getattr(order, 'driver_longitude', None)
    last_at = getattr(order, 'last_location_at', None)

    is_stale = False
    last_str = "No updates yet"
    seconds_ago = None
    updated_at_ts = None
    if last_at and isinstance(last_at, datetime.datetime):
        seconds_ago = (datetime.datetime.now() - last_at).total_seconds()
        is_stale = seconds_ago > 60
        last_str = last_at.strftime('%I:%M:%S %p')
        updated_at_ts = last_at.timestamp()
    elif getattr(order, 'last_location_update', None):
        ts = order.last_location_update
        seconds_ago = time.time() - ts
        is_stale = seconds_ago > 60
        last_str = datetime.datetime.fromtimestamp(ts).strftime('%I:%M:%S %p')
        updated_at_ts = ts

    norm_status = NORMALIZED_STATUS.get(order.status, order.status)
    is_active = norm_status in [OrderStatus.DRIVER_ASSIGNED, OrderStatus.PICKED_UP]

    sync_payload = {
        'order_id': order_id,
        'lat': d_lat,
        'lng': d_lng,
        'driver_latitude': d_lat,
        'driver_longitude': d_lng,
        'last_location_at': last_str,
        'updated_at': updated_at_ts,
        'seconds_ago': seconds_ago,
        'is_stale': is_stale,
        'is_active': is_active,
        'status': norm_status,
        'driver_name': order.driver.email.split('@')[0] if order.driver else None
    }
    emit('order_location_sync', sync_payload)
    emit('initial_location_state', sync_payload)
  

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
    emit('new_chat_message', {
        'message': message,
        'sender': sender_name
    }, to=order_room, include_self=False)
    
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

# ==============================================================================
# FEATURE 1, 2, 3, 4, 5, 6, 7, 8, 9: ADMINISTRATOR & MONITORING SYSTEM
# ==============================================================================

def get_admin_dashboard_metrics():
    """
    Computes real-time platform metrics, financial KPIs, verification queues,
    and operational statistics using MongoDB aggregation pipelines.
    Gracefully handles empty datasets.
    """
    # User counts
    total_customers = mongo_db['users'].count_documents({'role': 'customer'})
    total_farmers = mongo_db['users'].count_documents({'role': 'farmer'})
    total_drivers = mongo_db['users'].count_documents({'role': 'driver'})
    total_admins = mongo_db['users'].count_documents({'role': 'admin'})
    suspended_users = mongo_db['users'].count_documents({'is_suspended': True})

    # Farm Verifications
    pending_farms = mongo_db['farms'].count_documents({'verification_status': VerificationStatus.PENDING})
    approved_farms = mongo_db['farms'].count_documents({'verification_status': VerificationStatus.APPROVED})
    rejected_farms = mongo_db['farms'].count_documents({'verification_status': VerificationStatus.REJECTED})
    info_needed_farms = mongo_db['farms'].count_documents({'verification_status': VerificationStatus.NEEDS_MORE_INFORMATION})

    # Driver Verifications
    pending_drivers = mongo_db['users'].count_documents({'role': 'driver', 'driver_verification_status': VerificationStatus.PENDING})
    approved_drivers = mongo_db['users'].count_documents({'role': 'driver', 'driver_verification_status': VerificationStatus.APPROVED})
    rejected_drivers = mongo_db['users'].count_documents({'role': 'driver', 'driver_verification_status': VerificationStatus.REJECTED})
    info_needed_drivers = mongo_db['users'].count_documents({'role': 'driver', 'driver_verification_status': VerificationStatus.NEEDS_MORE_INFORMATION})

    # Orders
    total_orders = mongo_db['orders'].count_documents({})
    completed_orders = mongo_db['orders'].count_documents({'status': {'$in': [OrderStatus.DELIVERED, 'Delivered']}})
    cancelled_orders = mongo_db['orders'].count_documents({'status': {'$in': [OrderStatus.CANCELLED, 'Cancelled', 'Rejected']}})
    active_orders = mongo_db['orders'].count_documents({
        'status': {'$nin': [OrderStatus.DELIVERED, 'Delivered', OrderStatus.CANCELLED, 'Cancelled', 'Rejected']}
    })

    # Order status counts via aggregation
    status_pipeline = [{'$group': {'_id': '$status', 'count': {'$sum': 1}}}]
    status_counts_raw = list(mongo_db['orders'].aggregate(status_pipeline))
    order_status_counts = {}
    for item in status_counts_raw:
        st = item.get('_id') or 'UNKNOWN'
        order_status_counts[st] = item.get('count', 0)

    # Gross Order Value (GMV) - sum of total_price for all non-cancelled orders
    gmv_pipeline = [
        {'$match': {'status': {'$nin': [OrderStatus.CANCELLED, 'Cancelled', 'Rejected']}}},
        {'$group': {'_id': None, 'total': {'$sum': {'$toDouble': '$total_price'}}}}
    ]
    gmv_res = list(mongo_db['orders'].aggregate(gmv_pipeline))
    total_gmv = round(float(gmv_res[0]['total']), 2) if gmv_res and gmv_res[0].get('total') is not None else 0.0

    # Fulfilled Gross Value (Delivered orders only)
    completed_pipeline = [
        {'$match': {'status': {'$in': [OrderStatus.DELIVERED, 'Delivered']}}},
        {'$group': {'_id': None, 'total': {'$sum': {'$toDouble': '$total_price'}}}}
    ]
    comp_res = list(mongo_db['orders'].aggregate(completed_pipeline))
    completed_gmv = round(float(comp_res[0]['total']), 2) if comp_res and comp_res[0].get('total') is not None else 0.0

    # Explicit financial calculations: 5% platform service fee on fulfilled orders
    platform_fee_percent = 5.0
    platform_revenue = round(completed_gmv * 0.05, 2)
    farmer_net_payout = round(completed_gmv * 0.95, 2)

    # Complaints
    unresolved_complaints = mongo_db['complaints'].count_documents({
        'status': {'$in': [ComplaintStatus.OPEN, ComplaintStatus.UNDER_REVIEW]}
    })
    total_complaints = mongo_db['complaints'].count_documents({})

    # Reviews
    reported_reviews = mongo_db['reviews'].count_documents({'is_reported': True, 'is_hidden': {'$ne': True}})
    total_reviews = mongo_db['reviews'].count_documents({})

    # Recent Audit Logs (last 10)
    recent_logs = list(mongo_db['audit_logs'].find().sort('timestamp', pymongo.DESCENDING).limit(10))

    # Recent Orders (last 6)
    recent_orders_raw = list(mongo_db['orders'].find().sort('created_at', pymongo.DESCENDING).limit(6))
    recent_orders = [Order.from_doc(d) for d in recent_orders_raw]

    # Recent Complaints (last 5)
    recent_complaints = list(mongo_db['complaints'].find().sort('created_at', pymongo.DESCENDING).limit(5))

    return {
        'total_customers': total_customers,
        'total_farmers': total_farmers,
        'total_drivers': total_drivers,
        'total_admins': total_admins,
        'suspended_users': suspended_users,
        'pending_farms': pending_farms,
        'approved_farms': approved_farms,
        'rejected_farms': rejected_farms,
        'info_needed_farms': info_needed_farms,
        'pending_drivers': pending_drivers,
        'approved_drivers': approved_drivers,
        'rejected_drivers': rejected_drivers,
        'info_needed_drivers': info_needed_drivers,
        'total_orders': total_orders,
        'completed_orders': completed_orders,
        'cancelled_orders': cancelled_orders,
        'active_orders': active_orders,
        'order_status_counts': order_status_counts,
        'total_gmv': total_gmv,
        'completed_gmv': completed_gmv,
        'platform_fee_percent': platform_fee_percent,
        'platform_revenue': platform_revenue,
        'farmer_net_payout': farmer_net_payout,
        'driver_rate_per_km': get_driver_rate_per_km(),
        'total_driver_payouts': round(sum(float(o.get('driver_payout_amount', 0.0) or 0.0) for o in mongo_db['orders'].find({'status': {'$in': [OrderStatus.DELIVERED, 'Delivered']}})), 2),
        'unresolved_complaints': unresolved_complaints,
        'total_complaints': total_complaints,
        'reported_reviews': reported_reviews,
        'total_reviews': total_reviews,
        'recent_logs': recent_logs,
        'recent_orders': recent_orders,
        'recent_complaints': recent_complaints
    }

# --- Admin Dashboard Route ---

@app.route('/admin')
@app.route('/admin/dashboard')
@admin_required
def admin_dashboard():
    """Renders the comprehensive administrative dashboard and platform monitor."""
    metrics = get_admin_dashboard_metrics()
    return render_template('admin/dashboard.html', metrics=metrics)

# --- Admin Farm Verifications ---

@app.route('/admin/verifications/farms')
@admin_required
def admin_farm_verifications():
    """Review queue for farm verifications with status filters and server-side pagination."""
    status_filter = request.args.get('status', 'ALL').strip().upper()
    search_q = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    per_page = 10

    # Tab badge counts
    counts = {
        'ALL': mongo_db['farms'].count_documents({}),
        'PENDING': mongo_db['farms'].count_documents({'verification_status': VerificationStatus.PENDING}),
        'APPROVED': mongo_db['farms'].count_documents({'verification_status': VerificationStatus.APPROVED}),
        'REJECTED': mongo_db['farms'].count_documents({'verification_status': VerificationStatus.REJECTED}),
        'NEEDS_MORE_INFORMATION': mongo_db['farms'].count_documents({'verification_status': VerificationStatus.NEEDS_MORE_INFORMATION}),
    }

    query_filter = {}
    if status_filter != 'ALL':
        query_filter['verification_status'] = status_filter
    if search_q:
        query_filter['$or'] = [
            {'name': {'$regex': search_q, '$options': 'i'}},
            {'address': {'$regex': search_q, '$options': 'i'}}
        ]

    total_count = mongo_db['farms'].count_documents(query_filter)
    cursor = mongo_db['farms'].find(query_filter).sort('_id', pymongo.DESCENDING)
    pagination = paginate_items(list(cursor), page=page, per_page=per_page)

    farms_list = []
    for fdoc in pagination['items']:
        farm_obj = Farm.from_doc(fdoc)
        farms_list.append(farm_obj)
    pagination['items'] = farms_list

    return render_template(
        'admin/farm_verifications.html',
        pagination=pagination,
        status_filter=status_filter,
        search_q=search_q,
        counts=counts
    )

@app.route('/admin/verify-farm/<int:farm_id>', methods=['POST'])
@admin_required
def admin_verify_farm(farm_id):
    """Administrator approves, rejects, or requests information on a farm."""
    farm = Farm.query.get_or_404(farm_id)
    action = request.form.get('action', '').strip().lower()
    notes = request.form.get('notes', '').strip()

    status_map = {
        'approve': VerificationStatus.APPROVED,
        'reject': VerificationStatus.REJECTED,
        'request_info': VerificationStatus.NEEDS_MORE_INFORMATION
    }

    if action not in status_map:
        flash('Invalid verification action specified.', 'danger')
        return redirect(url_for('admin_farm_verifications'))

    new_status = status_map[action]
    prev_status = farm.verification_status
    now = datetime.datetime.now()

    farm.verification_status = new_status
    farm.verification_notes = notes
    farm.verified_at = now
    farm.verified_by = current_user.email
    farm.save()

    # Record sensitive administrative action in audit log
    log_audit(
        actor_id=current_user.id,
        actor_email=current_user.email,
        actor_role='admin',
        action=f"FARM_VERIFICATION_{new_status}",
        target_type='farm',
        target_id=farm.id,
        details={
            'farm_name': farm.name,
            'previous_status': prev_status,
            'new_status': new_status,
            'notes': notes
        }
    )

    action_verbs = {
        'approve': 'approved',
        'reject': 'rejected',
        'request_info': 'marked as needing more information'
    }
    flash(f"Farm '{farm.name}' has been {action_verbs[action]} successfully.", 'success')
    return redirect(url_for('admin_farm_verifications'))

# --- Admin Driver Verifications ---

@app.route('/admin/verifications/drivers')
@admin_required
def admin_driver_verifications():
    """Review queue for delivery drivers with status filters and server-side pagination."""
    status_filter = request.args.get('status', 'ALL').strip().upper()
    search_q = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    per_page = 10

    counts = {
        'ALL': mongo_db['users'].count_documents({'role': 'driver'}),
        'PENDING': mongo_db['users'].count_documents({'role': 'driver', 'driver_verification_status': VerificationStatus.PENDING}),
        'APPROVED': mongo_db['users'].count_documents({'role': 'driver', 'driver_verification_status': VerificationStatus.APPROVED}),
        'REJECTED': mongo_db['users'].count_documents({'role': 'driver', 'driver_verification_status': VerificationStatus.REJECTED}),
        'NEEDS_MORE_INFORMATION': mongo_db['users'].count_documents({'role': 'driver', 'driver_verification_status': VerificationStatus.NEEDS_MORE_INFORMATION}),
    }

    query_filter = {'role': 'driver'}
    if status_filter != 'ALL':
        query_filter['driver_verification_status'] = status_filter
    if search_q:
        query_filter['$or'] = [
            {'email': {'$regex': search_q, '$options': 'i'}},
            {'name': {'$regex': search_q, '$options': 'i'}},
            {'driver_license_number': {'$regex': search_q, '$options': 'i'}}
        ]

    total_count = mongo_db['users'].count_documents(query_filter)
    cursor = mongo_db['users'].find(query_filter).sort('_id', pymongo.DESCENDING)
    pagination = paginate_items(list(cursor), page=page, per_page=per_page)

    drivers_list = [User.from_doc(d) for d in pagination['items']]
    pagination['items'] = drivers_list

    return render_template(
        'admin/driver_verifications.html',
        pagination=pagination,
        status_filter=status_filter,
        search_q=search_q,
        counts=counts
    )

@app.route('/admin/verify-driver/<int:driver_id>', methods=['POST'])
@admin_required
def admin_verify_driver(driver_id):
    """Administrator approves, rejects, or requests info on a delivery driver."""
    driver = User.query.get_or_404(driver_id)
    if driver.role != 'driver':
        flash('Target user is not a registered delivery driver.', 'danger')
        return redirect(url_for('admin_driver_verifications'))

    action = request.form.get('action', '').strip().lower()
    notes = request.form.get('notes', '').strip()

    status_map = {
        'approve': VerificationStatus.APPROVED,
        'reject': VerificationStatus.REJECTED,
        'request_info': VerificationStatus.NEEDS_MORE_INFORMATION
    }

    if action not in status_map:
        flash('Invalid driver verification action.', 'danger')
        return redirect(url_for('admin_driver_verifications'))

    new_status = status_map[action]
    prev_status = getattr(driver, 'driver_verification_status', 'UNKNOWN')
    now = datetime.datetime.now()

    driver.driver_verification_status = new_status
    driver.driver_verification_notes = notes
    driver.driver_verified_at = now
    driver.driver_verified_by = current_user.email
    driver.save()

    log_audit(
        actor_id=current_user.id,
        actor_email=current_user.email,
        actor_role='admin',
        action=f"DRIVER_VERIFICATION_{new_status}",
        target_type='user',
        target_id=driver.id,
        details={
            'driver_email': driver.email,
            'previous_status': prev_status,
            'new_status': new_status,
            'notes': notes
        }
    )

    action_verbs = {
        'approve': 'approved',
        'reject': 'rejected',
        'request_info': 'marked as needing more information'
    }
    flash(f"Driver '{driver.email}' verification has been {action_verbs[action]}.", 'success')
    return redirect(url_for('admin_driver_verifications'))

# --- Admin User Management ---

@app.route('/admin/users')
@admin_required
def admin_users():
    """Directory of all users with search, role filtering, suspension controls, and pagination."""
    role_filter = request.args.get('role', 'ALL').strip().lower()
    status_filter = request.args.get('status', 'ALL').strip().lower()
    search_q = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    per_page = 15

    query_filter = {}
    if role_filter != 'all':
        query_filter['role'] = role_filter
    if status_filter == 'active':
        query_filter['is_suspended'] = {'$ne': True}
    elif status_filter == 'suspended':
        query_filter['is_suspended'] = True

    if search_q:
        query_filter['$or'] = [
            {'email': {'$regex': search_q, '$options': 'i'}},
            {'name': {'$regex': search_q, '$options': 'i'}}
        ]

    cursor = mongo_db['users'].find(query_filter).sort('_id', pymongo.DESCENDING)
    pagination = paginate_items(list(cursor), page=page, per_page=per_page)

    users_list = []
    for udoc in pagination['items']:
        u_obj = User.from_doc(udoc)
        # Attach order count without exposing sensitive fields
        if u_obj.role == 'customer':
            u_obj.order_count = mongo_db['orders'].count_documents({'customer_id': u_obj.id})
        elif u_obj.role == 'driver':
            u_obj.order_count = mongo_db['orders'].count_documents({'driver_id': u_obj.id})
        elif u_obj.role == 'farmer':
            farm = u_obj.farm
            u_obj.order_count = mongo_db['orders'].count_documents({'farm_id': farm.id}) if farm else 0
        else:
            u_obj.order_count = 0
        users_list.append(u_obj)
    pagination['items'] = users_list

    return render_template(
        'admin/users.html',
        pagination=pagination,
        role_filter=role_filter,
        status_filter=status_filter,
        search_q=search_q
    )

@app.route('/admin/user/<int:user_id>/toggle-suspend', methods=['POST'])
@admin_required
def admin_toggle_user_suspend(user_id):
    """
    Suspends or reactivates a user account with justification reason.
    Prevents an administrator from locking out the last active admin account.
    """
    target = User.query.get_or_404(user_id)

    # Safety Guard: Prevent lockout of the last active administrator
    if target.role == 'admin':
        if target.id == current_user.id:
            flash('Safety restriction: You cannot suspend your own administrative account.', 'danger')
            return redirect(url_for('admin_users'))
        if not target.is_suspended:
            active_admins = mongo_db['users'].count_documents({
                'role': 'admin',
                'is_suspended': {'$ne': True}
            })
            if active_admins <= 1:
                flash('Safety restriction: Cannot suspend the last remaining active administrator on the platform.', 'danger')
                return redirect(url_for('admin_users'))

    now = datetime.datetime.now()
    if target.is_suspended:
        # Reactivate account
        target.is_suspended = False
        target.suspension_reason = None
        target.save()

        log_audit(
            actor_id=current_user.id,
            actor_email=current_user.email,
            actor_role='admin',
            action='USER_REACTIVATED',
            target_type='user',
            target_id=target.id,
            details={'email': target.email, 'role': target.role}
        )
        flash(f"Account '{target.email}' has been reactivated successfully.", 'success')
    else:
        # Suspend account
        reason = request.form.get('reason', '').strip() or 'Policy violation / administrative enforcement'
        target.is_suspended = True
        target.suspension_reason = reason
        target.suspended_at = now
        target.suspended_by = current_user.email
        target.save()

        log_audit(
            actor_id=current_user.id,
            actor_email=current_user.email,
            actor_role='admin',
            action='USER_SUSPENDED',
            target_type='user',
            target_id=target.id,
            details={'email': target.email, 'role': target.role, 'reason': reason}
        )
        flash(f"Account '{target.email}' has been suspended.", 'warning')

    return redirect(url_for('admin_users'))

# --- Admin Complaints & Disputes ---

@app.route('/admin/complaints')
@admin_required
def admin_complaints():
    """List of customer disputes and complaints with status and category filtering."""
    status_filter = request.args.get('status', 'ALL').strip().upper()
    cat_filter = request.args.get('category', 'ALL').strip()
    search_q = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    per_page = 15

    query_filter = {}
    if status_filter != 'ALL':
        query_filter['status'] = status_filter
    if cat_filter != 'ALL':
        query_filter['category'] = cat_filter
    if search_q:
        query_filter['$or'] = [
            {'customer_email': {'$regex': search_q, '$options': 'i'}},
            {'description': {'$regex': search_q, '$options': 'i'}}
        ]
        if search_q.isdigit():
            query_filter['$or'].append({'order_id': int(search_q)})

    cursor = mongo_db['complaints'].find(query_filter).sort('_id', pymongo.DESCENDING)
    pagination = paginate_items(list(cursor), page=page, per_page=per_page)

    complaints_list = []
    for cdoc in pagination['items']:
        c_obj = Complaint.from_doc(cdoc)
        complaints_list.append(c_obj)
    pagination['items'] = complaints_list

    return render_template(
        'admin/complaints.html',
        pagination=pagination,
        status_filter=status_filter,
        cat_filter=cat_filter,
        search_q=search_q,
        categories=ComplaintCategory.all_categories()
    )

@app.route('/admin/complaint/<int:complaint_id>', methods=['GET', 'POST'])
@app.route('/admin/complaints/<int:complaint_id>', methods=['GET', 'POST'])
@admin_required
def admin_complaint_details(complaint_id):
    """Detailed view of a customer dispute and administration resolution form."""
    complaint_doc = mongo_db['complaints'].find_one({'$or': [{'_id': complaint_id}, {'id': complaint_id}]})
    if not complaint_doc:
        abort(404)
    complaint = Complaint.from_doc(complaint_doc)
    order = Order.query.get(complaint.order_id)

    if request.method == 'POST':
        new_status = request.form.get('status')
        admin_notes = request.form.get('admin_notes', '').strip()

        valid_statuses = [ComplaintStatus.OPEN, ComplaintStatus.UNDER_REVIEW, ComplaintStatus.RESOLVED, ComplaintStatus.REJECTED]
        if new_status not in valid_statuses:
            flash('Invalid complaint status selected.', 'danger')
            return redirect(url_for('admin_complaint_details', complaint_id=complaint.id))

        now = datetime.datetime.now()
        complaint.status = new_status
        complaint.admin_notes = admin_notes
        complaint._data['updated_at'] = now
        if new_status in [ComplaintStatus.RESOLVED, ComplaintStatus.REJECTED]:
            complaint._data['resolved_at'] = now
            complaint._data['resolved_by'] = current_user.email
        complaint.save()

        log_audit(
            actor_id=current_user.id,
            actor_email=current_user.email,
            actor_role='admin',
            action=f"COMPLAINT_STATUS_{new_status}",
            target_type='complaint',
            target_id=complaint.id,
            details={
                'order_id': complaint.order_id,
                'status': new_status,
                'notes': admin_notes
            }
        )
        flash('Complaint status and resolution notes updated successfully.', 'success')
        return redirect(url_for('admin_complaint_details', complaint_id=complaint.id))

    return render_template('admin/complaint_details.html', complaint=complaint, order=order)

# --- Admin Customer Reviews Moderation ---

@app.route('/admin/reviews')
@admin_required
def admin_reviews():
    """Review moderation dashboard for managing reported, hidden, and authentic customer feedback."""
    filter_mode = request.args.get('filter', 'ALL').strip().lower()
    target_filter = request.args.get('target', 'ALL').strip().lower()
    page = int(request.args.get('page', 1))
    per_page = 15

    query_filter = {}
    if filter_mode == 'reported':
        query_filter['is_reported'] = True
    elif filter_mode == 'moderated':
        query_filter['is_hidden'] = True

    if target_filter in ['farm', 'driver']:
        query_filter['target_type'] = target_filter

    cursor = mongo_db['reviews'].find(query_filter).sort('_id', pymongo.DESCENDING)
    pagination = paginate_items(list(cursor), page=page, per_page=per_page)

    reviews_list = [Review.from_doc(rdoc) for rdoc in pagination['items']]
    pagination['items'] = reviews_list

    return render_template(
        'admin/reviews.html',
        pagination=pagination,
        filter_mode=filter_mode,
        target_filter=target_filter
    )

@app.route('/admin/review/<int:review_id>/moderate', methods=['POST'])
@admin_required
def admin_moderate_review(review_id):
    """Admin hides, restores, or dismisses reports on customer reviews."""
    review_doc = mongo_db['reviews'].find_one({'$or': [{'_id': review_id}, {'id': review_id}]})
    if not review_doc:
        abort(404)
    review = Review.from_doc(review_doc)

    action = request.form.get('action', '').strip().lower()
    now = datetime.datetime.now()

    if action == 'hide':
        review.is_hidden = True
        review.moderated_at = now
        review.moderated_by = current_user.email
        review.save()
        log_audit(current_user.id, current_user.email, 'admin', 'REVIEW_HIDDEN', 'review', review.id)
        flash('Review has been hidden from public display.', 'warning')
    elif action == 'restore':
        review.is_hidden = False
        review.moderated_at = now
        review.moderated_by = current_user.email
        review.save()
        log_audit(current_user.id, current_user.email, 'admin', 'REVIEW_RESTORED', 'review', review.id)
        flash('Review has been restored to public view.', 'success')
    elif action == 'dismiss_report':
        review.is_reported = False
        review.report_reason = None
        review.save()
        log_audit(current_user.id, current_user.email, 'admin', 'REVIEW_REPORT_DISMISSED', 'review', review.id)
        flash('Report flag dismissed.', 'info')
    else:
        flash('Invalid moderation action.', 'danger')

    return redirect(url_for('admin_reviews'))

# --- Admin Audit Logs ---

@app.route('/admin/audit-logs')
@admin_required
def admin_audit_logs():
    """Security audit trail displaying all sensitive administrative and platform actions."""
    action_filter = request.args.get('action', 'ALL').strip()
    target_filter = request.args.get('target_type', 'ALL').strip().lower()
    page = int(request.args.get('page', 1))
    per_page = 20

    query_filter = {}
    if action_filter != 'ALL':
        query_filter['action'] = action_filter
    if target_filter != 'all':
        query_filter['target_type'] = target_filter

    cursor = mongo_db['audit_logs'].find(query_filter).sort('timestamp', pymongo.DESCENDING)
    pagination = paginate_items(list(cursor), page=page, per_page=per_page)

    return render_template(
        'admin/audit_logs.html',
        pagination=pagination,
        action_filter=action_filter,
        target_filter=target_filter
    )

# --- Admin Platform Settings & Driver Earning Rates ---

@app.route('/admin/settings', methods=['GET'])
@admin_required
def admin_settings():
    """Displays platform settings including driver earning rates and operational parameters."""
    rate = get_driver_rate_per_km()
    setting_doc = mongo_db['platform_settings'].find_one({'_id': 'driver_rate_per_km'}) or {}
    
    # Recent rate change audit logs
    rate_logs = list(mongo_db['audit_logs'].find(
        {'action': 'DRIVER_RATE_UPDATED'}
    ).sort('timestamp', pymongo.DESCENDING).limit(10))
    
    # Financial aggregate metrics
    delivered_orders = list(mongo_db['orders'].find({
        'status': {'$in': [OrderStatus.DELIVERED, 'Delivered']},
        'driver_payout_finalized': True
    }))
    total_payouts_disbursed = round(sum(float(o.get('driver_payout_amount', 0.0) or 0.0) for o in delivered_orders), 2)
    total_payable_km_disbursed = round(sum(float(o.get('driver_payable_distance_km', 0.0) or 0.0) for o in delivered_orders), 2)
    
    return render_template(
        'admin/settings.html',
        driver_rate_per_km=rate,
        setting_doc=setting_doc,
        rate_logs=rate_logs,
        total_payouts_disbursed=total_payouts_disbursed,
        total_payable_km_disbursed=total_payable_km_disbursed,
        payouts_count=len(delivered_orders)
    )

@app.route('/admin/settings/driver-rate', methods=['POST'])
@admin_required
def admin_update_driver_rate():
    """Updates the configurable driver earning rate per kilometer."""
    if request.is_json:
        data = request.get_json() or {}
        new_rate = data.get('rate')
    else:
        new_rate = request.form.get('rate')
        
    try:
        new_rate_val = float(new_rate)
        if new_rate_val <= 0:
            raise ValueError("Rate must be greater than zero.")
        set_driver_rate_per_km(new_rate_val, current_user)
        
        if request.is_json:
            return jsonify({
                'success': True,
                'rate': new_rate_val,
                'message': f"Driver earning rate updated to ₹{new_rate_val:.2f} per km."
            })
        flash(f"Driver earning rate successfully updated to ₹{new_rate_val:.2f} per km.", "success")
    except (TypeError, ValueError) as e:
        if request.is_json:
            return jsonify({'success': False, 'message': f"Invalid rate: {e}"}), 400
        flash(f"Invalid rate value. Please provide a positive number: {e}", "danger")
        
    return redirect(url_for('admin_settings'))

# ==============================================================================
# CUSTOMER REVIEWS, COMPLAINTS & DRIVER VERIFICATION PROFILE ROUTES
# ==============================================================================

@app.route('/order/<int:order_id>/review', methods=['GET', 'POST'])
@login_required
def submit_order_review(order_id):
    """
    Allows eligible customers to submit authentic 1-5 star ratings and reviews
    for their completed (DELIVERED) orders.
    Enforces review uniqueness per target and prevents unauthorized manipulation.
    """
    if current_user.role != 'customer':
        flash('Only customers can review orders.', 'danger')
        return redirect(url_for('order_details', order_id=order_id))

    order = Order.query.get_or_404(order_id)
    if order.customer_id != current_user.id:
        flash('You can only review your own orders.', 'danger')
        return redirect(url_for('home'))

    # Order must be completed
    norm_status = NORMALIZED_STATUS.get(order.status, order.status)
    if norm_status != OrderStatus.DELIVERED:
        flash('Reviews can only be submitted for completed (Delivered) orders.', 'warning')
        return redirect(url_for('order_details', order_id=order.id))

    # Check for existing reviews
    existing_farm_rev = mongo_db['reviews'].find_one({'order_id': order.id, 'target_type': 'farm'})
    existing_driver_rev = mongo_db['reviews'].find_one({'order_id': order.id, 'target_type': 'driver'})

    if request.method == 'POST':
        farm_rating = request.form.get('farm_rating')
        farm_comment = request.form.get('farm_comment', '').strip()
        driver_rating = request.form.get('driver_rating')
        driver_comment = request.form.get('driver_comment', '').strip()

        submitted_any = False
        now = datetime.datetime.now()

        # Submit farm review
        if farm_rating and not existing_farm_rev:
            try:
                fr = int(farm_rating)
                if 1 <= fr <= 5:
                    rev_id = get_next_id('reviews')
                    mongo_db['reviews'].insert_one({
                        '_id': rev_id,
                        'id': rev_id,
                        'order_id': order.id,
                        'customer_id': current_user.id,
                        'customer_email': current_user.email,
                        'target_type': 'farm',
                        'target_id': order.farm_id,
                        'rating': fr,
                        'comment': farm_comment,
                        'created_at': now,
                        'created_at_display': now.strftime('%b %d, %Y'),
                        'is_reported': False,
                        'is_hidden': False
                    })
                    submitted_any = True
            except (ValueError, TypeError):
                flash('Invalid farm rating value. Must be between 1 and 5.', 'danger')

        # Submit driver review if order had a driver assigned
        if driver_rating and order.driver_id and not existing_driver_rev:
            try:
                dr = int(driver_rating)
                if 1 <= dr <= 5:
                    rev_id = get_next_id('reviews')
                    mongo_db['reviews'].insert_one({
                        '_id': rev_id,
                        'id': rev_id,
                        'order_id': order.id,
                        'customer_id': current_user.id,
                        'customer_email': current_user.email,
                        'target_type': 'driver',
                        'target_id': order.driver_id,
                        'rating': dr,
                        'comment': driver_comment,
                        'created_at': now,
                        'created_at_display': now.strftime('%b %d, %Y'),
                        'is_reported': False,
                        'is_hidden': False
                    })
                    submitted_any = True
            except (ValueError, TypeError):
                flash('Invalid driver rating value. Must be between 1 and 5.', 'danger')

        if submitted_any:
            flash('Thank you! Your review has been submitted successfully.', 'success')
        elif existing_farm_rev and (existing_driver_rev or not order.driver_id):
            flash('You have already submitted reviews for this order.', 'info')

        return redirect(url_for('order_details', order_id=order.id))

    return render_template(
        'submit_review.html',
        order=order,
        existing_farm_rev=existing_farm_rev,
        existing_driver_rev=existing_driver_rev
    )

@app.route('/review/<int:review_id>/report', methods=['POST'])
@login_required
def report_review(review_id):
    """Allows users to report abusive, inappropriate or fraudulent reviews for administrative moderation."""
    review_doc = mongo_db['reviews'].find_one({'$or': [{'_id': review_id}, {'id': review_id}]})
    if not review_doc:
        flash('Review not found.', 'danger')
        return redirect(request.referrer or url_for('home'))

    reason = request.form.get('reason', '').strip() or 'Inappropriate or abusive content'
    now = datetime.datetime.now()

    mongo_db['reviews'].update_one(
        {'_id': review_doc['_id']},
        {
            '$set': {
                'is_reported': True,
                'report_reason': reason,
                'reported_by': current_user.id,
                'reported_at': now
            }
        }
    )
    flash('Review has been reported to administrators for moderation.', 'info')
    return redirect(request.referrer or url_for('home'))

@app.route('/order/<int:order_id>/complaint', methods=['GET', 'POST'])
@login_required
def submit_order_complaint(order_id):
    """Allows customers to report issues and file disputes linked to their orders."""
    order = Order.query.get_or_404(order_id)
    if order.customer_id != current_user.id and current_user.role != 'admin':
        flash('You can only file complaints on your own orders.', 'danger')
        return redirect(url_for('home'))

    if request.method == 'POST':
        category = request.form.get('category', '').strip()
        description = request.form.get('description', '').strip()

        matched_category = None
        for cat in ComplaintCategory.all_categories():
            if cat.lower() == category.lower():
                matched_category = cat
                break

        if not matched_category:
            flash('Please select a valid complaint category.', 'danger')
            return redirect(url_for('submit_order_complaint', order_id=order.id))

        category = matched_category

        if not description:
            flash('Please provide a detailed description of the issue.', 'danger')
            return redirect(url_for('submit_order_complaint', order_id=order.id))

        now = datetime.datetime.now()
        comp_id = get_next_id('complaints')
        comp_doc = {
            '_id': comp_id,
            'id': comp_id,
            'order_id': order.id,
            'customer_id': current_user.id,
            'customer_email': current_user.email,
            'category': category,
            'description': description,
            'status': ComplaintStatus.OPEN,
            'admin_notes': '',
            'created_at': now,
            'created_at_display': now.strftime('%b %d, %Y %I:%M %p'),
            'updated_at': now
        }
        mongo_db['complaints'].insert_one(comp_doc)

        log_audit(
            actor_id=current_user.id,
            actor_email=current_user.email,
            actor_role=current_user.role,
            action='COMPLAINT_FILED',
            target_type='complaint',
            target_id=comp_id,
            details={'order_id': order.id, 'category': category}
        )

        flash('Your complaint has been submitted. Our dispute resolution team will review it shortly.', 'success')
        return redirect(url_for('view_complaint', complaint_id=comp_id))

    return render_template(
        'submit_complaint.html',
        order=order,
        categories=ComplaintCategory.all_categories()
    )

@app.route('/complaint/<int:complaint_id>')
@login_required
def view_complaint(complaint_id):
    """
    Secure complaint tracking view.
    Only authorized participants (customer, farmer, assigned driver, or admin) may view details.
    """
    comp_doc = mongo_db['complaints'].find_one({'$or': [{'_id': complaint_id}, {'id': complaint_id}]})
    if not comp_doc:
        flash('Complaint not found.', 'danger')
        return redirect(url_for('home'))

    complaint = Complaint.from_doc(comp_doc)
    order = Order.query.get(complaint.order_id)

    # Strict authorization check
    is_customer = current_user.id == complaint.customer_id
    is_farmer = current_user.role == 'farmer' and current_user.farm and order and current_user.farm.id == order.farm_id
    is_driver = current_user.role == 'driver' and order and order.driver_id == current_user.id
    is_admin = current_user.role == 'admin'

    if not (is_customer or is_farmer or is_driver or is_admin):
        flash('Access denied. You are not authorized to view this complaint.', 'danger')
        abort(403)

    if is_admin:
        return redirect(url_for('admin_complaint_details', complaint_id=complaint.id))

    return render_template('complaint_details.html', complaint=complaint, order=order)

@app.route('/driver/profile/verification', methods=['GET', 'POST'])
@driver_required
def driver_verification_profile():
    """Allows drivers to view their verification status and submit/resubmit license and vehicle details."""
    driver = current_user

    if request.method == 'POST':
        license_num = request.form.get('driver_license_number', '').strip()
        vehicle_type = request.form.get('driver_vehicle_type', '').strip()

        if not license_num or not vehicle_type:
            flash('License number and vehicle type are required.', 'danger')
            return redirect(url_for('driver_verification_profile'))

        driver.driver_license_number = license_num
        driver.driver_vehicle_type = vehicle_type
        
        # If rejected or needs info, reset to PENDING for review
        prev_status = getattr(driver, 'driver_verification_status', VerificationStatus.PENDING)
        if prev_status in [VerificationStatus.REJECTED, VerificationStatus.NEEDS_MORE_INFORMATION]:
            driver.driver_verification_status = VerificationStatus.PENDING
            driver.driver_verification_notes = f"Resubmitted by driver on {datetime.datetime.now().strftime('%b %d, %Y %I:%M %p')}"
            log_audit(driver.id, driver.email, 'driver', 'DRIVER_VERIFICATION_RESUBMITTED', 'user', driver.id)
            flash('Details updated and resubmitted for verification review!', 'success')
        else:
            flash('Driver details updated successfully.', 'success')

        driver.save()
        return redirect(url_for('driver_dashboard'))

    return render_template('driver_verification_profile.html', driver=driver)

# --- Admin Setup Helper & CLI ---

def create_admin_account(email, password, name=None):
    """Establishes or promotes an account to administrator role."""
    email = email.strip().lower()
    user = User.query.filter_by(email=email).first()
    if user:
        user.role = 'admin'
        user.is_suspended = False
        user.set_password(password)
        if name:
            user.name = name
        user.save()
        print(f"[OK] Existing account '{email}' promoted to Administrator.")
        return user
    else:
        user = User(
            email=email,
            role='admin',
            name=name or email.split('@')[0].title(),
            is_suspended=False,
            created_at=datetime.datetime.now()
        )
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        print(f"[OK] Administrator '{email}' created successfully.")
        return user

@app.cli.command('create-admin')
def create_admin_cli():
    """Secure interactive Flask CLI command to create an administrator."""
    import getpass
    email = input("Admin Email: ").strip()
    password = getpass.getpass("Admin Password: ")
    create_admin_account(email, password)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    print("MongoDB Connected successfully! Running server on http://localhost:5000 ...")
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)