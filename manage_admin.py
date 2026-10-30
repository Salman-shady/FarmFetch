"""
Administrative management utility for FarmFetch platform.
Provides secure server-side administrator creation and management.
Usage:
    python manage_admin.py create --email admin@farm.com --password YourPassword
"""
import sys
import argparse
import datetime
from app import app, db, User, mongo_db, log_audit

def create_admin(email, password, name=None):
    with app.app_context():
        email = email.strip().lower()
        user = User.query.filter_by(email=email).first()
        if user:
            user.role = 'admin'
            user.is_suspended = False
            user.set_password(password)
            if name:
                user._data['name'] = name
            user.save()
            print(f"[OK] Existing user '{email}' was promoted to Administrator.")
            log_audit(user.id, email, 'admin', 'USER_PROMOTED_ADMIN', 'user', user.id, {'reason': 'CLI promotion'})
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
            log_audit(user.id, email, 'admin', 'ADMIN_ACCOUNT_CREATED', 'user', user.id, {'reason': 'CLI creation'})
            return user

def list_admins():
    with app.app_context():
        admins = mongo_db['users'].find({'role': 'admin'})
        print("\n--- System Administrators ---")
        count = 0
        for adm in admins:
            count += 1
            status = "SUSPENDED" if adm.get('is_suspended') else "ACTIVE"
            print(f"ID: {adm.get('_id')} | Email: {adm.get('email')} | Status: {status} | Created: {adm.get('created_at')}")
        if count == 0:
            print("No administrators found in database.")
        print("-----------------------------\n")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="FarmFetch Admin Management Tool")
    subparsers = parser.add_subparsers(dest='command', help='Sub-commands')

    create_parser = subparsers.add_parser('create', help='Create or promote an administrator')
    create_parser.add_argument('--email', required=True, help='Administrator email address')
    create_parser.add_argument('--password', required=True, help='Administrator password')
    create_parser.add_argument('--name', help='Administrator full name (optional)')

    list_parser = subparsers.add_parser('list', help='List all administrators')

    args = parser.parse_args()

    if args.command == 'create':
        create_admin(args.email, args.password, args.name)
    elif args.command == 'list':
        list_admins()
    else:
        parser.print_help()
