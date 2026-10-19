# FarmFetch: Hyperlocal Farm-to-Table Platform

FarmFetch is a full-stack Python web application connecting Customers, Farmers, and Delivery Drivers on a single platform with real-time order tracking and chat.

---

## 🚀 Quick Start (One-Click)

The simplest way to run the project on Windows:

1. Double-click **`run.bat`** (or open command prompt and run `run.bat`).
2. The script will:
   - Check Python and MongoDB
   - Activate the virtual environment (.venv)
   - Launch the server at **`http://localhost:5000`**
   - Automatically open the browser for you!

---

## ⚙️ Manual Setup & Running

### 1. Prerequisites
- **Python 3.8+** (installed and added to PATH)
- **MongoDB** (running locally on `mongodb://localhost:27017/`)

### 2. Virtual Environment Setup
```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Environment Variables (.env)
The `.env` file contains your configuration:
```env
SECRET_KEY='806f4687241566d2a035411c1e0a9787e8e836cedda573d151a06b6bebe5e3fc'
MONGO_URI='mongodb://localhost:27017/'
MONGO_DB_NAME='farm2table'
```

### 4. (Optional) Populate Sample Demo Data
To immediately get test accounts with farm produce:
```bash
python seed_data.py
```
This sets up:
- **Farmer**: `farmer@farm.com` | `password123`
- **Customer**: `customer@farm.com` | `password123`
- **Driver**: `driver@farm.com` | `password123`

### 5. Run the Application
```bash
python app.py
```
Open **`http://localhost:5000`** in your browser.

---

## 🛠️ Technology Stack
- **Backend**: Python 3, Flask, Flask-SocketIO (WebSockets), Flask-Login, Flask-Bcrypt
- **Database**: MongoDB (via PyMongo)
- **Frontend**: HTML5, Bootstrap 5, CSS3, JavaScript (Fetch API, Geolocation, Socket.IO client, Leaflet.js maps)
- **Real-Time**: Socket.IO for live driver GPS tracking, status alerts, and order chat
