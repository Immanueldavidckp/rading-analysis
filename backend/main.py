import os
import pyotp
import asyncio
import requests
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from NorenRestApiPy.NorenApi import NorenApi
from datetime import datetime

load_dotenv()

app = FastAPI()

# Allow frontend to communicate with FastAPI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Manage active WebSocket connections
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()
loop = asyncio.get_event_loop()

# Initialize Shoonya API
class ShoonyaApiPy(NorenApi):
    def __init__(self):
        # Reverting to the most stable trading endpoint
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWSTP/')

api = ShoonyaApiPy()

# --- State Tracker ---
class SessionTracker:
    def __init__(self):
        self.data = {} # {token: {high, high_time, low, low_time}}

    def update(self, token, price):
        now = datetime.now().strftime("%H:%M:%S")
        if token not in self.data:
            self.data[token] = {'h': price, 'ht': now, 'l': price, 'lt': now}
        else:
            if price > self.data[token]['h']:
                self.data[token]['h'] = price
                self.data[token]['ht'] = now
            if price < self.data[token]['l']:
                self.data[token]['l'] = price
                self.data[token]['lt'] = now
        return self.data[token]

tracker = SessionTracker()

# --- Shoonya Callbacks ---
def on_feed(data):
    if manager.active_connections:
        # Extract last price and token
        token = data.get('tk')
        lp = data.get('lp')
        if token and lp:
            stats = tracker.update(token, float(lp))
            data.update(stats) # Add high/low stats to the broadcast
        asyncio.run_coroutine_threadsafe(manager.broadcast(data), loop)

def on_open():
    print("✅ Shoonya Stream Connected.")
    api.subscribe('NSE', '26000') # Nifty 50
    api.subscribe('NSE', '2885')  # RELIANCE


# --- Server Startup ---
@app.on_event("startup")
async def startup_event():
    user_id = os.getenv('USER_ID')
    totp_secret = os.getenv('TOTP_SECRET')
    
    if not user_id or not totp_secret:
        print("❌ Login Failed: USER_ID and TOTP_SECRET must be set in .env")
        api.is_connected = False
        return

    try:
        totp = pyotp.TOTP(totp_secret).now()
        print(f"--- Debug: Attempting Login ---")
        print(f"User ID: {user_id}")
        print(f"TOTP Generated: {totp}")

        # --- RAW TEST (Back to TP) ---
        print("Running Raw Connection Test (TP)...")
        raw_url = "https://api.shoonya.com/NorenWClientTP/QuickLogon"
        payload = {
            "apkversion": "1.0.0",
            "uid": user_id,
            "pwd": os.getenv('PASSWORD'),
            "factor2": totp,
            "vc": os.getenv('VENDOR_CODE'),
            "appkey": os.getenv('API_SECRET'),
            "imei": os.getenv('IMEI'),
            "source": "API"
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        try:
            raw_res = requests.post(raw_url, data={'jData': json.dumps(payload)}, headers=headers, timeout=15)
            print(f"Raw HTTP Status: {raw_res.status_code}")
            print(f"Raw Response Text: {raw_res.text[:200]}") # Show first 200 chars
        except Exception as re:
            print(f"Raw Test Failed: {re}")
        # ----------------

        ret = api.login(
            userid=user_id,
            password=os.getenv('PASSWORD'),
            twoFA=totp,
            vendor_code=os.getenv('VENDOR_CODE'),
            api_secret=os.getenv('API_SECRET'),
            imei=os.getenv('IMEI')
        )
        
        print(f"Library Response: {ret}")
        
        if ret and isinstance(ret, dict) and ret.get('stat') == 'Ok':
            print("✅ Shoonya Login Successful!")
            api.is_connected = True
            api.start_websocket(order_update_callback=None, subscribe_callback=on_feed, socket_open_callback=on_open)
        else:
            status = ret.get('stat') if isinstance(ret, dict) else 'Non-JSON'
            print(f"❌ Library Login Failed. Status: {status}")
            api.is_connected = False
    except Exception as e:
        print(f"❌ Critical Login Error: {e}")
        api.is_connected = False

@app.get("/api/health")
def health_check():
    if getattr(api, 'is_connected', False):
        return {"status": "Live Market Connected"}
    return {"status": "Waiting for Login", "error": "Check server logs for details"}

# --- WebSocket Route ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)