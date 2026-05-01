import os
import pyotp
import asyncio
import random
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
        # Using the standard URL instead of the TP (Third Party) one
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClient/', websocket='wss://api.shoonya.com/NorenWSTP/')

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
        print(f"Vendor Code: {os.getenv('VENDOR_CODE')}")
        print(f"TOTP: {totp}")
        
        ret = api.login(
            userid=user_id,
            password=os.getenv('PASSWORD'),
            twoFA=totp,
            vendor_code=os.getenv('VENDOR_CODE'),
            api_secret=os.getenv('API_SECRET'),
            imei=os.getenv('IMEI')
        )
        
        print(f"Shoonya Response: {ret}")
        
        if ret and isinstance(ret, dict) and ret.get('stat') == 'Ok':
            print("✅ Shoonya Login Successful!")
            api.is_connected = True
            api.start_websocket(order_update_callback=None, subscribe_callback=on_feed, socket_open_callback=on_open)
        else:
            status = ret.get('stat') if isinstance(ret, dict) else 'Non-JSON Response'
            print(f"❌ Login Failed. Status: {status}")
            api.is_connected = False
    except Exception as e:
        print(f"❌ Login Error Exception: {e}")
        api.is_connected = False

@app.get("/api/health")
def health_check():
    if getattr(api, 'is_connected', False):
        return {"status": "Live Market Connected"}
    return {"status": "Waiting for Login", "error": "Ensure credentials are correct in .env"}

# --- WebSocket Route ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)