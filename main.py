"""
Blood Connect Blood Management System — FastAPI Backend
Handles donor/hospital registration, blood requests, distance-based matching,
real-time WebSocket broadcasting, and SMS via Twilio.
"""

import math
import json
import os
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# ──────────────────────────────────────────────────────────────
# App Initialization
# ──────────────────────────────────────────────────────────────
app = FastAPI(title="Blood Connect Blood Management System", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────────────────────
# Twilio Setup (optional — set env vars to enable real SMS)
# If not configured, SMS calls return a simulated success so the
# UI works in development without crashing.
# ──────────────────────────────────────────────────────────────
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN  = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM_NUMBER = os.getenv("TWILIO_FROM_NUMBER", "")  # e.g. "+12015551234"

twilio_client = None
if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_FROM_NUMBER:
    try:
        from twilio.rest import Client as TwilioClient
        twilio_client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        print("Twilio client initialised — real SMS enabled")
    except ImportError:
        print("twilio package not installed. Run: pip install twilio")
else:
    print("Twilio env vars not set — SMS will run in simulation mode")


def _send_sms(to_number: str, body: str) -> str:
    """Send an SMS via Twilio if configured, otherwise simulate."""
    if twilio_client:
        msg = twilio_client.messages.create(
            body=body,
            from_=TWILIO_FROM_NUMBER,
            to=to_number,
        )
        return f"sent ({msg.sid})"
    # Simulation mode — no real SMS sent
    return "simulated (Twilio not configured)"


# ──────────────────────────────────────────────────────────────
# In-Memory Data Stores
# ──────────────────────────────────────────────────────────────
donors: List[dict] = []
hospitals: List[dict] = []
blood_requests: List[dict] = []
users: List[dict] = []

_donor_id_counter    = 0
_hospital_id_counter = 0
_request_id_counter  = 0


# ──────────────────────────────────────────────────────────────
# Persistence (donors, hospitals, requests all survive restarts —
# previously only donors were saved, which meant hospital_id /
# request_id references silently broke on every server restart)
# ──────────────────────────────────────────────────────────────
DONORS_FILE    = "donors.json"
HOSPITALS_FILE = "hospitals.json"
REQUESTS_FILE  = "requests.json"
connected_clients: List[WebSocket] = []

def _load(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            return default
    return default

def _save(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def load_donors():
    global donors, _donor_id_counter
    donors = _load(DONORS_FILE, [])
    _donor_id_counter = max((d["id"] for d in donors), default=0)

def save_donors():
    _save(DONORS_FILE, donors)

def load_hospitals():
    global hospitals, _hospital_id_counter
    hospitals = _load(HOSPITALS_FILE, [])
    _hospital_id_counter = max((h["id"] for h in hospitals), default=0)

def save_hospitals():
    _save(HOSPITALS_FILE, hospitals)

def load_requests():
    global blood_requests, _request_id_counter
    blood_requests = _load(REQUESTS_FILE, [])
    _request_id_counter = max((r["id"] for r in blood_requests), default=0)

def save_requests():
    _save(REQUESTS_FILE, blood_requests)

load_donors()
load_hospitals()
load_requests()

# ──────────────────────────────────────────────────────────────
# Pydantic Models
# ──────────────────────────────────────────────────────────────

class DonorModel(BaseModel):
    full_name: str
    blood_group: str
    phone_number: str
    address: str
    location: Optional[str] = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    is_available: bool = True
    password: Optional[str] = None

class HospitalModel(BaseModel):
    name: str
    location: str
    contact_number: str
    address: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    password: Optional[str] = None

class BloodRequestModel(BaseModel):
    hospital_id: int
    blood_group: str
    target_location: Optional[str] = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None

class UserRegister(BaseModel):
    username: str
    password: str
    role: Optional[str] = "user"

class UserLogin(BaseModel):
    username: str
    password: str

class DonorLogin(BaseModel):
    phone_number: str
    password: str

class HospitalLogin(BaseModel):
    contact_number: str
    password: str

class SmsSingleModel(BaseModel):
    donor_id: int
    hospital_id: int
    blood_group: str

class SmsAllModel(BaseModel):
    hospital_id: int
    blood_group: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    target_location: Optional[str] = ""

class RequestResponseModel(BaseModel):
    request_id: int
    donor_id: int
    action: str  # "accept" | "reject"

# ──────────────────────────────────────────────────────────────
# Utility Functions
# ──────────────────────────────────────────────────────────────

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi    = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


COMPATIBILITY: dict = {
    "A+":  ["A+", "A-", "O+", "O-"],
    "A-":  ["A-", "O-"],
    "B+":  ["B+", "B-", "O+", "O-"],
    "B-":  ["B-", "O-"],
    "AB+": ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"],
    "AB-": ["A-", "B-", "AB-", "O-"],
    "O+":  ["O+", "O-"],
    "O-":  ["O-"],
}


def find_donors_in_radius(
    blood_group: str,
    lat: Optional[float],
    lon: Optional[float],
    radius_km: float,
) -> List[dict]:
    compatible = COMPATIBILITY.get(blood_group.upper(), [blood_group.upper()])
    matched = []
    for d in donors:
        if not d.get("is_available", True):
            continue
        if d["blood_group"].upper() not in compatible:
            continue
        if lat is not None and lon is not None and d.get("latitude") and d.get("longitude"):
            dist = haversine_km(lat, lon, d["latitude"], d["longitude"])
            if dist <= radius_km:
                matched.append({**d, "distance_km": round(dist, 2)})
        else:
            matched.append({**d, "distance_km": None})
    matched.sort(key=lambda x: (x["distance_km"] is None, x["distance_km"] or 0))
    return matched


async def broadcast(message: dict) -> None:
    payload = json.dumps(message)
    dead = []
    for client in connected_clients:
        try:
            await client.send_text(payload)
        except Exception:
            dead.append(client)
    for c in dead:
        if c in connected_clients:
            connected_clients.remove(c)

# ──────────────────────────────────────────────────────────────
# WebSocket
# ──────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        await websocket.send_text(json.dumps({
            "type": "CONNECTED",
            "message": "Connected to Blood Connect real-time feed",
            "timestamp": datetime.now().isoformat(),
        }))
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            if msg.get("type") == "PING":
                await websocket.send_text(json.dumps({"type": "PONG"}))
    except WebSocketDisconnect:
        if websocket in connected_clients:
            connected_clients.remove(websocket)

# ──────────────────────────────────────────────────────────────
# REST Endpoints
# ──────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "service": "Blood Connect Blood Management System"}


@app.get("/api/stats")
def get_stats():
    fulfilled = sum(1 for r in blood_requests if r.get("status") == "fulfilled")
    return {
        "donors": len(donors),
        "hospitals": len(hospitals),
        "requests": len(blood_requests),
        "fulfilled": fulfilled,
        "live_connections": len(connected_clients),
    }


# ── Register Donor ─────────────────────────────────────────────
@app.post("/api/register-donor")
async def register_donor(data: DonorModel):
    global _donor_id_counter

    if any(d["phone_number"] == data.phone_number for d in donors):
        raise HTTPException(status_code=400, detail="A donor with this phone number is already registered")

    _donor_id_counter += 1
    record = {
        "id": _donor_id_counter,
        "full_name": data.full_name,
        "blood_group": data.blood_group.upper().strip(),
        "phone_number": data.phone_number,
        "address": data.address,
        "location": data.location,
        "latitude": data.latitude,
        "longitude": data.longitude,
        "is_available": data.is_available,
        "password": data.password,
        "registered_at": datetime.now().isoformat(),
    }
    donors.append(record)
    save_donors() 
    await broadcast({
        "type": "DONOR_REGISTERED",
        "donor_name": data.full_name,
        "blood_group": data.blood_group.upper(),
        "location": data.location,
        "timestamp": datetime.now().isoformat(),
    })
    return {
        "success": True,
        "donor_id": _donor_id_counter,
        "token": f"token_donor_{_donor_id_counter}_{int(datetime.now().timestamp())}",
        "message": "Donor registered successfully",
    }


# ── Register Hospital ──────────────────────────────────────────
@app.post("/api/register-hospital")
async def register_hospital(data: HospitalModel):
    global _hospital_id_counter

    if any(h["contact_number"] == data.contact_number for h in hospitals):
        raise HTTPException(status_code=400, detail="A hospital with this contact number is already registered")

    _hospital_id_counter += 1
    record = {
        "id": _hospital_id_counter,
        "name": data.name,
        "location": data.location,
        "contact_number": data.contact_number,
        "address": data.address,
        "latitude": data.latitude,
        "longitude": data.longitude,
        "password": data.password,
        "registered_at": datetime.now().isoformat(),
    }
    hospitals.append(record)
    save_hospitals()
    await broadcast({
        "type": "HOSPITAL_REGISTERED",
        "hospital_name": data.name,
        "location": data.location,
        "timestamp": datetime.now().isoformat(),
    })
    return {
        "success": True,
        "hospital_id": _hospital_id_counter,
        "token": f"token_hospital_{_hospital_id_counter}_{int(datetime.now().timestamp())}",
        "message": "Hospital registered successfully",
    }


# ── Donor Login ────────────────────────────────────────────────
@app.post("/api/login-donor")
def login_donor(data: DonorLogin):
    donor = next(
        (d for d in donors
         if d["phone_number"] == data.phone_number and d.get("password") == data.password),
        None,
    )
    if not donor:
        raise HTTPException(status_code=401, detail="Invalid phone number or password")
    return {
        "success": True,
        "token": f"token_donor_{donor['id']}_{int(datetime.now().timestamp())}",
        "donor_id": donor["id"],
        "full_name": donor["full_name"],
        "blood_group": donor["blood_group"],
        "phone_number": donor["phone_number"],
        "location": donor["location"],
        "address": donor["address"],
        "latitude": donor["latitude"],
        "longitude": donor["longitude"],
        "is_available": donor["is_available"],
    }


# ── Hospital Login ─────────────────────────────────────────────
@app.post("/api/login-hospital")
def login_hospital(data: HospitalLogin):
    hospital = next(
        (h for h in hospitals
         if h["contact_number"] == data.contact_number and h.get("password") == data.password),
        None,
    )
    if not hospital:
        raise HTTPException(status_code=401, detail="Invalid contact number or password")
    return {
        "success": True,
        "token": f"token_hospital_{hospital['id']}_{int(datetime.now().timestamp())}",
        "hospital_id": hospital["id"],
        "name": hospital["name"],
        "location": hospital["location"],
        "contact_number": hospital["contact_number"],
        "address": hospital["address"],
        "latitude": hospital["latitude"],
        "longitude": hospital["longitude"],
    }


# ── Blood Request ──────────────────────────────────────────────
@app.post("/api/request-blood")
async def request_blood(data: BloodRequestModel):
    global _request_id_counter

    hospital = next((h for h in hospitals if h["id"] == data.hospital_id), None)
    if not hospital:
        raise HTTPException(
            status_code=404,
            detail=f"Hospital ID {data.hospital_id} not found. Please register the hospital first."
        )

    _request_id_counter += 1
    bg = data.blood_group.upper().strip()

    matched = find_donors_in_radius(bg, data.latitude, data.longitude, radius_km=2.0)
    radius_expanded = False
    if not matched:
        matched = find_donors_in_radius(bg, data.latitude, data.longitude, radius_km=5.0)
        radius_expanded = True

    blood_requests.append({
        "id": _request_id_counter,
        "hospital_id": data.hospital_id,
        "hospital_name": hospital["name"],
        "blood_group": bg,
        "target_location": data.target_location,
        "latitude": data.latitude,
        "longitude": data.longitude,
        "matched_count": len(matched),
        "radius_expanded": radius_expanded,
        # A request only becomes "fulfilled" once a donor explicitly accepts
        # it via /api/respond-request — it is NOT fulfilled just because
        # donors were found nearby.
        "status": "pending",
        "accepted_donor_id": None,
        "accepted_donor_name": None,
        "accepted_donor_phone": None,
        "accepted_at": None,
        "created_at": datetime.now().isoformat(),
    })
    save_requests()

    await broadcast({
        "type": "BLOOD_REQUEST",
        "request_id": _request_id_counter,
        "hospital_id": data.hospital_id,
        "hospital_name": hospital["name"],
        "location": hospital["location"],
        "blood_group": bg,
        "target_location": data.target_location or hospital["location"],
        "contact_number": hospital["contact_number"],
        "matched_count": len(matched),
        "radius_expanded": radius_expanded,
        "timestamp": datetime.now().isoformat(),
    })

    return {
        "success": True,
        "request_id": _request_id_counter,
        "blood_group": bg,
        "matched_donors": matched[:20],
        "total_matched": len(matched),
        "radius_expanded": radius_expanded,
        "search_radius_km": 5.0 if radius_expanded else 2.0,
        "message": f"Found {len(matched)} compatible donor(s) within {'5' if radius_expanded else '2'} km",
    }


# ── Pending Requests Compatible With a Donor ─────────────────────
@app.get("/api/donor-requests/{donor_id}")
def get_donor_requests(donor_id: int):
    donor = next((d for d in donors if d["id"] == donor_id), None)
    if not donor:
        raise HTTPException(status_code=404, detail=f"Donor ID {donor_id} not found")

    dg = donor["blood_group"].upper()
    eligible = []
    for r in blood_requests:
        if r.get("status") != "pending":
            continue
        if not donor.get("is_available", True):
            continue
        need = r["blood_group"].upper()
        if dg not in COMPATIBILITY.get(need, [need]):
            continue
        hospital = next((h for h in hospitals if h["id"] == r["hospital_id"]), None)
        eligible.append({
            "request_id": r["id"],
            "hospital_id": r["hospital_id"],
            "hospital_name": r["hospital_name"],
            "location": hospital["location"] if hospital else "",
            "contact_number": hospital["contact_number"] if hospital else "",
            "target_location": r.get("target_location") or (hospital["location"] if hospital else ""),
            "blood_group": r["blood_group"],
            "timestamp": r["created_at"],
        })
    eligible.sort(key=lambda r: r["timestamp"], reverse=True)
    return {"requests": eligible, "total": len(eligible)}


# ── Donor Responds to a Request (Accept / Reject) ───────────────
@app.post("/api/respond-request")
async def respond_request(data: RequestResponseModel):
    req = next((r for r in blood_requests if r["id"] == data.request_id), None)
    if not req:
        raise HTTPException(status_code=404, detail=f"Request ID {data.request_id} not found")

    donor = next((d for d in donors if d["id"] == data.donor_id), None)
    if not donor:
        raise HTTPException(status_code=404, detail=f"Donor ID {data.donor_id} not found")

    action = data.action.lower().strip()
    if action not in ("accept", "reject"):
        raise HTTPException(status_code=400, detail="action must be 'accept' or 'reject'")

    if action == "reject":
        # No state change and no hospital notification for a reject —
        # only an accepted donor should ever reach the hospital.
        return {"success": True, "status": req["status"], "message": "Response recorded"}

    # ── accept ──
    if req["status"] == "fulfilled":
        if req.get("accepted_donor_id") == donor["id"]:
            return {"success": True, "status": "fulfilled", "message": "You already accepted this request"}
        raise HTTPException(
            status_code=409,
            detail="This request has already been fulfilled by another donor",
        )

    req["status"] = "fulfilled"
    req["accepted_donor_id"] = donor["id"]
    req["accepted_donor_name"] = donor["full_name"]
    req["accepted_donor_phone"] = donor["phone_number"]
    req["accepted_at"] = datetime.now().isoformat()
    save_requests()

    hospital = next((h for h in hospitals if h["id"] == req["hospital_id"]), None)

    # Only NOW — after the donor has explicitly accepted — does the
    # hospital get notified, and only now does the request count as fulfilled.
    await broadcast({
        "type": "REQUEST_ACCEPTED",
        "request_id": req["id"],
        "hospital_id": req["hospital_id"],
        "donor_id": donor["id"],
        "donor_name": donor["full_name"],
        "donor_phone": donor["phone_number"],
        "blood_group": req["blood_group"],
        "timestamp": datetime.now().isoformat(),
    })

    return {
        "success": True,
        "status": "fulfilled",
        "hospital_name": hospital["name"] if hospital else req.get("hospital_name"),
        "hospital_contact": hospital["contact_number"] if hospital else None,
        "message": "Thanks! The hospital has been notified.",
    }


# ── SMS — Single Donor ─────────────────────────────────────────
@app.post("/api/send-sms")
def send_sms_single(data: SmsSingleModel):
    donor = next((d for d in donors if d["id"] == data.donor_id), None)
    if not donor:
        raise HTTPException(status_code=404, detail=f"Donor ID {data.donor_id} not found")

    hospital = next((h for h in hospitals if h["id"] == data.hospital_id), None)
    if not hospital:
        raise HTTPException(status_code=404, detail=f"Hospital ID {data.hospital_id} not found")

    phone = donor.get("phone_number", "").strip()
    if not phone:
        raise HTTPException(status_code=400, detail="Donor has no phone number on record")

    body = (
        f"URGENT BLOOD REQUEST - Blood Connect\n"
        f"Hospital: {hospital['name']} ({hospital['location']})\n"
        f"Blood Group Needed: {data.blood_group}\n"
        f"Contact: {hospital['contact_number']}\n"
        f"Please respond urgently if you are available to donate."
    )

    try:
        status = _send_sms(phone, body)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SMS failed: {str(e)}")

    return {
        "success": True,
        "donor_name": donor["full_name"],
        "donor_phone": phone,
        "sms_status": status,
    }


# ── SMS — All Matched Donors (up to 10) ───────────────────────
@app.post("/api/send-sms-all")
def send_sms_all(data: SmsAllModel):
    hospital = next((h for h in hospitals if h["id"] == data.hospital_id), None)
    if not hospital:
        raise HTTPException(status_code=404, detail=f"Hospital ID {data.hospital_id} not found")

    bg = data.blood_group.upper().strip()
    matched = find_donors_in_radius(bg, data.latitude, data.longitude, radius_km=2.0)
    if not matched:
        matched = find_donors_in_radius(bg, data.latitude, data.longitude, radius_km=5.0)

    if not matched:
        raise HTTPException(status_code=404, detail="No compatible donors found to send SMS to")

    targets = matched[:10]

    body = (
        f"URGENT BLOOD REQUEST - Blood Connect\n"
        f"Hospital: {hospital['name']} ({hospital['location']})\n"
        f"Blood Group Needed: {bg}\n"
        f"Contact: {hospital['contact_number']}\n"
        f"Please respond urgently if you are available to donate."
    )

    results = []
    sms_sent = 0
    for d in targets:
        phone = (d.get("phone_number") or "").strip()
        if not phone:
            results.append({"donor": d["full_name"], "status": "skipped (no phone)"})
            continue
        try:
            status = _send_sms(phone, body)
            results.append({"donor": d["full_name"], "status": status})
            sms_sent += 1
        except Exception as e:
            results.append({"donor": d["full_name"], "status": f"failed: {str(e)}"})

    return {
        "success": True,
        "sms_sent": sms_sent,
        "total_targets": len(targets),
        "results": results,
    }


# ── List Endpoints ─────────────────────────────────────────────
@app.get("/api/donors")
def list_donors():
    safe = [{k: v for k, v in d.items() if k != "password"} for d in donors]
    return {"donors": safe, "total": len(safe)}

@app.get("/api/hospitals")
def list_hospitals():
    safe = [{k: v for k, v in h.items() if k != "password"} for h in hospitals]
    return {"hospitals": safe, "total": len(safe)}

@app.get("/api/requests")
def list_requests(hospital_id: Optional[int] = None):
    reqs = blood_requests
    if hospital_id is not None:
        reqs = [r for r in reqs if r["hospital_id"] == hospital_id]
    reqs = sorted(reqs, key=lambda r: r["created_at"], reverse=True)
    return {"requests": reqs, "total": len(reqs)}


# ── Generic User Auth (legacy) ─────────────────────────────────
@app.post("/api/register")
def register_user(data: UserRegister):
    if any(u["username"] == data.username for u in users):
        raise HTTPException(status_code=400, detail="Username already taken")
    users.append({"username": data.username, "password": data.password, "role": data.role})
    return {"success": True, "message": "User registered"}

@app.post("/api/login")
def login_user(data: UserLogin):
    user = next(
        (u for u in users if u["username"] == data.username and u["password"] == data.password),
        None,
    )
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    return {
        "success": True,
        "username": user["username"],
        "role": user["role"],
        "token": f"token_{user['username']}_{int(datetime.now().timestamp())}",
    }
    