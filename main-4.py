import asyncio
import hashlib
import logging
import os
import xml.etree.ElementTree as ET
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime
from email.utils import parsedate_to_datetime

import httpx
import socketio
from fastapi import FastAPI, HTTPException, Query, Request, Depends
from fastapi.middleware.cors import CORSMiddleware

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG (from environment variables)
# ─────────────────────────────────────────────
ALERT_POLL_INTERVAL = int(os.getenv("ALERT_POLL_INTERVAL", "60"))
MAX_ALERTS = int(os.getenv("MAX_ALERTS", "100"))
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*")
_origins_list = [o.strip() for o in ALLOWED_ORIGINS.split(",") if o.strip()]
_is_wildcard = _origins_list == ["*"]
if _is_wildcard:
    logger.warning(
        "ALLOWED_ORIGINS is '*' — disabling allow_credentials, since "
        "browsers reject wildcard origin + credentials combinations."
    )

# Gateway key — must match BACKEND_X_KEY in your master gateway
GATEWAY_KEY = os.getenv("GATEWAY_KEY")
if not GATEWAY_KEY:
    raise RuntimeError(
        "GATEWAY_KEY environment variable is not set. "
        "Refusing to start an unsecured backend."
    )

# ─────────────────────────────────────────────
# GATEWAY VERIFICATION
# Only your master gateway can call this backend
# ─────────────────────────────────────────────
async def verify_gateway(request: Request):
    """
    Verifies every request came from the Ennex master gateway.
    Rejects anything that doesn't carry the correct gateway key.
    """
    key = request.headers.get("X-Ennex-Backend-Key")
    if not key or key != GATEWAY_KEY:
        client_host = request.client.host if request.client else "unknown"
        logger.warning(f"Unauthorized access attempt from {client_host}")
        raise HTTPException(status_code=403, detail="Forbidden")

# ─────────────────────────────────────────────
# IN-MEMORY STORE
# ─────────────────────────────────────────────
active_alerts: list[dict] = []

# Bounded dedup tracking: deque enforces a max size (auto-evicts oldest),
# set gives O(1) membership checks. Keep them in sync.
PROCESSED_IDS_MAX = MAX_ALERTS * 10
processed_alert_ids: set[str] = set()
processed_alert_ids_order: deque = deque(maxlen=PROCESSED_IDS_MAX)


def _mark_processed(alert_id: str) -> None:
    """Add an alert id to the dedup set, evicting the oldest id once the
    bounded deque is full so this can never grow without limit."""
    if len(processed_alert_ids_order) == processed_alert_ids_order.maxlen:
        oldest = processed_alert_ids_order[0]  # about to be evicted by append
        processed_alert_ids.discard(oldest)
    processed_alert_ids_order.append(alert_id)
    processed_alert_ids.add(alert_id)


# ─────────────────────────────────────────────
# LIFESPAN
# ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Ennex weather backend starting...")
    task = asyncio.create_task(alert_monitor_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("🛑 Alert monitor stopped cleanly.")


# ─────────────────────────────────────────────
# APP INIT
# ─────────────────────────────────────────────
app = FastAPI(
    title="Ennex — Real-Time Global Disaster & Weather API",
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins_list,
    allow_credentials=not _is_wildcard,
    allow_methods=["*"],
    allow_headers=["*"],
)

sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
socket_app = socketio.ASGIApp(sio, other_asgi_app=app)


# ─────────────────────────────────────────────
# REST ENDPOINTS
# ─────────────────────────────────────────────

# Health stays PUBLIC — Render needs this to verify service is alive
@app.get("/health")
def health():
    return {
        "status": "ok",
        "alerts_cached": len(active_alerts),
        "timestamp": datetime.utcnow().isoformat(),
    }


# All other routes protected by gateway verification
@app.get("/api/weather")
async def get_weather(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
):
    """
    Live weather from Open-Meteo — completely free, no API key needed.
    Public endpoint: weather data isn't sensitive, so this is callable
    directly from the browser without the gateway key.
    """
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&current_weather=true"
        f"&hourly=relativehumidity_2m"
        f"&timezone=auto"
    )
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
    except httpx.HTTPError as e:
        logger.error(f"Open-Meteo error: {e}")
        raise HTTPException(status_code=502, detail="Weather service unavailable.")

    current = data.get("current_weather", {})
    hourly = data.get("hourly", {})

    return {
        "location": {"lat": lat, "lon": lon},
        "temperature": f"{current.get('temperature', 'N/A')}°C",
        "wind_speed": f"{current.get('windspeed', 'N/A')} km/h",
        "condition": _wmo_to_condition(current.get("weathercode", 0)),
        "humidity": f"{(hourly.get('relativehumidity_2m') or [None])[0]}%",
        "is_day": bool(current.get("is_day", 1)),
        "timezone": data.get("timezone", "UTC"),
        "timestamp": current.get("time", datetime.utcnow().isoformat()),
        "source": "Open-Meteo",
    }


@app.get("/api/alerts")
def get_alerts(_=Depends(verify_gateway)):  # ← gateway verification added
    return {
        "count": len(active_alerts),
        "alerts": active_alerts,
        "last_updated": datetime.utcnow().isoformat(),
    }


@app.get("/api/alerts/types")
def get_alert_types(_=Depends(verify_gateway)):  # ← gateway verification added
    types = list({a["type"] for a in active_alerts})
    return {"types": types}


# ─────────────────────────────────────────────
# BACKGROUND ALERT MONITOR
# ─────────────────────────────────────────────

async def alert_monitor_loop():
    while True:
        try:
            logger.info("🔄 Polling global disaster feeds...")
            new_alerts: list[dict] = []

            await asyncio.gather(
                _fetch_usgs(new_alerts),
                _fetch_gdacs(new_alerts),
                _fetch_nasa_eonet(new_alerts),
            )

            if new_alerts:
                for alert in new_alerts:
                    logger.info(f"🚨 [{alert['type']}] {alert['title']}")
                    await sio.emit("emergency_alert", alert)
                logger.info(f"✅ Broadcasted {len(new_alerts)} new alert(s).")

            if len(active_alerts) > MAX_ALERTS:
                active_alerts[:] = active_alerts[:MAX_ALERTS]

        except Exception as e:
            logger.error(f"Alert monitor error: {e}", exc_info=True)

        await asyncio.sleep(ALERT_POLL_INTERVAL)


# ─────────────────────────────────────────────
# FEED 1 — USGS (Earthquakes)
# ─────────────────────────────────────────────

async def _fetch_usgs(new_alerts: list):
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_hour.geojson"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url)
            r.raise_for_status()

        for eq in r.json().get("features", []):
            alert_id = eq["id"]
            if alert_id in processed_alert_ids:
                continue
            _mark_processed(alert_id)

            props = eq["properties"]
            mag = props.get("mag", 0)

            alert = {
                "id": alert_id,
                "type": "EARTHQUAKE",
                "title": props.get("title", "Earthquake Detected"),
                "severity": "CRITICAL" if mag >= 6.0 else "WARNING",
                "location": props.get("place", "Unknown"),
                "magnitude": mag,
                "time": datetime.fromtimestamp(props["time"] / 1000.0).isoformat(),
                "description": f"Magnitude {mag} earthquake near {props.get('place', 'unknown location')}.",
                "source": "USGS",
                "source_url": props.get("url", "https://earthquake.usgs.gov"),
            }
            new_alerts.append(alert)
            active_alerts.insert(0, alert)

    except Exception as e:
        logger.warning(f"USGS feed error: {e}")


# ─────────────────────────────────────────────
# FEED 2 — GDACS
# ─────────────────────────────────────────────

async def _fetch_gdacs(new_alerts: list):
    url = "https://www.gdacs.org/xml/rss.xml"
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            r = await client.get(url)
            r.raise_for_status()

        root = ET.fromstring(r.text)
        ns = {
            "gdacs": "http://www.gdacs.org",
            "geo": "http://www.w3.org/2003/01/geo/wgs84_pos#",
        }

        for item in root.findall(".//item"):
            title_el = item.find("title")
            link_el = item.find("link")
            pub_el = item.find("pubDate")
            desc_el = item.find("description")

            title = title_el.text if title_el is not None else "Unknown Event"
            link = link_el.text if link_el is not None else "https://gdacs.org"
            pub_date = pub_el.text if pub_el is not None else datetime.utcnow().isoformat()
            description = desc_el.text if desc_el is not None else ""

            alert_id = "gdacs_" + hashlib.md5(
                (title + str(pub_date)).encode("utf-8")
            ).hexdigest()
            if alert_id in processed_alert_ids:
                continue
            _mark_processed(alert_id)

            event_type = _gdacs_event_type(title)
            severity = _gdacs_severity(title)

            lat_el = item.find("geo:lat", ns)
            lon_el = item.find("geo:long", ns)
            location = "Global"
            if lat_el is not None and lon_el is not None:
                location = f"Lat {lat_el.text}, Lon {lon_el.text}"

            alert = {
                "id": alert_id,
                "type": event_type,
                "title": title.strip(),
                "severity": severity,
                "location": location,
                "time": _parse_rss_date(pub_date),
                "description": (description or title).strip(),
                "source": "GDACS",
                "source_url": link,
            }
            new_alerts.append(alert)
            active_alerts.insert(0, alert)

    except Exception as e:
        logger.warning(f"GDACS feed error: {e}")


def _gdacs_event_type(title: str) -> str:
    t = title.upper()
    if "CYCLONE" in t or "HURRICANE" in t or "TYPHOON" in t:
        return "CYCLONE"
    elif "FLOOD" in t:
        return "FLOOD"
    elif "TSUNAMI" in t:
        return "TSUNAMI"
    elif "VOLCANO" in t or "VOLCANIC" in t or "ERUPTION" in t:
        return "VOLCANO"
    elif "EARTHQUAKE" in t or "QUAKE" in t:
        return "EARTHQUAKE"
    elif "DROUGHT" in t:
        return "DROUGHT"
    elif "FIRE" in t or "WILDFIRE" in t:
        return "WILDFIRE"
    else:
        return "DISASTER"


def _gdacs_severity(title: str) -> str:
    t = title.upper()
    if any(w in t for w in ["RED", "SEVERE", "EXTREME", "MAJOR", "CATEGORY 4", "CATEGORY 5"]):
        return "CRITICAL"
    elif any(w in t for w in ["ORANGE", "MODERATE", "CATEGORY 2", "CATEGORY 3"]):
        return "WARNING"
    else:
        return "INFO"


def _parse_rss_date(value: str) -> str:
    """Convert an RSS pubDate (RFC 2822, e.g. 'Fri, 21 Jun 2026 10:00:00 GMT')
    into ISO 8601 so all alert sources share a consistent 'time' format."""
    try:
        return parsedate_to_datetime(value).isoformat()
    except (TypeError, ValueError):
        return datetime.utcnow().isoformat()


# ─────────────────────────────────────────────
# FEED 3 — NASA EONET
# ─────────────────────────────────────────────

async def _fetch_nasa_eonet(new_alerts: list):
    url = "https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=20"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url)
            r.raise_for_status()

        for event in r.json().get("events", []):
            alert_id = f"eonet_{event['id']}"
            if alert_id in processed_alert_ids:
                continue
            _mark_processed(alert_id)

            categories = [c["title"] for c in event.get("categories", [])]
            event_type = _eonet_event_type(categories)

            geometries = event.get("geometry", [])
            location = "Global"
            if geometries:
                coords = geometries[-1].get("coordinates", [])
                if coords:
                    if isinstance(coords[0], list):
                        location = f"Lat {coords[0][1]}, Lon {coords[0][0]}"
                    else:
                        location = f"Lat {coords[1]}, Lon {coords[0]}"

            sources = event.get("sources", [])
            source_url = sources[0].get("url", "https://eonet.gsfc.nasa.gov") if sources else "https://eonet.gsfc.nasa.gov"

            alert = {
                "id": alert_id,
                "type": event_type,
                "title": event.get("title", "Natural Event Detected"),
                "severity": "WARNING",
                "location": location,
                "time": (geometries[-1].get("date") if geometries else datetime.utcnow().isoformat()),
                "description": f"Active {event_type.lower()} event tracked by NASA EONET: {event.get('title', '')}.",
                "source": "NASA EONET",
                "source_url": source_url,
            }
            new_alerts.append(alert)
            active_alerts.insert(0, alert)

    except Exception as e:
        logger.warning(f"NASA EONET feed error: {e}")


def _eonet_event_type(categories: list[str]) -> str:
    joined = " ".join(categories).upper()
    if "WILDFIRE" in joined or "FIRE" in joined:
        return "WILDFIRE"
    elif "STORM" in joined or "CYCLONE" in joined or "HURRICANE" in joined:
        return "STORM"
    elif "VOLCANO" in joined:
        return "VOLCANO"
    elif "FLOOD" in joined:
        return "FLOOD"
    elif "DROUGHT" in joined:
        return "DROUGHT"
    elif "ICE" in joined or "SEA" in joined:
        return "SEA_ICE"
    else:
        return "NATURAL_EVENT"


# ─────────────────────────────────────────────
# WEBSOCKET HANDLERS
# ─────────────────────────────────────────────

@sio.on("connect")
async def on_connect(sid, environ):
    logger.info(f"🔌 Client connected: {sid}")
    if active_alerts:
        await sio.emit("active_alerts_snapshot", active_alerts, to=sid)


@sio.on("disconnect")
async def on_disconnect(sid):
    logger.info(f"❌ Client disconnected: {sid}")


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _wmo_to_condition(code: int) -> str:
    mapping = {
        0: "Clear Sky",
        1: "Mainly Clear", 2: "Partly Cloudy", 3: "Overcast",
        45: "Foggy", 48: "Icy Fog",
        51: "Light Drizzle", 53: "Moderate Drizzle", 55: "Dense Drizzle",
        61: "Light Rain", 63: "Moderate Rain", 65: "Heavy Rain",
        71: "Light Snow", 73: "Moderate Snow", 75: "Heavy Snow",
        80: "Rain Showers", 81: "Moderate Showers", 82: "Violent Showers",
        95: "Thunderstorm", 96: "Thunderstorm with Hail", 99: "Heavy Thunderstorm",
    }
    return mapping.get(code, "Unknown")
