import os
import time
import json
import threading
from typing import Dict, Any, Optional, Tuple
import math

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# =========================
# CONFIG
# =========================
BASE_AIRPORTS = ["LEVC", "LEBL", "LEMD"]
POLL_SECONDS = 20

VATSIM_URL = "https://data.vatsim.net/v3/vatsim-data.json"
CDM_URL_TEMPLATE = "https://cdm.vatsimspain.es/CDMViewer.php?airport={icao}"

# Airport coordinates for ARR distance (extend later if needed)
AIRPORT_COORDS = {
    "LEVC": (39.4893, -0.4816),
    "LEBL": (41.2974, 2.0833),
    "LEMD": (40.4722, -3.5608),
}

# "On ground" heuristic for WELCOME rule
GROUND_MAX_ALT_FT = 2500
GROUND_MAX_GS_KT = 60

# ARR PKG trigger distance
ARR_PKG_DISTANCE_NM = 100.0

# Stand pools (simple deterministic allocation)
STAND_POOLS = {
    "LEVC": ["2", "3", "4", "5", "34", "35", "41", "42", "43"],
    "LEBL": ["T1-201", "T1-202", "T1-203", "T1-204"],
    "LEMD": ["T4-351", "T4-352", "T4-353", "T4-354"],
}

STATE_FILE = "state.json"

# =========================
# ENV
# =========================
load_dotenv()
HOPPIE_LOGON = os.getenv("HOPPIE_LOGON")
GOC_STATION = os.getenv("GOC_STATION", "HPFGOC").strip()

if not HOPPIE_LOGON:
    raise RuntimeError("Missing HOPPIE_LOGON in .env")

# =========================
# STATE (persist across restarts)
# =========================
# Structure:
# {
#   "welcome_sent": { "HPF123": true, ... },
#   "arr_pkg_sent": { "HPF123|LEMD": true, ... },
#   "last_tsat": { "HPF123|LEVC": "1234", ... }
# }
_state = {
    "welcome_sent": {},
    "arr_pkg_sent": {},
    "last_tsat": {},
}
_state_lock = threading.Lock()


def load_state():
    global _state
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            with _state_lock:
                _state["welcome_sent"] = data.get("welcome_sent", {}) or {}
                _state["arr_pkg_sent"] = data.get("arr_pkg_sent", {}) or {}
                _state["last_tsat"] = data.get("last_tsat", {}) or {}
    except Exception as e:
        print(f"[STATE] load failed: {e}")


def save_state():
    with _state_lock:
        data = {
            "welcome_sent": _state["welcome_sent"],
            "arr_pkg_sent": _state["arr_pkg_sent"],
            "last_tsat": _state["last_tsat"],
        }
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[STATE] save failed: {e}")


# =========================
# UTIL
# =========================
def to_crlf(text: str) -> str:
    msg = (text or "").strip()
    msg = msg.replace("\r\n", "\n").replace("\r", "\n")
    msg = msg.replace("\n", "\r\n")
    return msg


def nm_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in nautical miles."""
    R_km = 6371.0
    rlat1 = math.radians(lat1)
    rlat2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    km = R_km * c
    return km / 1.852


def distance_to_airport_nm(apt: str, lat: Any, lon: Any) -> Optional[float]:
    if apt not in AIRPORT_COORDS:
        return None
    if lat is None or lon is None:
        return None
    try:
        alat, alon = AIRPORT_COORDS[apt]
        return nm_distance(float(lat), float(lon), alat, alon)
    except Exception:
        return None


def on_groundish(alt: Any, gs: Any) -> bool:
    try:
        a = float(alt)
        g = float(gs)
        return a <= GROUND_MAX_ALT_FT and g <= GROUND_MAX_GS_KT
    except Exception:
        return False


def choose_stand(apt: str, callsign: str) -> str:
    pool = STAND_POOLS.get(apt) or ["STAND"]
    idx = sum(ord(c) for c in callsign) % len(pool)
    return pool[idx]


# =========================
# HOPPIE TELEX (fixed packet)
# =========================
def hoppie_telex(to_callsign: str, message: str) -> str:
    payload = {
        "logon": HOPPIE_LOGON,
        "from": GOC_STATION,
        "to": to_callsign.upper().strip(),
        "type": "telex",
        "packet": to_crlf(message),  # ✅ key point: packet, CRLF
    }
    r = httpx.post(
        "https://www.hoppie.nl/acars/system/connect.html",
        data=payload,
        timeout=10,
        follow_redirects=True,
    )
    r.raise_for_status()
    return r.text.strip()


# =========================
# VATSIM
# =========================
def fetch_vatsim_hpf() -> Dict[str, Dict[str, Any]]:
    r = httpx.get(VATSIM_URL, timeout=20, follow_redirects=True)
    r.raise_for_status()
    data = r.json()

    flights: Dict[str, Dict[str, Any]] = {}
    for p in data.get("pilots", []):
        cs = (p.get("callsign") or "").upper().strip()
        if not cs.startswith("HPF"):
            continue

        fp = p.get("flight_plan") or {}
        dep = (fp.get("departure") or "").upper().strip()
        arr = (fp.get("arrival") or "").upper().strip()

        flights[cs] = {
            "callsign": cs,
            "dep": dep,
            "arr": arr,
            "lat": p.get("latitude"),
            "lon": p.get("longitude"),
            "alt": p.get("altitude"),
            "gs": p.get("groundspeed"),
        }

    return flights


# =========================
# CDM / TSAT
# =========================
def fetch_cdm_tsats(apt: str) -> Dict[str, str]:
    url = CDM_URL_TEMPLATE.format(icao=apt)
    r = httpx.get(url, timeout=20, follow_redirects=True)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # Pick the largest table if multiple
    tables = soup.find_all("table")
    if not tables:
        return {}

    table = max(tables, key=lambda t: len(t.find_all("tr")))
    rows = table.find_all("tr")
    if len(rows) < 2:
        return {}

    header_cells = rows[0].find_all(["th", "td"])
    headers = [c.get_text(strip=True).upper() for c in header_cells]

    callsign_idx = None
    tsat_idx = None
    for i, h in enumerate(headers):
        if h in ("CALLSIGN", "ACID", "CS", "CSIGN"):
            callsign_idx = i
        if "TSAT" in h:
            tsat_idx = i

    if callsign_idx is None:
        callsign_idx = 0
    if tsat_idx is None:
        # fallback (CDM layout varies)
        tsat_idx = 4

    tsats: Dict[str, str] = {}
    for row in rows[1:]:
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if not cols:
            continue
        if callsign_idx >= len(cols):
            continue

        cs = (cols[callsign_idx] or "").upper().strip()
        if not cs:
            continue

        tsat = (cols[tsat_idx] or "").strip() if tsat_idx < len(cols) else ""
        if tsat in ("", "-", "—", "N/A", "NA"):
            continue

        tsats[cs] = tsat

    return tsats


# =========================
# MESSAGE TEMPLATES
# =========================
def build_welcome(cs: str) -> str:
    return (
        "HPF GOC\r\n"
        "WELCOME / IZU\r\n"
        f"CALLSIGN {cs}\r\n"
        "GOC READY AND ONLINE\r\n"
        "SEND READY WHEN PUSH-READY\r\n"
        "CONTACT GOC ANYTIME VIA TELEX"
    )


def build_arr_pkg(cs: str, arr: str, dnm: float, stand: str) -> str:
    return (
        "HPF GOC\r\n"
        "ARR PKG INFO\r\n"
        f"CALLSIGN {cs}\r\n"
        f"ARR {arr}\r\n"
        f"DIST {dnm:.0f}NM\r\n"
        f"STAND {stand}\r\n"
        "AFTER LANDING: VACATE ASAP / FOLLOW ATC"
    )


def build_tsat(dep: str, tsat: str) -> str:
    return (
        "HPF GOC\r\n"
        "TSAT UPDATE\r\n"
        f"APT {dep}\r\n"
        f"TSAT {tsat}"
    )


# =========================
# ACTIONS
# =========================
def send_welcome_if_needed(f: Dict[str, Any]):
    cs = f["callsign"]
    if not on_groundish(f.get("alt"), f.get("gs")):
        return

    with _state_lock:
        already = bool(_state["welcome_sent"].get(cs))
        if already:
            return
        _state["welcome_sent"][cs] = True
    save_state()

    try:
        resp = hoppie_telex(cs, build_welcome(cs))
        print(f"[WELCOME] sent to {cs} (ok: {resp})")
    except Exception as e:
        print(f"[WELCOME] failed to {cs}: {e}")


def send_arr_pkg_if_needed(f: Dict[str, Any]):
    cs = f["callsign"]
    arr = (f.get("arr") or "").upper().strip()
    if arr not in BASE_AIRPORTS:
        return

    dnm = distance_to_airport_nm(arr, f.get("lat"), f.get("lon"))
    if dnm is None or dnm > ARR_PKG_DISTANCE_NM:
        return

    key = f"{cs}|{arr}"
    with _state_lock:
        already = bool(_state["arr_pkg_sent"].get(key))
        if already:
            return
        _state["arr_pkg_sent"][key] = True
    save_state()

    stand = choose_stand(arr, cs)
    try:
        resp = hoppie_telex(cs, build_arr_pkg(cs, arr, dnm, stand))
        print(f"[ARRPKG] sent to {cs} ARR {arr} ({dnm:.0f}NM) -> {stand} (ok: {resp})")
    except Exception as e:
        print(f"[ARRPKG] failed to {cs}: {e}")


def send_tsat_if_changed(f: Dict[str, Any], tsat_tables: Dict[str, Dict[str, str]]):
    cs = f["callsign"]
    dep = (f.get("dep") or "").upper().strip()
    if dep not in BASE_AIRPORTS:
        return

    tsat = tsat_tables.get(dep, {}).get(cs)
    if not tsat:
        return

    key = f"{cs}|{dep}"
    with _state_lock:
        last = _state["last_tsat"].get(key)
        if last == tsat:
            return
        _state["last_tsat"][key] = tsat
    save_state()

    try:
        resp = hoppie_telex(cs, build_tsat(dep, tsat))
        print(f"[TSAT] sent to {cs} @ {dep}: {tsat} (ok: {resp})")
    except Exception as e:
        print(f"[TSAT] failed to {cs}: {e}")


# =========================
# WATCHER
# =========================
def watcher_loop():
    print("> HPF GOC – AUTO watcher running\n")
    print("Commands:")
    print("  telex <CALLSIGN> <MESSAGE...>   send TELEX")
    print("  ping <CALLSIGN>                send test TELEX")
    print("  help                           show help\n")
    print(f"Airports (ARR/TSAT scope): {', '.join(BASE_AIRPORTS)} | Poll: {POLL_SECONDS}s")
    print("WELCOME (IZU): first time seen + on ground-ish -> send once\n")

    last_cdm_err: Dict[str, str] = {}

    while True:
        # VATSIM
        try:
            flights = fetch_vatsim_hpf()
        except Exception as e:
            print(f"[VATSIM] error: {e}")
            time.sleep(POLL_SECONDS)
            continue

        # CDM TSAT tables (BASE only)
        tsat_tables: Dict[str, Dict[str, str]] = {}
        for apt in BASE_AIRPORTS:
            try:
                tsat_tables[apt] = fetch_cdm_tsats(apt)
                if apt in last_cdm_err:
                    del last_cdm_err[apt]
            except Exception as e:
                msg = str(e)
                if last_cdm_err.get(apt) != msg:
                    print(f"[CDM] {apt} error: {msg}")
                    last_cdm_err[apt] = msg
                tsat_tables[apt] = {}

        # Apply rules per flight
        for cs, f in flights.items():
            # 1) WELCOME (IZU): first time seen + on ground-ish
            send_welcome_if_needed(f)

            # 2) ARR PKG: <= 100NM to ARR (BASE only)
            send_arr_pkg_if_needed(f)

            # 3) TSAT changes (DEP BASE only)
            send_tsat_if_changed(f, tsat_tables)

        time.sleep(POLL_SECONDS)


# =========================
# CLI
# =========================
def cli_loop():
    while True:
        try:
            cmd = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting...")
            return

        if not cmd:
            continue

        if cmd.lower() == "help":
            print("telex <CALLSIGN> <MESSAGE...>")
            print("ping <CALLSIGN>")
            continue

        if cmd.lower().startswith("ping "):
            cs = cmd.split(maxsplit=1)[1].strip().upper()
            try:
                resp = hoppie_telex(cs, "HPF GOC TEST MESSAGE")
                print(f"Ping sent to {cs} (ok: {resp})")
            except Exception as e:
                print(f"Ping failed to {cs}: {e}")
            continue

        if cmd.lower().startswith("telex "):
            parts = cmd.split(maxsplit=2)
            if len(parts) < 3:
                print("Usage: telex <CALLSIGN> <MESSAGE>")
                continue
            cs = parts[1].upper().strip()
            msg = parts[2]
            try:
                resp = hoppie_telex(cs, msg)
                print(f"TELEX sent to {cs} (ok: {resp})")
            except Exception as e:
                print(f"TELEX failed to {cs}: {e}")
            continue

        print("Unknown command. Type 'help'.")


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    load_state()
    t = threading.Thread(target=watcher_loop, daemon=True)
    t.start()
    cli_loop()
