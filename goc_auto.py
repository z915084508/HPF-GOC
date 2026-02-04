import os
import time
import threading
import json
import math
from typing import Dict, Tuple, Optional, List, Any

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# =========================
# CONFIG
# =========================
AIRPORTS = ["LEVC", "LEBL", "LEMD"]
POLL_SECONDS = 20

VATSIM_URL = "https://data.vatsim.net/v3/vatsim-data.json"

# ✅ Correct VATSIM Spain CDM entry
CDM_URL_TEMPLATE = "https://cdm.vatsimspain.es/CDMViewer.php?airport={icao}"

# ---- STAND @100NM FEATURE ----
TRIGGER_NM = 100.0

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GATES_DIR = os.path.join(BASE_DIR, "gates")
STATE_DIR = os.path.join(BASE_DIR, "state")
SENT_FLAGS_PATH = os.path.join(STATE_DIR, "sent_flags.json")
LRU_STATE_PATH = os.path.join(STATE_DIR, "lru_state.json")

# 机场坐标（用于距离计算）
AIRPORT_COORDS = {
    "LEVC": (39.4893, -0.4816),
    "LEBL": (41.2971, 2.0785),
    "LEMD": (40.4719, -3.5626),
}

# =========================
# ENV
# =========================
load_dotenv()
HOPPIE_LOGON = os.getenv("HOPPIE_LOGON")
GOC_STATION = os.getenv("GOC_STATION", "HPFGOC")

if not HOPPIE_LOGON:
    raise RuntimeError("Missing HOPPIE_LOGON in .env")


# =========================
# FS HELPERS
# =========================
def ensure_dirs():
    os.makedirs(GATES_DIR, exist_ok=True)
    os.makedirs(STATE_DIR, exist_ok=True)

def load_json(path: str, default: Any):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, data: Any):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# =========================
# TEXT NORMALIZATION
# =========================
def to_crlf(text: str) -> str:
    """
    Normalize message:
    - trim
    - convert any newlines to CRLF (many ACARS clients require CRLF)
    """
    msg = (text or "").strip()
    msg = msg.replace("\r\n", "\n").replace("\r", "\n")
    msg = msg.replace("\n", "\r\n")
    return msg


# =========================
# HOPPIE TELEX (FIXED)
# =========================
def hoppie_telex(to_callsign: str, message: str) -> str:
    """
    Hoppie expects parameter name 'packet' (NOT 'message').
    Using wrong field can result in 'ok' but blank content on client.
    """
    msg = to_crlf(message)
    payload = {
        "logon": HOPPIE_LOGON,
        "from": GOC_STATION,
        "to": to_callsign.upper().strip(),
        "type": "telex",
        "packet": msg,  # ✅ critical fix
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
# GEO
# =========================
def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3440.065  # Earth radius in NM
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def normalize_callsign(cs: str) -> str:
    return (cs or "").strip().upper()

def callsign_prefix(cs: str) -> str:
    cs = normalize_callsign(cs)
    pref = ""
    for ch in cs:
        if ch.isalpha():
            pref += ch
        else:
            break
    return pref


# =========================
# GATE RULE ENGINE (JSON)
# =========================
def load_gate_rules(icao: str) -> Dict[str, Any]:
    path = os.path.join(GATES_DIR, f"{icao}.json")
    doc = load_json(path, default={})
    if not doc:
        raise FileNotFoundError(f"Missing gate rules: {path}")
    return doc

def match_rule(rule: Dict[str, Any], ctx: Dict[str, Any]) -> bool:
    m = rule.get("match", {}) or {}

    pref_list = m.get("callsign_prefix")
    if isinstance(pref_list, list) and pref_list:
        if ctx["prefix"] not in [p.upper() for p in pref_list]:
            return False

    ac_list = m.get("aircraft_icao")
    if isinstance(ac_list, list) and ac_list:
        if ctx["aircraft_icao"] not in [a.upper() for a in ac_list]:
            return False

    return True

def select_stand_from_pool(airport: str, candidates: List[str], lru_state: Dict[str, Any]) -> str:
    now = int(time.time())
    airport_map = lru_state.setdefault(airport, {})

    best = None
    best_ts = None
    for stand in candidates:
        s = stand.strip().upper()
        ts = int(airport_map.get(s, 0))
        if best is None or ts < best_ts:
            best = s
            best_ts = ts

    airport_map[best] = now
    return best

def predict_stand(airport: str, callsign: str, aircraft_icao: str, lru_state: Dict[str, Any]) -> Dict[str, str]:
    doc = load_gate_rules(airport)
    rules = doc.get("rules", []) or []
    rules_sorted = sorted(rules, key=lambda r: int(r.get("priority", 0)), reverse=True)

    ctx = {
        "callsign": normalize_callsign(callsign),
        "prefix": callsign_prefix(callsign),
        "aircraft_icao": (aircraft_icao or "").strip().upper(),
    }

    for rule in rules_sorted:
        if not match_rule(rule, ctx):
            continue

        stands = rule.get("stands", {}) or {}
        stype = (stands.get("type") or "pool").strip().lower()
        label = (rule.get("label") or rule.get("name") or "APRON").strip()

        if stype == "fixed":
            s = (stands.get("stand") or "").strip().upper()
            if s:
                return {"stand": s, "label": label}

        candidates = stands.get("candidates") or []
        candidates = [c.strip().upper() for c in candidates if str(c).strip()]
        if candidates:
            stand = select_stand_from_pool(airport, candidates, lru_state)
            return {"stand": stand, "label": label}

    return {"stand": "TBD", "label": "APRON"}

def build_stand_telex(arr: str, stand: str, label: str) -> str:
    return (
        f"HPF GOC ARR STAND\r\n"
        f"AIRPORT {arr}\r\n"
        f"EST STAND {stand} ({label})\r\n"
        f"SUBJ CHG"
    )


# =========================
# VATSIM
# =========================
def fetch_vatsim_hpf() -> Dict[str, Dict[str, Any]]:
    """
    Return:
    {
      CALLSIGN: {
        dep, arr,
        lat, lon,
        aircraft_icao
      }
    }
    for all online HPF* pilots.
    """
    r = httpx.get(VATSIM_URL, timeout=20, follow_redirects=True)
    r.raise_for_status()
    data = r.json()

    flights: Dict[str, Dict[str, Any]] = {}
    for p in data.get("pilots", []):
        cs = normalize_callsign(p.get("callsign"))
        if not cs.startswith("HPF"):
            continue

        fp = p.get("flight_plan") or {}
        dep = normalize_callsign(fp.get("departure"))
        arr = normalize_callsign(fp.get("arrival"))

        lat = p.get("latitude")
        lon = p.get("longitude")

        aircraft_raw = (fp.get("aircraft") or "").strip().upper()  # e.g. "A20N/M"
        aircraft_icao = aircraft_raw.split("/")[0].strip().upper() if aircraft_raw else ""

        flights[cs] = {
            "dep": dep,
            "arr": arr,
            "lat": float(lat) if lat is not None else None,
            "lon": float(lon) if lon is not None else None,
            "aircraft_icao": aircraft_icao,
        }

    return flights


# =========================
# CDM (CDMViewer.php)
# =========================
def fetch_cdm_airport(apt: str) -> Dict[str, str]:
    """
    Return {CALLSIGN: TSAT} for one airport from VATSIM Spain CDMViewer.php.
    We parse the first table and try to locate TSAT column robustly.
    """
    url = CDM_URL_TEMPLATE.format(icao=apt)

    r = httpx.get(url, timeout=20, follow_redirects=True)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    table = soup.find("table")
    if not table:
        return {}

    rows = table.find_all("tr")
    if not rows:
        return {}

    header_cells = rows[0].find_all(["th", "td"])
    headers = [c.get_text(strip=True).upper() for c in header_cells]
    tsat_idx = None
    callsign_idx = None

    for i, h in enumerate(headers):
        if h in ("CALLSIGN", "ACID", "CS", "CSIGN"):
            callsign_idx = i
        if "TSAT" in h:
            tsat_idx = i

    if callsign_idx is None:
        callsign_idx = 0
    if tsat_idx is None:
        tsat_idx = 4

    tsats: Dict[str, str] = {}

    for row in rows[1:]:
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if not cols:
            continue
        if callsign_idx >= len(cols):
            continue

        callsign = normalize_callsign(cols[callsign_idx])
        if not callsign:
            continue

        tsat = ""
        if tsat_idx < len(cols):
            tsat = (cols[tsat_idx] or "").strip()

        if tsat in ("", "-", "—", "N/A", "NA"):
            continue

        tsats[callsign] = tsat

    return tsats


def tsat_state_for(cs: str, apt: str, cdm_tables: Dict[str, Dict[str, str]]) -> Tuple[str, Optional[str]]:
    table = cdm_tables.get(apt, {})
    if cs not in table:
        return "IN_CDM_BUT_NO_TSAT", None
    return "TSAT_ASSIGNED", table[cs]


# =========================
# WATCHER (AUTO)
# =========================
def watcher_loop():
    ensure_dirs()

    sent_flags: Dict[str, Any] = load_json(SENT_FLAGS_PATH, default={})
    lru_state: Dict[str, Any] = load_json(LRU_STATE_PATH, default={})

    print("HPF GOC – AUTO watcher running\n")
    print("Commands:")
    print("  telex <CALLSIGN> <MESSAGE...>   send TELEX")
    print("  ping <CALLSIGN>                send test TELEX")
    print("  help                           show help\n")
    print(f"Airports: {', '.join(AIRPORTS)} | Poll: {POLL_SECONDS}s\n")
    print(f"Stand trigger: <= {TRIGGER_NM:.0f}NM to ARR for {', '.join(AIRPORTS)} (one-shot)\n")

    last: Dict[Tuple[str, str], Tuple[str, Optional[str]]] = {}  # (cs, apt) -> (state, tsat)
    welcomed = set()  # callsigns welcomed once per process
    last_cdm_error: Dict[str, str] = {}

    while True:
        # --- VATSIM ---
        try:
            online = fetch_vatsim_hpf()
        except Exception as e:
            print(f"[VATSIM] error: {e}")
            time.sleep(POLL_SECONDS)
            continue

        # --- CDM prefetch ---
        cdm_tables: Dict[str, Dict[str, str]] = {}
        for apt in AIRPORTS:
            try:
                cdm_tables[apt] = fetch_cdm_airport(apt)
                if apt in last_cdm_error:
                    del last_cdm_error[apt]
            except Exception as e:
                msg = str(e)
                if last_cdm_error.get(apt) != msg:
                    print(f"[CDM] {apt} error: {msg}")
                    last_cdm_error[apt] = msg
                cdm_tables[apt] = {}

        changed_state_files = False

        # --- FIRST ONLINE WELCOME ---
        for cs, info in online.items():
            dep = info.get("dep", "")
            if dep not in AIRPORTS:
                continue
            if cs in welcomed:
                continue

            welcomed.add(cs)

            msg = (
                "HPF GOC\r\n"
                "GOC READY AND ONLINE\r\n"
                f"BASE {dep}\r\n"
                "SEND READY WHEN PUSH-READY"
            )
            try:
                resp = hoppie_telex(cs, msg)
                print(f"[TELEX] welcome sent to {cs} ({resp})")
            except Exception as e:
                print(f"[TELEX] welcome failed to {cs}: {e}")

        # --- TSAT tracking ---
        for cs, info in online.items():
            dep = info.get("dep", "")
            if dep not in AIRPORTS:
                continue

            state, tsat = tsat_state_for(cs, dep, cdm_tables)
            key = (cs, dep)
            prev = last.get(key)
            curr = (state, tsat)

            if prev != curr:
                last[key] = curr

                if state == "TSAT_ASSIGNED":
                    print(f"[TSAT] {cs} @ {dep}: {tsat}")

                    msg = (
                        "HPF GOC TSAT UPDATE\r\n"
                        f"AIRPORT {dep}\r\n"
                        f"CALLSIGN {cs}\r\n"
                        f"TSAT {tsat}"
                    )
                    try:
                        resp = hoppie_telex(cs, msg)
                        print(f"[TELEX] TSAT sent to {cs} ({resp})")
                    except Exception as e:
                        print(f"[TELEX] TSAT failed to {cs}: {e}")
                else:
                    print(f"[TSAT] {cs} @ {dep}: {state}")

        # --- ARR STAND @100NM (ONE-SHOT) ---
        for cs, info in online.items():
            arr = info.get("arr", "")
            if arr not in AIRPORTS:
                continue

            lat = info.get("lat")
            lon = info.get("lon")
            if lat is None or lon is None:
                continue

            if arr not in AIRPORT_COORDS:
                continue

            apt_lat, apt_lon = AIRPORT_COORDS[arr]
            dist_nm = haversine_nm(lat, lon, apt_lat, apt_lon)

            sent_key = f"{cs}|{arr}|stand100nm_sent"
            if sent_flags.get(sent_key):
                continue

            if dist_nm <= TRIGGER_NM:
                try:
                    pred = predict_stand(arr, cs, info.get("aircraft_icao", ""), lru_state)
                    msg = build_stand_telex(arr, pred["stand"], pred["label"])
                    resp = hoppie_telex(cs, msg)
                    print(f"[STAND] sent to {cs} ARR {arr} ({dist_nm:.0f}NM) -> {pred['stand']} ({resp})")

                    sent_flags[sent_key] = True
                    changed_state_files = True
                except FileNotFoundError as e:
                    print(f"[STAND] missing gate file for {arr}: {e}")
                except Exception as e:
                    print(f"[STAND] failed to {cs} ARR {arr}: {e}")

        if changed_state_files:
            save_json(SENT_FLAGS_PATH, sent_flags)
            save_json(LRU_STATE_PATH, lru_state)

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

        if cmd == "help":
            print("telex <CALLSIGN> <MESSAGE...>")
            print("ping <CALLSIGN>")
            continue

        if cmd.lower().startswith("ping "):
            cs = cmd.split(maxsplit=1)[1].strip().upper()
            try:
                resp = hoppie_telex(cs, "HPF GOC TEST MESSAGE")
                print(f"Ping sent to {cs} ({resp})")
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
                print(f"TELEX sent to {cs} ({resp})")
            except Exception as e:
                print(f"TELEX failed to {cs}: {e}")
            continue

        print("Unknown command. Type 'help'.")


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    t = threading.Thread(target=watcher_loop, daemon=True)
    t.start()
    cli_loop()
