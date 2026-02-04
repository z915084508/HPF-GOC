import json
import math
import os
import time
from typing import Dict, Any, Optional, List

import requests

# =========================
# CONFIG
# =========================
VATSIM_DATA_URL = "https://data.vatsim.net/v3/vatsim-data.json"
POLL_SECONDS = 20

AIRPORTS = {"LEVC", "LEBL", "LEMD"}
TRIGGER_NM = 100.0

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GATES_DIR = os.path.join(BASE_DIR, "gates")
STATE_DIR = os.path.join(BASE_DIR, "state")
SENT_FLAGS_PATH = os.path.join(STATE_DIR, "sent_flags.json")
LRU_STATE_PATH = os.path.join(STATE_DIR, "lru_state.json")

# 机场坐标（够用：用于距离计算）
AIRPORT_COORDS = {
    "LEVC": (39.4893, -0.4816),   # Valencia
    "LEBL": (41.2971, 2.0785),    # Barcelona
    "LEMD": (40.4719, -3.5626),   # Madrid Barajas
}

# =========================
# HELPERS
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

def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # Earth radius in NM
    R = 3440.065
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def normalize_callsign(cs: str) -> str:
    return (cs or "").strip().upper()

def callsign_prefix(cs: str) -> str:
    # 取字母段做 prefix，例如 VLG123 -> VLG
    cs = normalize_callsign(cs)
    prefix = "HPF"
    for ch in cs:
        if ch.isalpha():
            prefix += ch
        else:
            break
    return prefix

def safe_get(d: Dict[str, Any], path: List[str], default=None):
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

# =========================
# GATE RULE ENGINE (JSON)
# =========================
def load_gate_rules(icao: str) -> Dict[str, Any]:
    path = os.path.join(GATES_DIR, f"{icao}.json")
    rules = load_json(path, default={})
    if not rules:
        raise FileNotFoundError(f"Missing gate rules: {path}")
    return rules

def match_rule(rule: Dict[str, Any], ctx: Dict[str, Any]) -> bool:
    """
    ctx keys:
      - callsign
      - prefix
      - aircraft_icao
    """
    m = rule.get("match", {}) or {}

    # callsign_prefix
    pref_list = m.get("callsign_prefix")
    if isinstance(pref_list, list) and pref_list:
        if ctx["prefix"] not in [p.upper() for p in pref_list]:
            return False

    # aircraft_icao
    ac_list = m.get("aircraft_icao")
    if isinstance(ac_list, list) and ac_list:
        if ctx["aircraft_icao"] not in [a.upper() for a in ac_list]:
            return False

    return True

def select_stand_from_pool(
    airport: str,
    candidates: List[str],
    lru_state: Dict[str, Any]
) -> str:
    """
    简单 LRU：优先选“最久没用过”的 stand
    lru_state structure:
      { "LEBL": { "M30": 1700000000, "M31": ... } }
    """
    now = int(time.time())
    airport_map = lru_state.setdefault(airport, {})

    # 取 timestamp 最小的（或不存在则当作 0）
    best = None
    best_ts = None
    for stand in candidates:
        s = stand.strip().upper()
        ts = int(airport_map.get(s, 0))
        if best is None or ts < best_ts:
            best = s
            best_ts = ts

    # 更新使用时间
    airport_map[best] = now
    return best

def predict_stand(
    airport: str,
    callsign: str,
    aircraft_icao: str,
    lru_state: Dict[str, Any]
) -> Dict[str, str]:
    """
    returns dict:
      { "stand": "M32", "label": "T1 CONTACT" }
    """
    rules_doc = load_gate_rules(airport)
    rules = rules_doc.get("rules", []) or []
    rules_sorted = sorted(rules, key=lambda r: int(r.get("priority", 0)), reverse=True)

    ctx = {
        "callsign": normalize_callsign(callsign),
        "prefix": callsign_prefix(callsign),
        "aircraft_icao": (aircraft_icao or "").strip().upper(),
    }

    for rule in rules_sorted:
        if match_rule(rule, ctx):
            stands = rule.get("stands", {}) or {}
            stype = stands.get("type", "pool")
            label = rule.get("label", rule.get("name", "EST STAND")).strip()

            if stype == "fixed":
                stand = (stands.get("stand") or "").strip().upper()
                if stand:
                    return {"stand": stand, "label": label}

            # default: pool
            candidates = stands.get("candidates") or []
            candidates = [c.strip().upper() for c in candidates if str(c).strip()]
            if not candidates:
                continue
            stand = select_stand_from_pool(airport, candidates, lru_state)
            return {"stand": stand, "label": label}

    # Fallback（如果 JSON 没写 default rule）
    return {"stand": "TBD", "label": "EST STAND"}

# =========================
# VATSIM FETCH
# =========================
def fetch_vatsim_data() -> Dict[str, Any]:
    r = requests.get(VATSIM_DATA_URL, timeout=15)
    r.raise_for_status()
    return r.json()

def iter_relevant_flights(data: Dict[str, Any]):
    pilots = data.get("pilots", []) or []
    for p in pilots:
        cs = normalize_callsign(p.get("callsign", ""))
        if not cs:
            continue
        fp = p.get("flight_plan") or {}
        arr = (fp.get("arrival") or "").strip().upper()
        if arr not in AIRPORTS:
            continue

        lat = p.get("latitude")
        lon = p.get("longitude")
        if lat is None or lon is None:
            continue

        ac = (fp.get("aircraft") or "").strip().upper()  # e.g. "A20N/M"
        # 取 "/" 前面当 ICAO 机型（你也可以按你习惯改）
        aircraft_icao = ac.split("/")[0].strip().upper() if ac else ""

        yield {
            "callsign": cs,
            "arrival": arr,
            "lat": float(lat),
            "lon": float(lon),
            "aircraft_icao": aircraft_icao,
        }

# =========================
# TELEX SENDER
# =========================
def send_telex(to_callsign: str, message: str):
    """
    TODO: 把你现有的 Hoppie/ACARS 发送函数粘进来。
    你昨天已经实现过 “欢迎信息 / TSAT 更新” 的发送，这里同样方式发即可。
    """
    # 示例：仅打印（先确认逻辑正确）
    print(f"\n=== TELEX OUT ===\nTO: {to_callsign}\n{message}\n===============")

def build_telex(arr: str, stand: str, label: str) -> str:
    # 你要“100NM 直接给具体机位”，所以消息短、直接
    # 仍建议加 SUBJ CHG（因为是 ruleset）
    return (
        f"GOC {arr} ARR INFO\n"
        f"EST STAND: {stand} ({label}) - SUBJ CHG\n"
    )

# =========================
# MAIN LOOP
# =========================
def main():
    ensure_dirs()

    sent_flags: Dict[str, Any] = load_json(SENT_FLAGS_PATH, default={})
    lru_state: Dict[str, Any] = load_json(LRU_STATE_PATH, default={})

    print("GOC Stand @100NM trigger ONLINE")

    while True:
        try:
            data = fetch_vatsim_data()
            changed = False

            for f in iter_relevant_flights(data):
                cs = f["callsign"]
                arr = f["arrival"]
                lat, lon = f["lat"], f["lon"]

                if arr not in AIRPORT_COORDS:
                    continue
                apt_lat, apt_lon = AIRPORT_COORDS[arr]
                dist_nm = haversine_nm(lat, lon, apt_lat, apt_lon)

                # 每个 callsign + arrival 只发一次（避免改航路/掉线重连也重复）
                key = f"{cs}|{arr}|stand100nm_sent"
                already_sent = bool(sent_flags.get(key, False))

                if (dist_nm <= TRIGGER_NM) and (not already_sent):
                    pred = predict_stand(arr, cs, f["aircraft_icao"], lru_state)
                    msg = build_telex(arr, pred["stand"], pred["label"])
                    send_telex(cs, msg)

                    sent_flags[key] = True
                    changed = True

            if changed:
                save_json(SENT_FLAGS_PATH, sent_flags)
                save_json(LRU_STATE_PATH, lru_state)

        except Exception as e:
            print(f"[WARN] loop error: {e}")

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
