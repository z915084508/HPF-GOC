import os
import time
import threading
from typing import Dict, Tuple, Optional

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

# =========================
# ENV
# =========================
load_dotenv()
HOPPIE_LOGON = os.getenv("HOPPIE_LOGON")
GOC_STATION = os.getenv("GOC_STATION", "HPFGOC")

if not HOPPIE_LOGON:
    raise RuntimeError("Missing HOPPIE_LOGON in .env")


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
# VATSIM
# =========================
def fetch_vatsim_hpf() -> Dict[str, Dict[str, str]]:
    """Return {CALLSIGN: {dep, arr}} for all online HPF* pilots."""
    r = httpx.get(VATSIM_URL, timeout=20, follow_redirects=True)
    r.raise_for_status()
    data = r.json()

    flights: Dict[str, Dict[str, str]] = {}
    for p in data.get("pilots", []):
        cs = (p.get("callsign") or "").upper().strip()
        if not cs.startswith("HPF"):
            continue

        fp = p.get("flight_plan") or {}
        dep = (fp.get("departure") or "").upper().strip()
        arr = (fp.get("arrival") or "").upper().strip()

        flights[cs] = {"dep": dep, "arr": arr}

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

    # Try to find TSAT column index from header
    header_cells = rows[0].find_all(["th", "td"])
    headers = [c.get_text(strip=True).upper() for c in header_cells]
    tsat_idx = None
    callsign_idx = None

    for i, h in enumerate(headers):
        if h in ("CALLSIGN", "ACID", "CS", "CSIGN"):
            callsign_idx = i
        if "TSAT" in h:
            tsat_idx = i

    # Fallbacks (common layout): callsign first col, TSAT around 4/5
    if callsign_idx is None:
        callsign_idx = 0
    if tsat_idx is None:
        # many CDM tables put TSAT at 5th column (index 4)
        tsat_idx = 4

    tsats: Dict[str, str] = {}

    for row in rows[1:]:
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if not cols:
            continue
        if callsign_idx >= len(cols):
            continue

        callsign = (cols[callsign_idx] or "").upper().strip()
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
    print("HPF GOC – AUTO watcher running\n")
    print("Commands:")
    print("  telex <CALLSIGN> <MESSAGE...>   send TELEX")
    print("  ping <CALLSIGN>                send test TELEX")
    print("  help                           show help\n")
    print(f"Airports: {', '.join(AIRPORTS)} | Poll: {POLL_SECONDS}s\n")

    last: Dict[Tuple[str, str], Tuple[str, Optional[str]]] = {}  # (cs, apt) -> (state, tsat)
    welcomed = set()  # callsigns welcomed once per process

    # prevent CDM error spam: only print if message changes
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

            if prev == curr:
                continue

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
