import time
import httpx
from bs4 import BeautifulSoup

VATSIM_URL = "https://data.vatsim.net/v3/vatsim-data.json"
AIRPORTS = ["LEVC", "LEBL", "LEMD"]
CDM_URL = "https://cdm.vatsimspain.es/CDMViewer.php?airport={icao}"

POLL_SECONDS = 20

def fetch_vatsim_hpf():
    r = httpx.get(VATSIM_URL, timeout=15)
    r.raise_for_status()
    data = r.json()

    hpf = {}
    for p in data.get("pilots", []):
        cs = (p.get("callsign") or "").upper()
        if not cs.startswith("HPF"):
            continue
        fp = p.get("flight_plan") or {}
        dep = (fp.get("departure") or "").upper()
        arr = (fp.get("arrival") or "").upper()
        hpf[cs] = {"dep": dep, "arr": arr}
    return hpf

def fetch_cdm_airport(icao: str):
    url = CDM_URL.format(icao=icao)
    r = httpx.get(url, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    out = {}
    for tr in soup.select("tr"):
        cols = [td.get_text(strip=True) for td in tr.select("td")]
        if len(cols) < 5:
            continue
        callsign = cols[0].upper()
        tsat = (cols[4] or "").strip()
        if callsign:
            out[callsign] = tsat  # may be '----'
    return out

def tsat_state_for(cs: str, base: str, cdm_tables: dict):
    table = cdm_tables.get(base, {})
    if cs not in table:
        return ("NOT_IN_CDM", None)
    raw = table[cs]
    if raw and raw != "----":
        return ("TSAT_ASSIGNED", raw)
    return ("IN_CDM_BUT_NO_TSAT", None)

def main():
    print("HPF GOC – TSAT watcher running...")
    print(f"Airports: {', '.join(AIRPORTS)} | Poll: {POLL_SECONDS}s\n")

    last = {}  # (cs, base) -> (state, tsat_value)

    while True:
        try:
            online = fetch_vatsim_hpf()
        except Exception as e:
            print(f"[VATSIM] error: {e}")
            time.sleep(POLL_SECONDS)
            continue

        # prefetch CDM tables
        cdm_tables = {}
        for apt in AIRPORTS:
            try:
                cdm_tables[apt] = fetch_cdm_airport(apt)
            except Exception as e:
                print(f"[CDM] {apt} error: {e}")
                cdm_tables[apt] = {}

        # evaluate
        for cs, info in online.items():
            dep = info["dep"]
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
                else:
                    print(f"[TSAT] {cs} @ {dep}: {state}")

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()