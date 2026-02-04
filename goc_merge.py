import httpx
from bs4 import BeautifulSoup

VATSIM_URL = "https://data.vatsim.net/v3/vatsim-data.json"
AIRPORTS = ["LEVC", "LEBL", "LEMD"]
CDM_URL = "https://cdm.vatsimspain.es/CDMViewer.php?airport={icao}"

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
    """
    Return dict callsign -> tsat_raw (can be '----')
    """
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
        tsat = cols[4]
        if callsign:
            out[callsign] = tsat
    return out

print("HPF GOC – merge VATSIM + CDM...\n")

online = fetch_vatsim_hpf()
print(f"Online HPF flights: {len(online)}")

# pull CDM snapshots
cdm_all = {}
for apt in AIRPORTS:
    cdm_all[apt] = fetch_cdm_airport(apt)

print("\nResults:\n")
for cs, info in online.items():
    dep = info["dep"]
    base = dep if dep in AIRPORTS else ""

    tsat_found = None
    tsat_state = None
    in_cdm = False

    # only check base airport table (dep) if it’s one of the 3
    if base:
        table = cdm_all[base]
        if cs in table:
            in_cdm = True
            tsat_raw = (table[cs] or "").strip()
            if tsat_raw and tsat_raw != "----":
                tsat_found = tsat_raw
                tsat_state = "TSAT_ASSIGNED"
            else:
                tsat_state = "IN_CDM_BUT_NO_TSAT"
        else:
            tsat_state = "NOT_IN_CDM"

    if not base:
        print(f"{cs} dep={dep}: not a base airport, skip CDM")
        continue

    if tsat_found:
        print(f"{cs} @ {base}: TSAT {tsat_found}")
    else:
        print(f"{cs} @ {base}: {tsat_state}")
