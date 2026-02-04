import httpx

VATSIM_URL = "https://data.vatsim.net/v3/vatsim-data.json"
BASES = {"LEVC", "LEBL", "LEMD"}

def is_on_ground(pilot: dict) -> bool:
    alt = pilot.get("altitude", 0)
    gs = pilot.get("groundspeed", 0)
    return alt < 200 and gs < 40

print("HPF GOC – fetching VATSIM data...")

r = httpx.get(VATSIM_URL, timeout=15)
r.raise_for_status()
data = r.json()

pilots = data.get("pilots", [])

hpf_pilots = []

for p in pilots:
    callsign = (p.get("callsign") or "").upper()
    if not callsign.startswith("HPF"):
        continue

    fp = p.get("flight_plan") or {}
    dep = (fp.get("departure") or "").upper()
    arr = (fp.get("arrival") or "").upper()

    ground = is_on_ground(p)

    hpf_pilots.append({
        "callsign": callsign,
        "dep": dep,
        "arr": arr,
        "ground": ground
    })

print(f"Found {len(hpf_pilots)} HPF flights online:\n")

for f in hpf_pilots:
    flag = "GROUND" if f["ground"] else "AIR"
    base = ""
    if f["dep"] in BASES:
        base = f" (BASE {f['dep']})"

    print(f"{f['callsign']}  {f['dep']}→{f['arr']}  {flag}{base}")
