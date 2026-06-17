import httpx

AIRPORTS = ["LEVC", "LEBL", "LEMD"]
CDM_URL = "https://viff-system.network/ifps/cdmAirport?airport={airport}"

def fetch_cdm(icao: str) -> dict:
    """
    Return {callsign: tsat}
    """
    url = CDM_URL.format(airport=icao)
    r = httpx.get(url, timeout=15)
    r.raise_for_status()

    data_list = r.json()
    rows = {}

    for item in data_list:
        callsign = str(item.get("callsign", "")).upper().strip()
        # 嵌套读取内层cdmData
        cdm_data = item.get("cdmData", {})
        tsat = str(cdm_data.get("tsat", "")).strip()

        if callsign.startswith("HPF") and tsat and tsat not in ("----", "", "-", "N/A"):
            rows[callsign] = tsat

    return rows


print("HPF GOC – fetching CDM TSAT...\n")

for apt in AIRPORTS:
    try:
        tsats = fetch_cdm(apt)
        if not tsats:
            print(f"{apt}: no HPF TSAT")
            continue

        for cs, tsat in tsats.items():
            print(f"{cs} @ {apt} TSAT {tsat}")

    except Exception as e:
        print(f"{apt}: CDM error {e}")