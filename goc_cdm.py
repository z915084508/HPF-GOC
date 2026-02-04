
import httpx
from bs4 import BeautifulSoup

AIRPORTS = ["LEVC", "LEBL", "LEMD"]
CDM_URL = "https://cdm.vatsimspain.es/CDMViewer.php?airport={icao}"

def fetch_cdm(icao: str) -> dict:
    """
    Return {callsign: tsat}
    """
    url = CDM_URL.format(icao=icao)
    r = httpx.get(url, timeout=15)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    rows = {}

    for tr in soup.select("tr"):
        cols = [td.get_text(strip=True) for td in tr.select("td")]
        if len(cols) < 5:
            continue

        callsign = cols[0].upper()
        tsat = cols[4]

        if callsign.startswith("HPF") and tsat and tsat != "----":
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
