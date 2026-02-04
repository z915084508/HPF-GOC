import os
import httpx
from dotenv import load_dotenv

load_dotenv()

HOPPIE_LOGON = os.getenv("HOPPIE_LOGON")
GOC_STATION = os.getenv("GOC_STATION", "HPFGOC").upper()
HOPPIE_CONNECT = "https://www.hoppie.nl/acars/system/connect.html"

def send_telex(to_station: str, text: str):
    data = {
        "logon": HOPPIE_LOGON,
        "from": GOC_STATION,
        "to": to_station.upper(),
        "type": "telex",
        "packet": text,
    }
    r = httpx.post(HOPPIE_CONNECT, data=data, timeout=20)
    r.raise_for_status()
    print("Server:", r.text.strip())

if __name__ == "__main__":
    # 把这里改成你自己在线的 VATSIM 呼号，例如 HPF123
    send_telex("HPF123", "HPF GOC TEST\nIF YOU SEE THIS, LINK IS OK")
