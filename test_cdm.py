import httpx

CDM_URL_TEMPLATE = "https://viff-system.network/ifps/cdmAirport?airport={}"

def test():
    apt = "LEBL"
    res = httpx.get(CDM_URL_TEMPLATE.format(apt), timeout=15)
    data = res.json()
    print(f"{apt} 接口返回航班总数：{len(data)}")
    for item in data:
        cs = item.get("callsign","")
        # 嵌套读取cdmData下的tsat
        cdm_data = item.get("cdmData", {})
        tsat = cdm_data.get("tsat","")
        if cs.startswith("HPF"):
            print(f"呼号:{cs} | TSAT:{tsat}")

if __name__ == "__main__":
    test()