import requests
import json

def getAuctions(fromDate, toDate, borderIds, horizon):
    url = "https://api.seecao.com/api/data/filter_export"

    payload = json.dumps({
    "type": horizon,
    "borders": borderIds,
    "dayFrom": fromDate,
    "dayTo": toDate
    })
    
    headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Authorization': 'Bearer false',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json',
    'Origin': 'https://seecao.com',
    'Referer': 'https://seecao.com/export/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code == 400:
        raise Exception("SEECAO: Bad request")
    elif response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Unexpected status code: {response.status_code}")
    