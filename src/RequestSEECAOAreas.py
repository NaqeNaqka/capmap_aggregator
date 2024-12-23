import requests
from logging_config import setup_logging
logger = setup_logging()

def getAreas():
    url = "https://api.seecao.com/api/config"

    headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Authorization': 'Bearer false',
    'Connection': 'keep-alive',
    'Origin': 'https://seecao.com',
    'Referer': 'https://seecao.com/auctions/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
    }

    
    response = requests.request("GET", url, headers=headers)

    # Check if the response status code is 400 (Bad Request)
    if response.status_code == 400:
        raise Exception("SEECAO: Bad request. Response: \n", response.text)
        
    elif response.status_code == 200:
        return response.text
    else:
        # Handle other status codes if needed
        logger.error(f"Unexpected status code: {response.status_code}")
        return None