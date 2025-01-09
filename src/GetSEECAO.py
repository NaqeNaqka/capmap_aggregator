from RequestSEECAOAreas import getAreas
from RequestSEECAOBorders import getAuctions

import json
import time
from time import sleep
from datetime import datetime
import tracemalloc

import asyncio
import aiohttp
from aiohttp import ClientSession, ServerDisconnectedError

from logging_config import setup_logging
logger = setup_logging()

retries = 3
delay = 1
def getSEECAO(start_date, end_date, horizon):
    #get all area code pairs from SEECAO
    for attempt in range(1, retries + 1):
        requestFailed = False
        try:
            area_data = getAreas()
            parsed_area_data = json.loads(area_data)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}. Attempt {attempt} of {retries}. Retrying in {delay} seconds...")
            requestFailed = True
            
        if requestFailed:   
            sleep(delay)
            
    if requestFailed:  
        # If all retries fail, raise an exception
        raise Exception(f"Failed to fetch SEECAO's border IDs after {retries} attempts.")
    
    logger.info(f"Collected area code pairs from SEECAO. Horizon {horizon}.")
        
    # key value pairs stored in dicts using labels (border names) as keys
    border_id_by_label = {border["label"]: border["value"] for border in parsed_area_data["borders"]}

    # Store all border IDs (area codes) in a list
    id_list = []
    for key in border_id_by_label:
        id_list.append(border_id_by_label[key])

    #format parameters
    fromDate = start_date.strftime('%Y-%m-%d')
    toDate = end_date.strftime('%Y-%m-%d')

    #get all auctions matching parameters
    for attempt in range(1, retries + 1):
        requestFailed = False
        try:
            auctionData = getAuctions(fromDate, toDate, id_list, horizon.lower())
        except:
            logger.error("Could not get auction data from SEECAO")
    
        if requestFailed:
            sleep(delay)
    
    if requestFailed:  
        # If all retries fail, raise an exception
        raise Exception(f"Failed to fetch SEECAO's auction data after {retries} attempts.")
    
    logger.info(f"Collected auction data from SEECAO. Horizon {horizon}.")

    try: #type list
        auctions = json.loads(auctionData).get("auctions")
    except Exception as e:
        raise Exception(f"Failed to parse auction data from SEECAO:\n{e}")

    asyncio.run(processAuctions(auctions, horizon))
    
    return auctions

async def processAuctions(auctionsList, horizon): 
    async with ClientSession() as session:
        tasks = []
        for auction in auctionsList:
            auctionID = auction.get("auctionId")
            tasks.append(getAuctionSpecs(auctionID, session))

        responses = await asyncio.gather(*tasks)

        for response in responses:
            auctionSpecs = response.get("auctionData")
            currAuctionID = auctionSpecs.get("auctionIdentification")
            
            for auction in auctionsList:
                if auction.get("auctionId") == currAuctionID:
                    index = auctionsList.index(auction)
                    border = auction.get('border', 'N/D').replace(" ", "")
                    
                    if horizon.lower() == "yearly":
                        month = "Y"
                    else:
                        month = auction.get("month")
                    
                    processedAuction = {
                                'Year': auction.get("year"),
                                'Month': month,
                                'Border': border,
                                'Market period start': auction.get('deliveryPeriodStart'),
                                'Market period stop': auction.get('deliveryPeriodEnd'),
                                'AuctionId': currAuctionID,
                                'TimeTable': auction.get('timetable', 'N/D'),
                                'OfferedCapacity (MW)': auction.get('offered', "N/D"),
                                'Return (MW)': auction.get('return', "N/D"),
                                'ATC (MW)': auction.get('atc', "N/D"),
                                'Total requested capacity (MW)': auction.get('requested', "N/D"),
                                'Price (€/MWH)': auction.get('price', "N/D"),
                                'Total allocated capacity (MW)': auction.get('allocated', "N/D"),
                                'Number of participants': auction.get('numberOfParticipants', "N/D"),
                                'Awarded participants': auction.get('numberOfSuccessfullParticipants', "N/D"),
                                'Additional information': '-',
                                'Maintenances': auctionSpecs.get('maintancePeriods', 'none'),
                                'Source': "SEECAO"
                            }
                    
                    auctionsList.remove(auction)
                    if not auction.get("cancelled"):
                        auctionsList.insert(index, processedAuction)
                    else:
                        logger.info(f"Removed cancelled auction {currAuctionID}")
                        

async def getAuctionSpecs(auctionID, session):
    url = f"https://api.seecao.com/api/data?auctionIdentification={auctionID}"
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

    for attempt in range(1, retries + 1):
        requestFailed = False
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Collected specifications for {auctionID}.")
                    return data
                else:
                    logger.info(f"Failed data retrieval for {auctionID}. Status code: {response.status}\nServer response:")
                    response_text = await response.text()
                    logger.info(response_text)
                
                
        except ServerDisconnectedError:
            logger.info(f"Server disconnected. Attempt {attempt} of {retries}. Retrying in {delay} seconds...")
            requestFailed = True
        except aiohttp.ClientError as e:
            logger.info(f"Client error: {e}. Attempt {attempt} of {retries}. Retrying in {delay} seconds...")
            requestFailed = True
        except Exception as e:
            logger.info(f"Unexpected error: {e}. Attempt {attempt} of {retries}. Retrying in {delay} seconds...")
            requestFailed = True

        if requestFailed:   
            await asyncio.sleep(delay)

    # If all retries fail, raise an exception
    raise Exception(f"Failed to fetch {url} after {retries} attempts.") 


    
if __name__ == "__main__":
    tracemalloc.start()
    start = time.perf_counter()

    start_date = datetime(2019, 12, 1, 23, 0, 0)  # December 1, 2019, 23:00:00
    end_date = datetime(2025, 1, 1, 23, 59, 59)  # January 1, 2025, 23:59:59
    
    yearly_auctions = getSEECAO(start_date, end_date, "Yearly")
    monthly_auctions = getSEECAO(start_date, end_date, "Monthly")
    all_data = yearly_auctions + monthly_auctions
    

    if all_data:
        seecaoTestFileName = "seecao_test.json"
        with open(seecaoTestFileName, 'w') as file:
                file.write(json.dumps(all_data))
        logger.info(f"Data successfully exported to {seecaoTestFileName}")

    else:
        logger.info("\nNo Data collected.")


    end = time.perf_counter()
    curr, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
        
    def convert_size(size_bytes):
        # Handle the case for 0 bytes
        if size_bytes == 0:
            return "0B"
        
        # Define the units
        size_name = ("B", "KB", "MB", "GB", "TB", "PB")
        i = int((size_bytes).bit_length() - 1) // 10  # Find which unit to use
        p = 1024 ** i
        s = size_bytes / p
        return f"{s:.2f} {size_name[i]}"

    converted_size = convert_size(peak)

    logger.info(f"Finished in {end-start:.2f}sec.")
    logger.info(f"Peak memory usage: {converted_size}.")
    