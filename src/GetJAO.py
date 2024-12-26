import asyncio
import aiohttp
from aiohttp import ClientSession, ServerDisconnectedError

import json
import time
from datetime import datetime, timedelta
import tracemalloc

from logging_config import setup_logging
logger = setup_logging()

unwantedBorders = []
corridors = []
all_data = []
date_ranges = []

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json',
    'Origin': 'https://www.jao.eu',
    'Referer': 'https://www.jao.eu/auctions',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
}

async def getCorridors(session, horizon, retries = 3, delay = 1):
    url = 'https://www.jao.eu/api/v1/auction/calls/getcorridorhorizonpairs'
    payload = json.dumps({
        "horizon": horizon
    })
    
    for attempt in range(1, retries + 1):
        requestFailed = False
        try:
            async with session.post(url, headers=headers, data=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    # Extract corridorCode values into a list
                    global corridors
                    for item in data:
                        border = item["corridorCode"]
                        if border not in unwantedBorders and border not in corridors:
                            corridors.append(border)
                         
                    logger.info(f"Collected corridor pairs from JAO. Horizon {horizon}.")
                    return
                else:
                    logger.info(f"Failed pairs retrieval for horizon {horizon}. Status code: {response.status}.\nReason:")
                    if (response.status == 405 or response.status == 400):
                        response_text = await response.text()
                        logger.info("Unhandled Bad Request: ", response_text)
                        requestFailed = True           
        
        except ServerDisconnectedError:
            logger.info(f"Server disconnected. Attempt {attempt} of {retries}. Retrying in {delay} seconds...")
            requestFailed = True
        except aiohttp.ClientError as e:
            logger.info(f"Client error: {e}. Attempt {attempt} of {retries}. Retrying in {delay} seconds...")
            requestFailed = True
        except Exception as e:
            logger.info(f"Unexpected error: {e}. Attempt {attempt} of {retries}. Retrying in {delay} seconds...")
            requestFailed = True
        except InterruptedError as e:
            logger.info("Interrupt instruction received")
            break
            
        if requestFailed:   
            await asyncio.sleep(delay)
            
    if requestFailed:  
        # If all retries fail, raise an exception
        raise Exception(f"Failed to fetch {url} after {retries} attempts.")        
                
                    
        
async def fetch_auction(session, corridor, date_range, horizon, retries = 3, delay = 1):
    url = "https://www.jao.eu/api/v1/auction/calls/getauctions"
    
    payload = json.dumps({
        "horizon": horizon,
        "corridor": corridor, 
        'fromdate': date_range['fromdate'],
        'todate': date_range['todate']
    })
    
        
    for attempt in range(1, retries + 1):
        requestFailed = False
        try:
            async with session.post(url, headers=headers, data=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Collected {horizon} auction for {corridor} from {date_range['fromdate']} to {date_range['todate']}.")
                    return data
                else:
                    logger.info(f"Failed data retrieval for {corridor} from {date_range['fromdate']} to {date_range['todate']}. Status code: {response.status}.\nReason:")
                    if (response.status == 405 or response.status == 400):
                        response_text = await response.text()
                        
                        # Look for the keyword "\u0022No Data found\u0022" in the response text
                        if '\\u0022No Data found\\u0022' in response_text:
                            logger.info("No Data found.")
                        else:
                            logger.warning("Unhandled Bad Request: ", response_text)
                            
                    return None
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
        

async def aggregate(horizon):
    async with ClientSession() as session:
        await getCorridors(session, horizon)
        tasks = []
        for corridor in corridors:
            for date_range in date_ranges:
                tasks.append(fetch_auction(session, corridor, date_range, horizon))

        responses = await asyncio.gather(*tasks)

        for data in responses:
            if data:
                for auction in data:
                    results = auction.get('results', [])
                    products = auction.get('products', []) 
                    for product in products:
                        for result in results:
                            auctionID = auction.get('identification', 'N/D')
                            
                            if (auction.get('cancelled')):
                                logger.info(f"Cancelled auction skipped. ({auctionID})")
                            else:

                                # Extract the last 9 characters from auction ID
                                last_9_chars = auctionID.split('-')[-2]  # Splits by '-' and takes the last part
                                year = f"20{last_9_chars[:2]}"  # First two characters represent the year
                                month = datetime(int(year), int(last_9_chars[2:4]), 1).strftime('%b')   # Next two characters represent the month
                                
                                if horizon == "Yearly":
                                    month = "Y"

                                newAuction = {
                                    'Year': year,
                                    'Month': month,
                                    'Market period start': auction.get('marketPeriodStart'),
                                    'Market period stop': auction.get('marketPeriodStop'),
                                    'AuctionId': auctionID,
                                    'Border': auction.get('corridorCode', 'N/D'),
                                    'TimeTable': product.get('productHour', 'N/D'),
                                    'OfferedCapacity (MW)': result.get('offeredCapacity', "N/D"),
                                    'Return (MW)': product.get('resoldCapacity', "N/D"),
                                    'ATC (MW)': product.get('atc', "N/D"),
                                    'Total requested capacity (MW)': result.get('requestedCapacity', "N/D"),
                                    'Price (€/MWH)': result.get('auctionPrice', "N/D"),
                                    'Total allocated capacity (MW)': result.get('allocatedCapacity', "N/D"),
                                    'Number of participants': product.get('bidderPartyCount', "N/D"),
                                    'Awarded participants': product.get('winnerPartyCount', "N/D"),
                                    'Additional information': auction.get('additionalMessage', '-'),
                                    'Maintenances': auction.get('maintenances', 'none'),
                                    'Source': "JAO"
                                }
                                if newAuction not in all_data:
                                    all_data.append(newAuction)

def getJao(start_date, end_date, horizon):

    if horizon == "Yearly":
        # Initialize the starting year for the loop
        current_year = start_date.year

        while current_year <= end_date.year:
            # Calculate the `fromdate` and `todate` for the current range, shifted one year back
            fromdate = datetime(current_year - 1, 12, 1, 23, 0, 0)
            todate = datetime(current_year, 1, 1, 23, 59, 59)
            
            # Append the range to the list
            date_ranges.append({
                'fromdate': fromdate.strftime('%Y-%m-%d-%H:%M:%S'),
                'todate': todate.strftime('%Y-%m-%d-%H:%M:%S')
            })
            
            # Increment the year
            current_year += 1
    
    else: #Monthly
        current_start_date = start_date
        #populate date_ranges
        # Loop until we reach or surpass the end date
        while current_start_date <= end_date:
            # Calculate the end date for the current range (last moment of the current month)
            next_month_start_date = current_start_date + timedelta(days=32)  # Add a month (approximately)
            next_month_start_date = next_month_start_date.replace(day=1)  # Reset to the first of the next month
            
            # The last second of the current month
            current_end_date = next_month_start_date - timedelta(seconds=1)
            
            # Ensure the 'to date' is set to 23:59:59 for all months except the last
            if current_end_date.month != 12:
                current_end_date = current_end_date.replace(hour=23, minute=59, second=59)
            
            # Handle case if the current_end_date exceeds the overall end date
            if current_end_date > end_date:
                current_end_date = end_date
            
            # Append the current date range to the list
            date_ranges.append({
                'fromdate': current_start_date.strftime('%Y-%m-%d-%H:%M:%S'),
                'todate': current_end_date.strftime('%Y-%m-%d-%H:%M:%S')
            })
            
            # Move to the next month for the next iteration
            current_start_date = next_month_start_date
    
    #out of loop
    asyncio.run(aggregate(horizon))

    return all_data

    
if __name__ == "__main__":
    tracemalloc.start()
    start = time.perf_counter()

    start_date = datetime(2022, 1, 1, 23, 0, 0)  # January 1, 2024, 23:00:00
    end_date = datetime(2024, 12, 31, 23, 59, 59)  # December 31, 2024, 23:59:59
    
    yearly_auctions = getJao(start_date, end_date, "Yearly")
    monthly_auctions = getJao(start_date, end_date, "Monthly")
    
    all_data = yearly_auctions + monthly_auctions

    if all_data:
        jaoTestFileName = "jao_test.json"
        with open(jaoTestFileName, 'w') as file:
                file.write(json.dumps(all_data))
        logger.info(f"Data successfully exported to {jaoTestFileName}")

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
    