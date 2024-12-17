import json
import tracemalloc
import time
import threading
import concurrent
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from GetJAO import getJao
from GetSEECAO import getSEECAO

print("Aggregator running...")
tracemalloc.start()
start = time.perf_counter()

# Start date and end date
start_date = datetime(2019, 12, 1, 23, 0, 0)  # December 1, 2019, 23:00:00
end_date = datetime(2025, 1, 1, 23, 59, 59)  # January 1, 2025, 23:59:59

all_data = []
listLock = threading.Lock()

def getDataFrom(source, start_date, end_date, horizon):
    if source == "JAO":
        collector = getJao
    else:
        collector = getSEECAO
        
    data = collector(start_date, end_date, horizon)
    with listLock:
        all_data.append(data)
    

with ThreadPoolExecutor(max_workers=10) as executor:
    # Caution: setting the horizon to Yearly will collect auctions based ONLY on the dates' years (JAO)
    executor.submit(getDataFrom, "JAO", start_date, end_date, "Monthly")
    executor.submit(getDataFrom, "JAO", start_date, end_date, "Yearly")
    executor.submit(getDataFrom, "SEECAO", start_date, end_date, "Monthly")
    executor.submit(getDataFrom, "SEECAO", start_date, end_date, "Yearly")

if all_data:
    testFileName = "aggregator_test.json"
    with open(testFileName, 'w') as file:
            file.write(json.dumps(all_data))
    print(f"Data successfully exported to {testFileName}")
else:
    print("\nNo Data collected.")


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

print(f"Finished in {end-start:.2f}sec.")
print(f"Peak memory usage: {converted_size}.")
