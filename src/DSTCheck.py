from datetime import datetime, timedelta
#checks if date is in daylight savings time

def isInDST(date_string):
    
    # Convert to datetime object
    date = datetime.strptime(date_string, '%Y-%m-%d')
    
    # Define the start and end dates of DST for Europe in 2024
    dst_start = datetime(date.year, 3, 31)  # Last Sunday in March
    dst_end = datetime(date.year, 10, 27)   # Last Sunday in October
    
    # Find the last Sunday in March
    while dst_start.weekday() != 6:
        dst_start -= timedelta(days=1)
        
    # Find the last Sunday in October
    while dst_end.weekday() != 6:
        dst_end -= timedelta(days=1)
    
    # Check if the date is between start and end dates of DST
    return dst_start <= date < dst_end


# Example usage:

#date_to_check = datetime(2024, 10, 27)
#if isInDST(date_to_check):
    print("The date is in Daylight Saving Time in Europe.")
#else:
#    print("The date is not in Daylight Saving Time in Europe.")