from dotenv import load_dotenv
load_dotenv()

import os
from supabase import create_client
from supabase.client import ClientOptions
from storage3.utils import StorageException
from logging_config import setup_logging
logger = setup_logging()

auctionsFileName = "auctions.json"

def uploadToSupa():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    logger.info("Connecting to Supabase...")
    supabase = create_client(url, key,
    options=ClientOptions(
        postgrest_client_timeout=10,
        storage_client_timeout=10,
        schema="public",
    ))

    SIGN_OUT = supabase.auth.sign_out

    email = os.environ.get("SUPABASE_USER")
    passw = os.environ.get("SUPABASE_USER_PASS")
    session = None
    logger.info("Signing in...")
    try:
        session = supabase.auth.sign_in_with_password({
            "email": email, "password":passw
        })
    except Exception as e:
        SIGN_OUT()
        raise Exception(f"Unhandled Error:\n{e}")
        
        
    BUCKET_NAME = 'capmap-storage'

    logger.info("Getting list of bucket files...")
    response = supabase.storage.from_(BUCKET_NAME).list(
    "",
    {"limit": 10, "offset": 0, "sortBy": {"column": "name", "order": "desc"}},
    )

    auctionsFileName = "auctions.json"
    UPSERT = "false"
    if response:
        for item in response:
            if item["name"] == auctionsFileName:
                logger.info(f"{auctionsFileName} already exists. It will be overwritten by the local version.")
                UPSERT = "true"

        
    logger.info("Uploading auctions...")
    with open(auctionsFileName, 'rb') as f:
        try:
            response = supabase.storage.from_(BUCKET_NAME).upload(
                file=f,
                path=auctionsFileName,
                file_options={"cache-control": "3600", "upsert": UPSERT},
            )
            logger.info(response)
        except StorageException as e:
            SIGN_OUT()
            raise Exception(e)
    
    logger.info("Uploading date ranges...")
    with open("aggregation_range.json", 'rb') as f:
        try:
            response = supabase.storage.from_(BUCKET_NAME).upload(
                file=f,
                path="aggregation_range.json",
                file_options={"cache-control": "3600", "upsert": UPSERT},
            )
            logger.info(response)
        except StorageException as e:
            SIGN_OUT()
            raise Exception(e)

            
    SIGN_OUT()
    
    
    
def checkRemoteFileDate():
    from datetime import datetime
    import pytz
    
    logger.info("Initiating last update check...")
    
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    logger.info("Connecting to Supabase...")
    supabase = create_client(url, key,
    options=ClientOptions(
        postgrest_client_timeout=10,
        storage_client_timeout=10,
        schema="public",
    ))

    SIGN_OUT = supabase.auth.sign_out

    email = os.environ.get("SUPABASE_USER")
    passw = os.environ.get("SUPABASE_USER_PASS")
    session = None
    logger.info("Signing in...")
    try:
        session = supabase.auth.sign_in_with_password({
            "email": email, "password":passw
        })
    except Exception as e:
        SIGN_OUT()
        raise Exception(f"Unhandled Error:\n{e}")
        
        
    BUCKET_NAME = 'capmap-storage'

    logger.info("Getting list of bucket files...")
    response = supabase.storage.from_(BUCKET_NAME).list(
    "",
    {"limit": 10, "offset": 0, "sortBy": {"column": "name", "order": "desc"}},
    )

    if not response:
        logger.info("No remote files were found.")
        return None

    #TODO use updated_at, created_at
    try:
        for item in response:
            if item["name"] == auctionsFileName:
                responseData = item["updated_at"]
    except:
        logger.info("No updated_at value was provided, using creation date...")
        responseData = response[0]["created_at"]
        
    lastModifiedDate = datetime.strptime(responseData[:-1], "%Y-%m-%dT%H:%M:%S.%f")
    
    # Set the object's timezone to UTC
    lastModifiedDate_UTC = lastModifiedDate.replace(tzinfo=pytz.utc)

    # Define the Tirana timezone
    tirana_tz = pytz.timezone("Europe/Tirane")

    # Convert UTC datetime to Tirana time
    lastModifiedDate_local = lastModifiedDate_UTC.astimezone(tirana_tz)

    SIGN_OUT()
    return lastModifiedDate_local
    
if __name__ == "__main__":
    response = checkRemoteFileDate()
    logger.info(response)
    