from dotenv import load_dotenv
load_dotenv()

import os
from supabase import create_client
from supabase.client import ClientOptions
from storage3.utils import StorageException

def uploadToSupa():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    print("Connecting to Supabase...")
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
    print("Signing in...")
    try:
        session = supabase.auth.sign_in_with_password({
            "email": email, "password":passw
        })
    except Exception as e:
        SIGN_OUT()
        raise Exception(f"Unhandled Error:\n{e}")
        
        
    BUCKET_NAME = 'capmap-storage'

    print("Getting list of bucket files...")
    response = supabase.storage.from_(BUCKET_NAME).list(
    "",
    {"limit": 10, "offset": 0, "sortBy": {"column": "name", "order": "desc"}},
    )

    UPSERT = "false"
    if response:
        print("File already exists. It will be overwritten by the local version.")
        UPSERT = "true"

        
    auctionsFileName = "auctions.json"
    print("Uploading auctions...")
    with open(auctionsFileName, 'rb') as f:
        try:
            response = supabase.storage.from_(BUCKET_NAME).upload(
                file=f,
                path=auctionsFileName,
                file_options={"cache-control": "3600", "upsert": UPSERT},
            )
            print(response)
        except StorageException as e:
            SIGN_OUT()
            raise Exception(e)

            
    SIGN_OUT()
    
    
    
def checkRemoteFileDate():
    from datetime import datetime
    import pytz
    
    print("Initiating last update check...")
    
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    print("Connecting to Supabase...")
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
    print("Signing in...")
    try:
        session = supabase.auth.sign_in_with_password({
            "email": email, "password":passw
        })
    except Exception as e:
        SIGN_OUT()
        raise Exception(f"Unhandled Error:\n{e}")
        
        
    BUCKET_NAME = 'capmap-storage'

    print("Getting list of bucket files...")
    response = supabase.storage.from_(BUCKET_NAME).list(
    "",
    {"limit": 10, "offset": 0, "sortBy": {"column": "name", "order": "desc"}},
    )

    if not response:
        print("No remote files were found.")
        return None

    #TODO use updated_at, created_at
    try:
        responseData = response[0]["updated_at"]
    except:
        print("No updated_at value was provided, using creation date...")
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
    print(response)
    