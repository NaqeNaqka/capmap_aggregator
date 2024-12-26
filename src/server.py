from dotenv import load_dotenv
load_dotenv()

import os
import sys
import asyncio
import queue
import logging
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse
from starlette.responses import StreamingResponse

from aggregate import main

# Set up a global logger
logger = logging.getLogger("my_fastapi_app")
logger.setLevel(logging.INFO)

app = FastAPI()

LOG_STREAM = False      # Stream Logs over SSE (GET)

if LOG_STREAM:
    # Create a console handler for logging
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(console_handler)

    # Thread-safe queue for logs
    log_thread_queue = queue.Queue()

    loop = asyncio.new_event_loop()

    class QueueHandler(logging.Handler):
        def emit(self, record):
            msg = self.format(record)
            log_thread_queue.put(msg)
            
    # Add the QueueHandler to the logger
    queue_handler = QueueHandler()
    queue_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(queue_handler)

    log_queue = asyncio.Queue()
    

    html = """
    <!DOCTYPE html>
    <html>
        <head>
            <title>Log Stream</title>
        </head>
        <body>
            <h1>Log Stream</h1>
            <p>Please don't reload during aggregation.</p>
            <pre id="log"></pre>
            <script>
                const eventSource = new EventSource("/sse/logs");
                const logElement = document.getElementById("log");
                eventSource.onmessage = function(event) {
                    logElement.textContent += event.data + "\\n";
                };
            </script>
        </body>
    </html>
    """
else:
    html = """
    <!DOCTYPE html>
    <html>
        <head>
            <title>Log Stream</title>
        </head>
        <body>
            <h1>Log Stream is disabled</h1>
            <p>Use POST to aggregate.</p>
        </body>
    </html>
    """


SECRET_PHRASE = os.environ.get("SECRET_PHRASE")

async def generate_logs():
    """Generator for SSE log messages."""
    while True:
        try:
            msg = log_thread_queue.get(block=True, timeout=1)  # Wait for logs
            yield f"data: {msg}\n\n"
        except queue.Empty:
            await asyncio.sleep(0.1)

if LOG_STREAM:
    @app.get("/sse/logs")
    async def sse_logs():
        """Endpoint for server-sent events to stream logs."""
        return StreamingResponse(generate_logs(), media_type="text/event-stream")
        

@app.get("/", response_class=HTMLResponse)
async def get():
    logger.info("GET request received at root endpoint.")
    return HTMLResponse(html)

from datetime import datetime

@app.post("/run-main")
async def run_main(request: Request, background_tasks: BackgroundTasks):
    """Trigger the main function if the secret phrase is correct and validate optional dates."""
    body = await request.json()
    secret = body.get("secret")
    start_date = body.get("start_date")
    end_date = body.get("end_date")

    # Validate the secret phrase
    if secret != SECRET_PHRASE:
        logger.warning("Invalid secret phrase provided.")
        raise HTTPException(status_code=403, detail="Forbidden: Invalid secret phrase")

    # Validate dates
    if end_date and not start_date:
        logger.warning("End date provided without a start date.")
        raise HTTPException(status_code=400, detail="Bad Request: End date cannot be provided without a start date.")

    def parse_date(date_str):
        """Helper function to parse and validate a date string."""
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Bad Request: Date '{date_str}' is not in the correct format (YYYY-MM-DD)."
            )

    parsed_start_date = parse_date(start_date) if start_date else None
    parsed_end_date = parse_date(end_date) if end_date else None

    # Further validation for dates
    if parsed_start_date and parsed_end_date and parsed_start_date > parsed_end_date:
        logger.warning("Start date is after the end date.")
        raise HTTPException(status_code=400, detail="Bad Request: Start date cannot be after end date.")

    logger.info("POST request received with valid parameters.")
    logger.info(f"Start date: {parsed_start_date}, End date: {parsed_end_date}")

    # Pass the parsed dates to the main function
    background_tasks.add_task(main, parsed_start_date, parsed_end_date)
    return {"message": "Main function started with dates", "start_date": start_date, "end_date": end_date}


if __name__ == "__main__":
    import uvicorn
    import resource
    import sys

    def memory_limit_half():
        """Limit max memory usage to half."""
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        # Convert KiB to bytes, and divide in two to half
        resource.setrlimit(resource.RLIMIT_AS, (int(get_memory() * 1024 / 2), hard))

    def get_memory():
        with open('/proc/meminfo', 'r') as mem:
            free_memory = 0
            for i in mem:
                sline = i.split()
                if str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
                    free_memory += int(sline[1])
        return free_memory  # KiB

    memory_limit_half()
    try:
        uvicorn.run(app, host="0.0.0.0", port=8080)
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)