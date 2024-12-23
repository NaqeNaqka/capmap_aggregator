import sys
import asyncio
import queue
import logging
import threading
from fastapi import FastAPI, WebSocket, Request, BackgroundTasks
from fastapi.responses import HTMLResponse

from aggregate import main

# Set up a global logger
logger = logging.getLogger("my_fastapi_app")
logger.setLevel(logging.INFO)

# Global flag and lock to ensure thread-safety
is_main_running = False
main_lock = threading.Lock()

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

app = FastAPI()

html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Log Stream</title>
    </head>
    <body>
        <h1>Log Stream</h1>
        <pre id="log"></pre>
        <script>
            const logElement = document.getElementById("log");
            const socket = new WebSocket("ws://localhost:8000/ws/logs");
            socket.onmessage = function(event) {
                logElement.textContent += event.data + "\\n";
            };
        </script>
    </body>
</html>
"""

log_queue = asyncio.Queue()

async def process_logs():
    """Background task to move logs from thread-safe queue to asyncio queue."""
    while True:
        try:
            msg = log_thread_queue.get(block=True, timeout=1)  # Wait for logs
            await log_queue.put(msg)
        except queue.Empty:
            await asyncio.sleep(0.1)

@app.on_event("startup")
async def startup_event():
    """Start the background task to process logs on app startup."""
    asyncio.create_task(process_logs())
    
@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            message = await log_queue.get()
            await websocket.send_text(message)
    except Exception as e:
        logger.error(f"Error in WebSocket connection: {e}")
    finally:
        await websocket.close()

@app.get("/", response_class=HTMLResponse)
async def get(background_tasks: BackgroundTasks):
    logger.info("GET request received at root endpoint.")
    global is_main_running
    with main_lock:  # Ensure thread-safety
        if is_main_running:
            logger.info("Skipping main; already running.")
            return
        is_main_running = True
    try:
        logger.info("Running main...")
        background_tasks.add_task(main)  # Call main in the background
    finally:
        with main_lock:
            is_main_running = False
                    
    return HTMLResponse(html)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)