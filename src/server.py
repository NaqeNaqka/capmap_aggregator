import sys
import asyncio
import queue
import logging
import threading
from fastapi import FastAPI, WebSocket, Request, BackgroundTasks
from fastapi.responses import HTMLResponse
from starlette.responses import StreamingResponse

from aggregate import main

# Set up a global logger
logger = logging.getLogger("my_fastapi_app")
logger.setLevel(logging.INFO)

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
        <p>Please don't reload.</p>
    </head>
    <body>
        <h1>Log Stream</h1>
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

log_queue = asyncio.Queue()

async def generate_logs():
    """Generator for SSE log messages."""
    while True:
        try:
            msg = log_thread_queue.get(block=True, timeout=1)  # Wait for logs
            yield f"data: {msg}\n\n"
        except queue.Empty:
            await asyncio.sleep(0.1)
            
@app.get("/sse/logs")
async def sse_logs():
    """Endpoint for server-sent events to stream logs."""
    return StreamingResponse(generate_logs(), media_type="text/event-stream")
    
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
    background_tasks.add_task(main)  # Call main in the background                    
    return HTMLResponse(html)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
