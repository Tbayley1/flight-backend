import asyncio
import asyncpg
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# Keep track of who is connected to the WebSocket
connected_clients = set()

async def listen_to_postgres():
    """Connects to Postgres and listens for NOTIFY events."""
    try:
        conn = await asyncpg.connect(
            user="user",
            password="password",
            database="flight_data",
            host="postgres",
            port=5432
        )

        async def handle_notification(connection, pid, channel, payload):
            print(f"🔔 NEW DEAL DETECTED! Broadcasting to {len(connected_clients)} clients...", flush=True)
            for client in connected_clients:
                # We use a try/except here just in case a client drops mid-broadcast
                try:
                    await client.send_text(payload)
                except Exception:
                    pass

        await conn.add_listener("anomaly_channel", handle_notification)
        print("🎧 API is now listening to Postgres 'anomaly_channel'...", flush=True)

        while True:
            await asyncio.sleep(1)

    except Exception as e:
        print(f"Database connection error: {e}")


# --- THE MODERN WAY TO HANDLE STARTUP ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Everything before the 'yield' runs on STARTUP
    db_task = asyncio.create_task(listen_to_postgres())
    yield
    # Everything after the 'yield' runs on SHUTDOWN
    db_task.cancel()

app = FastAPI(title="Flight Deal Event Gateway", lifespan=lifespan)
# -----------------------------------------


@app.websocket("/ws/deals")
async def websocket_endpoint(websocket: WebSocket):
    """The endpoint that frontend clients connect to."""
    await websocket.accept()
    connected_clients.add(websocket)
    print("💻 New client connected to WebSocket!", flush=True)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        print("💻 Client disconnected.", flush=True)