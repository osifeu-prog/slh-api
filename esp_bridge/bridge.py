import serial, asyncio, json, time as _time
from aiohttp import web

PORT = "COM5"
BAUD = 115200

# ----------------------------------------------------------------------
#  Shared helpers
# ----------------------------------------------------------------------
def serial_send(cmd: str, read_bytes: int = 500) -> dict:
    try:
        s = serial.Serial(PORT, BAUD, timeout=2)
        # discard stale data
        s.reset_input_buffer()
        s.write((cmd + "\n").encode("utf-8"))
        _time.sleep(1)
        resp = s.read(read_bytes).decode(errors="ignore").strip()
        s.close()
        return {"ok": True, "response": resp}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def serial_status() -> dict:
    try:
        s = serial.Serial(PORT, BAUD, timeout=1)
        s.close()
        return {"ok": True, "connected": True, "port": PORT, "baudrate": BAUD}
    except Exception as e:
        return {"ok": False, "connected": False, "port": None, "error": str(e)}

# -------- Fake device registry (static for now) --------
_DEVICES = {
    "esp32-14335C6C32C0": {
        "device_id": "esp32-14335C6C32C0",
        "status": "online",
        "fw": "v4",
        "ip": "10.0.0.4",
        "rssi": -42,
        "last_seen": "2026-05-08T16:00:00"
    }
}

# ----------------------------------------------------------------------
#  Request handlers (all endpoints the bot expects)
# ----------------------------------------------------------------------
async def status_handler(request):
    return web.json_response(serial_status())

async def serial_status_handler(request):
    # /api/serial/status
    return web.json_response(serial_status())

async def serial_send_handler(request):
    # /api/serial/send   (JSON: {"command": "..."})
    try:
        data = await request.json()
    except:
        return web.json_response({"ok": False, "error": "invalid json"})
    cmd = data.get("command", data.get("cmd", ""))
    return web.json_response(serial_send(cmd))

async def serial_reset_handler(request):
    # /api/serial/reset — toggle DTR (best effort)
    try:
        s = serial.Serial(PORT, BAUD)
        s.setDTR(False)
        _time.sleep(0.1)
        s.setDTR(True)
        s.close()
        return web.json_response({"ok": True, "message": "DTR reset sent"})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)})

async def serial_ports_handler(request):
    # /api/serial/ports — list COM ports
    import serial.tools.list_ports
    ports = [{"device": p.device, "description": p.description} for p in serial.tools.list_ports.comports()]
    return web.json_response({"ok": True, "ports": ports})

async def esp_status_handler(request):
    # /api/esp/status
    return web.json_response({"ok": True, "devices": list(_DEVICES.values())})

async def esp_device_command(request):
    # /api/esp/commands/{device_id}
    device_id = request.match_info.get("device_id", "")
    try:
        data = await request.json()
    except:
        data = {}
    command = data.get("command", data.get("screen", "HOME"))
    # Forward the command to the serial device
    result = serial_send(command)
    result["device_id"] = device_id
    return web.json_response(result)

async def device_claim(request):
    # /api/device/claim/{device_id}
    device_id = request.match_info.get("device_id", "")
    # Just acknowledge the pairing
    return web.json_response({"ok": True, "device_id": device_id, "signing_token": "dummy_token"})

async def esp_flash(request):
    # /api/esp/flash — not implemented, return mock
    data = await request.json() if request.can_read_body else {}
    port = data.get("port", PORT)
    return web.json_response({"ok": True, "message": "Flash simulated; use PlatformIO for real flashing"})

async def esp_compile(request):
    # /api/esp/compile — not implemented
    return web.json_response({"ok": True, "message": "Compile simulated"})

async def serial_monitor(request):
    # /api/serial/monitor — return last few lines (optional)
    return web.json_response({"ok": True, "output": "Monitor not implemented in minimal bridge"})

# ----------------------------------------------------------------------
#  App & routes
# ----------------------------------------------------------------------
app = web.Application()

# Core endpoints
app.router.add_get("/status", status_handler)
app.router.add_post("/cmd", serial_send_handler)                     # simpler POST /cmd

# API endpoints matching the bot's expectations
app.router.add_get("/api/serial/status", serial_status_handler)
app.router.add_post("/api/serial/send", serial_send_handler)
app.router.add_post("/api/serial/reset", serial_reset_handler)
app.router.add_get("/api/serial/ports", serial_ports_handler)
app.router.add_get("/api/serial/monitor", serial_monitor)
app.router.add_get("/api/esp/status", esp_status_handler)
app.router.add_post("/api/esp/commands/{device_id}", esp_device_command)
app.router.add_post("/api/device/claim/{device_id}", device_claim)
app.router.add_post("/api/esp/flash", esp_flash)
app.router.add_post("/api/esp/compile", esp_compile)

# (Optional) health
app.router.add_get("/api/health", lambda r: web.json_response({"status": "ok"}))

if __name__ == "__main__":
    print("🚀 ESP Bridge listening on http://0.0.0.0:8765")
    web.run_app(app, host="0.0.0.0", port=8765)
