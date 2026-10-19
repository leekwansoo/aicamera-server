# play mp4 video file
import cv2
import base64
import asyncio
import datetime
import random
import os
import json
import uuid
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
from usb_device import list_usb_devices
from main import nvr_handler

from main import STATE, STATIC_DIR, BASE_DIR, CAMERA_RTSP, get_usb_camera_index
CAMERA_RTSP = CAMERA_RTSP
BASE_DIR = BASE_DIR
UPLOAD_DIR = BASE_DIR / "uploads"
STATIC_DIR = STATIC_DIR
STATE = STATE
        
app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Open your USB webcam  with the index obtained from get_usb_camera_index()

from main import _mjpeg_frames, _webcam_mjpeg, play_video, webcam_stream, tapo_camera_stream,camera
# websocket = None  # Replace with an actual WebSocket instance before
@app.get("/")
def index(request: Request):
    return templates.TemplateResponse(request, "index.html")

@app.get("/tapo_camera")
def index(request: Request):
    return templates.TemplateResponse(request, "tapo_camera.html")

@app.get("/video")
def video(request: Request):
    return templates.TemplateResponse(request, "video.html")

@app.get("/webcam_index")
def get_webcam_index():
    camera_index = get_usb_camera_index()
    if camera_index is None:
        camera_index = 0
    return {"camera_index": camera_index}

@app.get("/webcam")
def webcam(request: Request):
    return templates.TemplateResponse(request, "webcam-dashboard.html")

@app.get("/api/camera/{camera_id}/stream")
async def api_camera_stream(camera_id: str):
    """[UPLINK] RTSP -> MJPEG stream for browser <img>"""
    url = CAMERA_RTSP.get(camera_id)
    if not url:
        return JSONResponse({"error": f"Set CAMERA1_RTSP/CAMERA2_RTSP env for {camera_id}"}, status_code=404)
    try:
        import cv2  # noqa: F401
    except ImportError:
        return JSONResponse({"error": "opencv-python not installed"}, status_code=500)
    return StreamingResponse(_mjpeg_frames(url), media_type="multipart/x-mixed-replace; boundary=frame")

@app.get("/api/webcam/stream")
async def api_webcam_stream(webcam: int = 0):
    """[UPLINK] USB Webcam -> MJPEG stream for browser <img>"""
    if not camera.isOpened():
        return JSONResponse({"error": "Webcam not available"}, status_code=404)
    return StreamingResponse(_webcam_mjpeg(), media_type="multipart/x-mixed-replace; boundary=frame")
@app.post("/start_recording")
def start_recording():
    nvr_handler.start_recording()
    return {"status": "recording started"}

@app.post("/stop_recording")
def stop_recording():
    nvr_handler.stop_recording()
    return {"status": "recording stopped"}

@app.post("/api/recording/audio")
async def recording_audio(request: Request):
    data = await request.body()
    print("Received audio data of length:", len(data))
    ok, detail = await asyncio.to_thread(nvr_handler.add_audio, data)
    if not ok:
        return JSONResponse({"error": detail}, status_code=400)
    return {"status": "audio added", "file": detail}

@app.get("/api/status")
async def api_all_status():
    """[UPLINK] Terminal -> Server : Get all devices status (Both-way demo)"""
    # Simulate temp drift
    
    return {
        "timestamp": datetime.datetime.now().isoformat(),
        "direction": "Terminal -> Server (UPLINK)",
        "devices": {
            "Door1": {"status": STATE["Door1"]},
            "Camera1": {"online": True, "motion": random.choice([True, False]), "recording": True},
            "Webcam1": {"online": True, "motion": random.choice([True, False]), "recording": True},
        }
    }
@app.websocket("/ws/bothway")
async def websocket_bothway(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = {
                "type": "UPLINK",
                "timestamp": datetime.datetime.now().isoformat(),
                "Door1": {"status": STATE["Door1"]},
                "Camera1": {"motion": random.choice([True, False])},
                "Webcam1": {"motion": random.choice([True, False])},
            }
            await websocket.send_text(json.dumps(data))
            await asyncio.sleep(10)
    except WebSocketDisconnect:
        pass

@app.post("/api/video/upload")
async def upload_video(request: Request):
    """Receive the raw mp4 bytes and store them for WebSocket playback."""
    data = await request.body()
    if not data:
        return JSONResponse({"error": "empty file"}, status_code=400)
    UPLOAD_DIR.mkdir(exist_ok=True)
    video_id = uuid.uuid4().hex
    (UPLOAD_DIR / f"{video_id}.mp4").write_bytes(data)
    return {"id": video_id}

# WebSocket endpoint for playing uploaded video files
@app.websocket("/ws/play/{video_id}")
async def play_file_ws(websocket: WebSocket, video_id: str):
    await websocket.accept()
    path = UPLOAD_DIR / f"{video_id}.mp4"
    if not video_id.isalnum() or not path.exists():
        await websocket.close(code=1008)
        return
    cap = cv2.VideoCapture(str(path))
    fps = cap.get(cv2.CAP_PROP_FPS) or 30
    delay = 1.0 / min(max(fps, 1), 60)
    loop = asyncio.get_running_loop()
    start = loop.time()
    index = 0
    try:
        while True:
            ok, frame = await asyncio.to_thread(cap.read)
            if not ok:
                break
            frame = cv2.resize(frame, (640, 480))
            nvr_handler.write_frame(frame)
            ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 70])
            if ok:
                await websocket.send_text(base64.b64encode(buf.tobytes()).decode("utf-8"))
            index += 1
            await asyncio.sleep(max(0, start + index * delay - loop.time()))
        await websocket.send_text("END")
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        cap.release()
        path.unlink(missing_ok=True)

# WebSocket endpoint for streaming live video from the webcam
@app.websocket("/ws/video")
async def video_ws(websocket: WebSocket):
    await webcam_stream(websocket)
    
# WebSocket endpoint for streaming live video from the tapo_camera
@app.websocket("/ws/tapo_camera")
async def tapo_camera_ws(websocket: WebSocket):
    await tapo_camera_stream(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("video-server:app", host="127.0.0.1", port=8000, reload=True)