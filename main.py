import asyncio
import os
from dotenv import load_dotenv
import aiohttp

load_dotenv()

from google.oauth2 import service_account
import google.auth.transport.requests
from shazamio import Shazam
import time
import argparse
import json

SEGMENT_DURATION = 10  # 분석할 오디오 길이 (초)
TEMP_AUDIO_FILE = "temp_segment.mp3"

# Firebase 설정
CRED_PATH = os.getenv("SHAZAMIO_CRED_PATH", "serviceAccountKey.json")
DATABASE_URL = os.getenv("SHAZAMIO_DATABASE_URL", "https://tbsapp-function-default-rtdb.asia-southeast1.firebasedatabase.app")
 

# Global credentials object
firebase_creds = None
LAST_DETECTED_KEY = None
LAST_SENT_STATUS = None

def init_firebase_auth():
    """Load Firebase credentials for REST API"""
    global firebase_creds
    if not os.path.exists(CRED_PATH):
        print(f"⚠️ [Firebase] Warning: '{CRED_PATH}' not found. Data will NOT be saved to DB.")
        return False
        
    try:
        scopes = [
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/firebase.database"
        ]
        firebase_creds = service_account.Credentials.from_service_account_file(
            CRED_PATH, scopes=scopes
        )
        print("✅ [Firebase] Auth initialized (REST API mode).")
        return True
    except Exception as e:
        print(f"⚠️ [Firebase] Auth init failed: {e}")
        return False

FIREBASE_READY = init_firebase_auth()

def get_access_token():
    """helper to get a fresh access token"""
    global firebase_creds
    if not firebase_creds:
        return None
    
    # Refresh if expired
    if not firebase_creds.valid:
        request = google.auth.transport.requests.Request()
        firebase_creds.refresh(request)
    
    return firebase_creds.token

async def save_to_firebase_rest(data):
    """Save data to Firebase RTDB via REST API"""
    if not FIREBASE_READY or not firebase_creds:
        return

    token = get_access_token()
    if not token:
        print("   -> ❌ Firebase Token Error")
        return

    # Data to save
    db_data = data.copy()
    db_data['timestamp_server'] = int(time.time())
    db_data['detected_at_readable'] = time.strftime('%Y-%m-%d %H:%M:%S')

    # URLs
    # Remove trailing slash from DATABASE_URL if present
    base_url = DATABASE_URL.rstrip('/')
    now_playing_url = f"{base_url}/tbs_radio/now_playing.json?access_token={token}"
    history_url = f"{base_url}/tbs_radio/history.json?access_token={token}"

    async with aiohttp.ClientSession() as session:
        try:
            # 1. Update Now Playing (PUT replaces data)
            async with session.put(now_playing_url, json=db_data) as resp:
                if resp.status != 200:
                    print(f"   -> ❌ Now Playing Update Failed: {resp.status} {await resp.text()}")

            # 2. Add to History (POST generates new ID)
            async with session.post(history_url, json=db_data) as resp:
                 if resp.status != 200:
                    print(f"   -> ❌ History Save Failed: {resp.status} {await resp.text()}")
                 else:
                    print("   -> 📤 Saved to Firebase RTDB (REST)")
                    
        except Exception as e:
            print(f"   -> ❌ REST API Request Error: {e}")

async def clear_now_playing_rest():
    """Clear the now_playing node in Firebase dict"""
    if not FIREBASE_READY or not firebase_creds:
        return

    token = get_access_token()
    if not token:
        return

    # Delete or set to null
    base_url = DATABASE_URL.rstrip('/')
    now_playing_url = f"{base_url}/tbs_radio/now_playing.json?access_token={token}"

    async with aiohttp.ClientSession() as session:
        try:
            # Send empty JSON {} to clear
            async with session.put(now_playing_url, json={}) as resp:
                if resp.status != 200:
                    print(f"   -> ❌ Clear Now Playing Failed: {resp.status}")
                # else:
                #    print("   -> 🗑️ Now playing cleared.")
        except Exception as e:
            print(f"   -> ❌ Clear Request Error: {e}")


async def capture_audio_segment(url, duration, output_file):
    """
    ffmpeg를 사용하여 HLS 스트림에서 오디오 세그먼트를 캡처합니다.
    """
    print(f"Adding {duration}s audio capture from stream...")
    
    # ffmpeg 명령어 구성
    cmd = [
        "ffmpeg",
        "-i", url,
        "-t", str(duration),
        "-vn",
        "-acodec", "libmp3lame",
        "-f", "mp3",
        "-y",
        "-loglevel", "error",
        output_file
    ]
    
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    stdout, stderr = await process.communicate()
    
    if process.returncode != 0:
        print(f"Error capturing audio: {stderr.decode()}")
        return False
    return True

async def on_music_detected(track_info):
    """
    음악이 감지되었을 때 실행되는 함수입니다.
    이곳에 원하는 로직(알림 전송, 로그 기록 등)을 추가하세요.
    """
    title = track_info.get('title')
    subtitle = track_info.get('subtitle')
    print(f"\n🎉 [ACTION TRIGGERED] Music Found: {title} - {subtitle}")
    
    # 전체 메타데이터 출력 (개발용)
    # import json  <-- Moved to top
    print(json.dumps(track_info, indent=2, ensure_ascii=False))
    
    # Firebase 저장 (REST API)
    global LAST_DETECTED_KEY
    current_key = track_info.get('key')
    
    if current_key and current_key == LAST_DETECTED_KEY:
        print(f"   -> ⏭️ Same song detected ({current_key}). Skipping DB write.")
        # 만약 같은 곡이라도 타임스탬프 갱신이 필요하다면 여기에 로직 추가 (사용자 요청: write하지 않음)
        return

    if FIREBASE_READY:
        await save_to_firebase_rest(track_info)
        # 저장이 성공했든 실패했든 키 업데이트 (실패해도 계속 재시도하지 않도록? 아니면 성공 시에만? -> 일단 업데이트)
        if current_key:
            LAST_DETECTED_KEY = current_key
    else:
        print("   -> 🚫 Firebase not ready (Check serviceAccountKey.json)")


async def main():
    global LAST_DETECTED_KEY, LAST_SENT_STATUS
    parser = argparse.ArgumentParser(description="ShazamIO HLS Stream Detector")
    parser.add_argument("url", nargs="?", help="HLS Stream URL")
    args = parser.parse_args()

    hls_url = args.url or os.getenv("SHAZAMIO_HLS_URL") or "https://cdnfm.tbs.seoul.kr/tbs/_definst_/8434_tbs.stream_audio-only/playlist.m3u8"


    if hls_url == "YOUR_HLS_STREAM_URL_HERE":
        print("경고: URL을 설정해주세요.")
        return

    print(f"📡 Monitoring Stream: {hls_url}")
    print("Pre-buffering and analyzing... (Press Ctrl+C to stop)")

    # Shazam 인스턴스 초기화
    shazam = Shazam()

    while True:
        try:
            # 1. 오디오 캡처 (스트림 상태 확인 겸용)
            success = await capture_audio_segment(hls_url, SEGMENT_DURATION, TEMP_AUDIO_FILE)
            
            if success and os.path.exists(TEMP_AUDIO_FILE):
                try:
                    out = await shazam.recognize(TEMP_AUDIO_FILE)
                    track = out.get('track')
                    
                    if track:
                        # 음악 감지 성공! -> 액션 실행
                        await on_music_detected(track)
                        LAST_SENT_STATUS = 'music'
                    else:
                        # 음악 아님 (Speech, Noise)
                        print(f"\r[Listening] Speech/Noise detected at {time.strftime('%H:%M:%S')}...", end="", flush=True)
                        
                        # 음악이 안 나오면 Now Playing 삭제 (빈 json)
                        
                        # 상태가 empty가 아니면 (즉, 이전에 음악이었거나, 막 시작해서 모르는 경우)
                        if LAST_SENT_STATUS != 'empty':
                            if FIREBASE_READY:
                                await clear_now_playing_rest()
                                print(f"\n   -> ⏹️ Music stopped. Cleared 'now_playing'.")
                            LAST_DETECTED_KEY = None
                            LAST_SENT_STATUS = 'empty'

                except Exception as e:
                    # 인식 중 에러 발생 (예: URL invalid 등)
                    error_msg = str(e)
                    if "URL is invalid" in error_msg:
                        print(f"\n⚠️ Shazam API Issue (Rate Limit? Retrying in 30s...): {error_msg}")
                        # 세션 문제일 수 있으므로 인스턴스 재생성 시도
                        await asyncio.sleep(30) # 대기 시간 증가
                        shazam = Shazam()
                    else:
                        print(f"\nError during recognition: {e}")
            else:
                # 스트림이 오프라인이거나 캡처 실패 시
                print(f"\n⚠️ Stream might be offline. Retrying in 30 seconds...")
                await asyncio.sleep(30)
                
        except Exception as e:
            print(f"\nCritical Error: {e}")
            await asyncio.sleep(30)
            
        # 반복 대기 (너무 빠른 루프 방지 -> API 보호)
        await asyncio.sleep(30)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Monitoring Stopped.")
