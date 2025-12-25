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
import random

SEGMENT_DURATION = 15  # 분석할 오디오 길이 (초) - 정확도 향상

# Firebase 설정
CRED_PATH = os.getenv("SHAZAMIO_CRED_PATH", "serviceAccountKey.json")
DATABASE_URL = os.getenv("SHAZAMIO_DATABASE_URL", "https://tbsapp-function-default-rtdb.asia-southeast1.firebasedatabase.app")
 

# Global credentials object
firebase_creds = None
# State management dictionaries (keyed by channel_id)
LAST_DETECTED_KEY = {}
LAST_SENT_STATUS = {}

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

async def save_to_firebase_rest(data, channel_id):
    """Save data to Firebase RTDB via REST API for a specific channel"""
    if not FIREBASE_READY or not firebase_creds:
        return

    token = get_access_token()
    if not token:
        print(f"   [{channel_id}] -> ❌ Firebase Token Error")
        return

    # Data to save
    db_data = data.copy()
    db_data['timestamp_server'] = int(time.time())
    db_data['detected_at_readable'] = time.strftime('%Y-%m-%d %H:%M:%S')

    # URLs (Option A: Sub-paths)
    # Remove trailing slash from DATABASE_URL if present
    base_url = DATABASE_URL.rstrip('/')
    now_playing_url = f"{base_url}/tbs_radio/{channel_id}/now_playing.json?access_token={token}"
    history_url = f"{base_url}/tbs_radio/{channel_id}/history.json?access_token={token}"

    async with aiohttp.ClientSession() as session:
        try:
            # 1. Update Now Playing (PUT replaces data)
            async with session.put(now_playing_url, json=db_data) as resp:
                if resp.status != 200:
                    print(f"   [{channel_id}] -> ❌ Now Playing Update Failed: {resp.status} {await resp.text()}")

            # 2. Add to History (POST generates new ID)
            async with session.post(history_url, json=db_data) as resp:
                 if resp.status != 200:
                    print(f"   [{channel_id}] -> ❌ History Save Failed: {resp.status} {await resp.text()}")
                 else:
                    print(f"   [{channel_id}] -> 📤 Saved to Firebase RTDB (REST)")
                    
        except Exception as e:
            print(f"   [{channel_id}] -> ❌ REST API Request Error: {e}")

async def clear_now_playing_rest(channel_id):
    """Clear the now_playing node in Firebase dict for a specific channel"""
    if not FIREBASE_READY or not firebase_creds:
        return

    token = get_access_token()
    if not token:
        return

    # Delete or set to null
    base_url = DATABASE_URL.rstrip('/')
    now_playing_url = f"{base_url}/tbs_radio/{channel_id}/now_playing.json?access_token={token}"

    async with aiohttp.ClientSession() as session:
        try:
            # Send empty JSON {} to clear
            async with session.put(now_playing_url, json={}) as resp:
                if resp.status != 200:
                    print(f"   [{channel_id}] -> ❌ Clear Now Playing Failed: {resp.status}")
                # else:
                #    print(f"   [{channel_id}] -> 🗑️ Now playing cleared.")
        except Exception as e:
            print(f"   [{channel_id}] -> ❌ Clear Request Error: {e}")


async def capture_audio_segment(url, duration, output_file):
    """
    ffmpeg를 사용하여 HLS 스트림에서 오디오 세그먼트를 캡처합니다.
    """
    # ffmpeg 명령어 구성
    cmd = [
        "ffmpeg",
        "-i", url,
        "-t", str(duration),
        "-vn",
        "-acodec", "pcm_s16le", # WAV로 변경하여 CPU 사용량 감소 (인코딩 부하 제거)
        "-ar", "44100",
        "-ac", "2",
        "-f", "wav",
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

async def on_music_detected(track_info, channel_id):
    """
    Music detected handler.
    No global keyword needed since we are mutating the global dictionary, not reassigning it.
    """
    title = track_info.get('title')
    subtitle = track_info.get('subtitle')
    print(f"\n🎉 [{channel_id.upper()}] Music Found: {title} - {subtitle}")
    
    # Firebase 저장 (REST API)
    current_key = track_info.get('key')
    last_key = LAST_DETECTED_KEY.get(channel_id)
    
    if current_key and current_key == last_key:
        print(f"   [{channel_id}] -> ⏭️ Same song detected ({current_key}). Skipping DB write.")
        return

    if FIREBASE_READY:
        await save_to_firebase_rest(track_info, channel_id)
        
        if current_key:
            # Mutate dictionary directly
            LAST_DETECTED_KEY[channel_id] = current_key
    else:
        print(f"   [{channel_id}] -> 🚫 Firebase not ready")

async def monitor_stream(url, channel_id, lock, start_delay=0):
    """
    Monitor a specific stream URL for music.
    """
    if start_delay > 0:
        print(f"⏳ [{channel_id.upper()}] Waiting {start_delay}s to start...")
        await asyncio.sleep(start_delay)

    print(f"📡 Monitoring Stream [{channel_id.upper()}]: {url}")
    
    temp_file = f"temp_segment_{channel_id}.wav"
    
    # Initialize state for this channel
    LAST_DETECTED_KEY[channel_id] = None
    LAST_SENT_STATUS[channel_id] = None
    
    # Shazam 인스턴스 초기화
    shazam = Shazam()

    while True:
        try:
            # 1. 오디오 캡처
            success = await capture_audio_segment(url, SEGMENT_DURATION, temp_file)
            
            if success and os.path.exists(temp_file):
                try:
                    # API 호출 부분을 Lock으로 감싸서 동시 실행 방지
                    async with lock:
                        out = await shazam.recognize_song(temp_file)
                        
                    track = out.get('track')
                    
                    if track:
                        # 음악 감지 성공!
                        await on_music_detected(track, channel_id)
                        LAST_SENT_STATUS[channel_id] = 'music'
                    else:
                        # 음악 아님 (Speech, Noise)
                        
                        # 음악이 안 나오면 Now Playing 삭제
                        if LAST_SENT_STATUS.get(channel_id) != 'empty':
                            print(f"\n[{channel_id.upper()}] Speech/Noise detected (Music stopped).")
                            if FIREBASE_READY:
                                await clear_now_playing_rest(channel_id)
                                print(f"   [{channel_id}] -> ⏹️ Cleared 'now_playing'.")
                            LAST_DETECTED_KEY[channel_id] = None
                            LAST_SENT_STATUS[channel_id] = 'empty'

                except Exception as e:
                    # 인식 중 에러 발생
                    error_msg = str(e)
                    if "URL is invalid" in error_msg:
                        print(f"\n⚠️ [{channel_id}] Shazam API Issue (Rate Limit?): {error_msg}")
                        # 에러 발생 시 더 길게 대기
                        await asyncio.sleep(60)
                        shazam = Shazam()
                    else:
                        print(f"\n[{channel_id}] Error during recognition: {e}")
            else:
                # 스트림 캡처 실패
                print(f"\n⚠️ [{channel_id}] Stream capture failed. Retrying...")
                await asyncio.sleep(30)
                
        except Exception as e:
            print(f"\n[{channel_id}] Critical Error: {e}")
            await asyncio.sleep(30)
            
        # 반복 대기 (채널 별로 엇갈리게 실행하고 Random Jitter 추가)
        # 기본 30초 + 0~5초 랜덤 추가 (간격 늘림)
        wait_time = 30 + random.uniform(0, 5)
        await asyncio.sleep(wait_time)


async def main():
    parser = argparse.ArgumentParser(description="ShazamIO Multi-Stream Detector")
    parser.add_argument("url", nargs="?", help="Override FM Stream URL (Optional)")
    args = parser.parse_args()

    # 스트림 설정
    fm_url = args.url or os.getenv("SHAZAMIO_HLS_URL") or "https://cdnfm.tbs.seoul.kr/tbs/_definst_/8434_tbs.stream_audio-only/playlist.m3u8"
    efm_url = "https://cdnefm.tbs.seoul.kr/tbs/_definst_/tbs_efm_app_360.smil/playlist.m3u8"

    print("🚀 Starting ShazamIO Multi-Channel Detector... (FM & eFM) v2.0")
    print("Option A: Separated DB paths (tbs_radio/fm/..., tbs_radio/efm/...)")
    
    # 동시성 제어를 위한 Lock 생성
    api_lock = asyncio.Lock()

    # 두 개의 모니터링 태스크 실행
    await asyncio.gather(
        monitor_stream(fm_url, "fm", api_lock, start_delay=0),
        monitor_stream(efm_url, "efm", api_lock, start_delay=12) 
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Monitoring Stopped.")
