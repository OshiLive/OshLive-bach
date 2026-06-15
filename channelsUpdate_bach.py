import requests
import psycopg2
from psycopg2.extras import execute_values
import logging
import os
import re
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# 1. 환경 변수 로드
load_dotenv()
API_KEY = os.getenv("API_KEY")
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "port": os.getenv("DB_PORT")
}

# --- 로깅 설정 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("batch_update.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_db_connection():
    # options 설정을 통해 인코딩 및 타임아웃 보조 설정 가능
    dsn = (f"host={DB_CONFIG['host']} dbname={DB_CONFIG['database']} "
           f"user={DB_CONFIG['user']} password={DB_CONFIG['password']} "
           f"port={DB_CONFIG['port']} options='-c client_encoding=utf8'")
    return psycopg2.connect(dsn)

def get_existing_data():
    """기존 데이터를 가져올 때만 잠깐 연결하고 바로 닫음"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT channel_id, name, subscriber_count, video_count, profile_img_url FROM oshilive.channels")
        rows = cur.fetchall()
        return {row[0]: {"name": row[1], "subs": row[2], "vids": row[3], "photo": row[4]} for row in rows}
    except Exception as e:
        logging.error(f"❌ 기존 데이터 로드 중 에러: {e}")
        return {}
    finally:
        if conn:
            cur.close()
            conn.close()

def create_tuple(channel):
    return (
        channel.get('id'),
        channel.get('name'),
        channel.get('english_name'),
        channel.get('org'),
        channel.get('photo'),
        channel.get('twitter'),
        channel.get('subscriber_count', 0),
        channel.get('video_count', 0),
        True
    )

def save_to_db(values):
    """실제 저장이 필요할 때만 연결해서 처리 (점유 시간 최소화)"""
    if not values:
        return

    query = """
    INSERT INTO oshilive.channels (
        channel_id, name, english_name, org, 
        profile_img_url, twitter_id, 
        subscriber_count, video_count, is_active
    ) VALUES %s
    ON CONFLICT (channel_id) DO UPDATE SET
        name = EXCLUDED.name,
        english_name = EXCLUDED.english_name,
        org = EXCLUDED.org,
        profile_img_url = EXCLUDED.profile_img_url,
        twitter_id = EXCLUDED.twitter_id,
        subscriber_count = EXCLUDED.subscriber_count,
        video_count = EXCLUDED.video_count,
        is_active = EXCLUDED.is_active,
        updated_at = CURRENT_TIMESTAMP;
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        execute_values(cur, query, values)
        conn.commit()
        logging.info(f"✅ DB 저장 성공 (총 {len(values)}건)")
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"❌ DB 저장 에러: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            logging.info("🔒 DB 커넥션 반납 완료")

def check_and_fix_single_image(channel, old_photo=None):
    """
    개별 채널의 프로필 이미지 유효성을 검사하고, 깨진 이미지(404)인 경우 
    기존 DB에 저장된 백업 주소(googleusercontent)를 재사용하거나 유튜브 채널 페이지에서 직접 og:image를 파싱하여 복구합니다.
    """
    c_id = channel.get('id')
    c_name = channel.get('name')
    photo_url = channel.get('photo')
    
    if not photo_url:
        return channel
        
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        # 1. GET stream=True 와 User-Agent 헤더로 SSL EOF 및 봇 차단 우회
        resp = requests.get(photo_url, headers=headers, stream=True, timeout=3)
        status_code = resp.status_code
        resp.close()  # 커넥션 자원 즉시 반환
        
        if status_code == 404:
            logging.info(f"⚠️ [이미지 깨짐 감지] {c_name} ({c_id}) - 복구 시도")
            
            # 2. 기존 DB에 이미 복구된 유효한 googleusercontent.com 주소가 있다면, 스크래핑 없이 바로 재사용
            if old_photo and "googleusercontent.com" in old_photo:
                logging.info(f"♻️ [이미지 재사용] {c_name} -> 기존 DB 백업 주소 사용 ({old_photo[:50]}...)")
                channel['photo'] = old_photo
                return channel
                
            # 3. 유튜브 채널 페이지에서 실시간 og:image 태그 긁어오기 (No API Key 우회법)
            yt_url = f"https://www.youtube.com/channel/{c_id}"
            yt_resp = requests.get(yt_url, headers=headers, timeout=5)
            if yt_resp.status_code == 200:
                match = re.search(r'<meta property="og:image" content="([^"]+)"', yt_resp.text)
                if match:
                    new_photo = match.group(1)
                    logging.info(f"✅ [이미지 복구 성공] {c_name} ➔ {new_photo}")
                    channel['photo'] = new_photo
                else:
                    match2 = re.search(r'"avatar":.*?{"url":"([^"]+)"', yt_resp.text)
                    if match2:
                        new_photo = match2.group(1)
                        logging.info(f"✅ [이미지 복구 성공(avatar)] {c_name} ➔ {new_photo}")
                        channel['photo'] = new_photo
                    else:
                        logging.warning(f"❌ [이미지 복구 실패] {c_name} - 유튜브 페이지에서 이미지 태그를 찾지 못함")
            else:
                logging.warning(f"❌ [이미지 복구 실패] {c_name} - 유튜브 채널 페이지 접근 실패 ({yt_resp.status_code})")
    except Exception as e:
        logging.warning(f"⚠️ {c_name} 이미지 검증 중 에러 발생: {e}")
        # SSL 에러 등으로 검증에 실패한 경우 기존 DB 이미지가 있으면 일단 보존하는 안전 장치 추가
        if old_photo:
            logging.info(f"🛡️ [검증 실패 대응] {c_name} -> 기존 DB 주소 보존 ({old_photo[:50]}...)")
            channel['photo'] = old_photo
    return channel


def validate_and_fix_channel_images(channels, existing_data):
    """
    업데이트 대상 채널 리스트를 ThreadPoolExecutor를 사용해 병렬로 빠르게 이미지 검증 및 복구 처리합니다.
    """
    fixed_channels = []
    # 과도한 요청 방지 및 병렬 성능을 고려하여 worker 개수 지정
    workers = min(len(channels), 20) if channels else 1
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for c in channels:
            c_id = c.get('id')
            old_photo = existing_data.get(c_id, {}).get('photo') if existing_data else None
            futures.append(executor.submit(check_and_fix_single_image, c, old_photo))
            
        for future in as_completed(futures):
            fixed_channels.append(future.result())
            
    return fixed_channels

def run_batch(force_img_check=False):
    headers = {"X-APIKEY": API_KEY}
    exclude_keywords = ['clip', '클립', '切抜き', '切り抜き', 'fan', 'archive', '다시보기', 'vod', 'replay']
    
    # 1. 시작 시점에 기존 데이터 로드 (연결 바로 닫힘)
    existing_data = get_existing_data()
    if not existing_data and os.getenv("DB_HOST"): # DB 연결 실패 대응
        logging.warning("⚠️ 기존 데이터를 불러오지 못했습니다. 신규 등록 위주로 진행합니다.")

    channels_to_update = []
    limit = 100
    offset = 0
    stats = {"new": 0, "updated": 0, "skipped": 0}

    # 2. API 데이터 수집 루프 (DB 연결 없음)
    logging.info("🌐 Holodex API 데이터 수집 시작...")
    while True:
        list_url = "https://holodex.net/api/v2/channels"
        params = {"limit": limit, "offset": offset, "type": "vtuber", "lang": "ja"}
        
        try:
            # 타임아웃을 30초로 넉넉하게 설정
            resp = requests.get(list_url, headers=headers, params=params, timeout=30)
            if resp.status_code != 200: 
                logging.error(f"API 응답 에러: {resp.status_code}")
                break
            
            data = resp.json()
            if not data: break

            for channel in data:
                c_id = channel.get('id')
                c_name = channel.get('name')
                new_subs = channel.get('subscriber_count', 0)
                new_vids = channel.get('video_count', 0)

                if any(k in (c_name or "").lower() for k in exclude_keywords):
                    continue

                if c_id not in existing_data:
                    logging.info(f"✨ [신규] {c_name}")
                    stats["new"] += 1
                    channels_to_update.append(channel)
                else:
                    old = existing_data[c_id]
                    
                    # 프로필 이미지 변경 판정 고도화
                    photo_changed = False
                    new_photo = channel.get('photo')
                    old_photo = old['photo']
                    if old_photo != new_photo:
                        # DB에 이미 복구된 유튜브 이미지(googleusercontent.com)가 있고
                        # API에서 전달된 이미지가 ggpht.com 계열이면 업데이트 대상에서 제외 (불필요한 404/스크래핑 루프 방지)
                        if old_photo and "googleusercontent.com" in old_photo and new_photo and "ggpht.com" in new_photo:
                            photo_changed = False
                        else:
                            photo_changed = True

                    # force_img_check가 True면 기존 데이터와 비교해 변경이 없어도 강제로 이미지 복구 프로세스 태움
                    if force_img_check or old['name'] != c_name or old['subs'] != new_subs or old['vids'] != new_vids or photo_changed:
                        if not force_img_check:
                            stats["updated"] += 1
                        else:
                            if old['name'] != c_name or old['subs'] != new_subs or old['vids'] != new_vids or photo_changed:
                                stats["updated"] += 1
                            else:
                                stats["skipped"] += 1
                        channels_to_update.append(channel)
                    else:
                        stats["skipped"] += 1

            offset += limit

        except requests.exceptions.RequestException as e:
            logging.error(f"🌐 API 호출 중 네트워크 에러: {e}")
            break

    # 3. 수집 완료 후 이미지 URL 깨짐 검증 및 복구 (병렬 처리 & 100개 단위 청크 저장)
    if channels_to_update:
        total_channels = len(channels_to_update)
        chunk_size = 100
        logging.info(f"📸 이미지 URL 404 여부 검증 및 복구 시작 (총 대상: {total_channels}건, 청크 크기: {chunk_size}건)...")
        
        for i in range(0, total_channels, chunk_size):
            chunk = channels_to_update[i:i + chunk_size]
            logging.info(f"📦 [청크 {i // chunk_size + 1}] 이미지 검사 및 복구 중 ({i + 1}~{min(i + chunk_size, total_channels)} / {total_channels})...")
            
            # 병렬 검증 수행
            fixed_chunk = validate_and_fix_channel_images(chunk, existing_data)
            
            # 튜플 변환
            update_values = [create_tuple(c) for c in fixed_chunk]
            
            # DB 저장
            logging.info(f"💾 [청크 {i // chunk_size + 1}] DB 저장 시작 ({len(update_values)}건)...")
            save_to_db(update_values)
    else:
        logging.info("ℹ️ 변경사항이 없어 DB 저장을 건너뜁니다.")
    
    logging.info(f"🏁 배치 완료 | 신규: {stats['new']} | 수정: {stats['updated']} | 유지/기타: {stats['skipped']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OshiLive 채널 정보 업데이트 배치 스크립트")
    parser.add_argument("--force-img-check", action="store_true", help="수정 변동이 없는 모든 채널에 대해서도 이미지 깨짐 여부를 검사하고 강제 복구합니다.")
    args = parser.parse_args()
    
    run_batch(force_img_check=args.force_img_check)