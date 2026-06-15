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

def check_and_fix_single_image(channel):
    """
    개별 채널의 프로필 이미지 유효성을 검사하고, 깨진 이미지(404)인 경우 
    유튜브 채널 페이지에서 직접 og:image를 파싱하여 복구합니다.
    """
    c_id = channel.get('id')
    c_name = channel.get('name')
    photo_url = channel.get('photo')
    
    if not photo_url:
        return channel
        
    try:
        # 1. 가볍게 HEAD 요청을 보내 이미지 주소가 살아있는지(404 여부) 체크
        resp = requests.head(photo_url, timeout=3)
        if resp.status_code == 404:
            logging.info(f"⚠️ [이미지 깨짐 감지] {c_name} ({c_id}) - 유튜브 페이지에서 직접 조회 시도")
            
            # 2. 유튜브 채널 페이지에서 실시간 og:image 태그 긁어오기 (No API Key 우회법)
            yt_url = f"https://www.youtube.com/channel/{c_id}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
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
        
    return channel

def validate_and_fix_channel_images(channels):
    """
    업데이트 대상 채널 리스트를 ThreadPoolExecutor를 사용해 병렬로 빠르게 이미지 검증 및 복구 처리합니다.
    """
    fixed_channels = []
    # 과도한 요청 방지 및 병렬 성능을 고려하여 worker 개수 지정
    workers = min(len(channels), 20) if channels else 1
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_channel = {executor.submit(check_and_fix_single_image, c): c for c in channels}
        for future in as_completed(future_to_channel):
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
                    # force_img_check가 True면 기존 데이터와 비교해 변경이 없어도 강제로 이미지 복구 프로세스 태움
                    if force_img_check or old['name'] != c_name or old['subs'] != new_subs or old['vids'] != new_vids or old['photo'] != channel.get('photo'):
                        if not force_img_check:
                            stats["updated"] += 1
                        else:
                            if old['name'] != c_name or old['subs'] != new_subs or old['vids'] != new_vids or old['photo'] != channel.get('photo'):
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

    # 3. 수집 완료 후 이미지 URL 깨짐 검증 및 복구 (병렬 처리)
    if channels_to_update:
        logging.info(f"📸 이미지 URL 404 여부 검증 및 복구 시작 (대상: {len(channels_to_update)}건)...")
        channels_to_update = validate_and_fix_channel_images(channels_to_update)
        
        # 튜플 변환
        update_values = [create_tuple(c) for c in channels_to_update]
        
        logging.info(f"💾 DB 저장 시작 (업데이트 대상: {len(update_values)}건)...")
        save_to_db(update_values)
    else:
        logging.info("ℹ️ 변경사항이 없어 DB 저장을 건너뜁니다.")
    
    logging.info(f"🏁 배치 완료 | 신규: {stats['new']} | 수정: {stats['updated']} | 유지/기타: {stats['skipped']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OshiLive 채널 정보 업데이트 배치 스크립트")
    parser.add_argument("--force-img-check", action="store_true", help="수정 변동이 없는 모든 채널에 대해서도 이미지 깨짐 여부를 검사하고 강제 복구합니다.")
    args = parser.parse_args()
    
    run_batch(force_img_check=args.force_img_check)