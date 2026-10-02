import gc
import sys
import requests
from psycopg2.extras import execute_values
from core.config import Config
from core.logger import get_logger
from core.db import get_db_cursor

logger = get_logger("ChannelSync")

HOLODEX_CHANNELS_URL = "https://holodex.net/api/v2/channels"

def fetch_all_channels_from_holodex():
    """
    Holodex API (limit=100 배치)를 사용하여 전 세계 버튜버/채널 정보를 일괄 수집
    개별 1개씩 호출하는 N+1 문제 및 404 HTML Scraping을 완전히 제거하여 실행속도 극대화
    """
    headers = {"X-APIKEY": Config.HOLODEX_API_KEY}
    all_channels = []
    offset = 0
    limit = 100

    logger.info("Holodex API 채널 데이터 수집 시작...")

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "type": "vtuber"
        }
        try:
            resp = requests.get(HOLODEX_CHANNELS_URL, headers=headers, params=params, timeout=15)
            if resp.status_code == 429:
                logger.warning(f"Holodex API Rate Limit 발생 (offset={offset}). 잠시 대기...")
                break
            if resp.status_code != 200:
                logger.error(f"Holodex API 호출 실패 (status={resp.status_code}): {resp.text[:200]}")
                break

            data = resp.json()
            if not data:
                break

            all_channels.extend(data)
            logger.info(f"Holodex 배치 수집 완료: +{len(data)}건 (누적: {len(all_channels)}건)")

            if len(data) < limit:
                break

            offset += limit
        except Exception as e:
            logger.error(f"Holodex API 수집 중 예외 발생: {e}")
            break

    return all_channels

def process_and_save_channels(raw_channels):
    """
    수집한 채널 데이터 중 핵심 필드만 추출하여 DB에 Bulk Upsert
    불필요 필드(yt_handle, top_topics, description 등)는 배제하여 메모리 및 DB 용량 방어
    """
    if not raw_channels:
        logger.warning("저장할 채널 데이터가 없습니다.")
        return

    upsert_map = {}
    for item in raw_channels:
        c_id = item.get('id')
        if not c_id or c_id in upsert_map:
            continue

        banner = item.get('banner') or item.get('header')
        # 필요 최소 유효성 검사만 수행 (HTTP HTML Scraper 호출 전면 제거)
        if banner and ("googleusercontent.com" not in banner and "ggpht.com" not in banner):
            banner = None

        subscriber_count = int(item.get('subscriber_count') or 0)
        video_count = int(item.get('video_count') or 0)
        published_at = item.get('published_at')

        upsert_map[c_id] = (
            c_id,
            item.get('name'),
            item.get('english_name'),
            item.get('org'),
            item.get('photo'),
            banner,
            item.get('twitter'),
            subscriber_count,
            video_count,
            published_at,
            True  # is_active
        )

    upsert_tuples = list(upsert_map.values())

    upsert_sql = """
    INSERT INTO oshilive.channels (
        channel_id, name, english_name, org, 
        profile_img_url, banner_img_url, twitter_id, 
        subscriber_count, video_count, published_at, is_active
    ) VALUES %s
    ON CONFLICT (channel_id) DO UPDATE SET
        name = EXCLUDED.name,
        english_name = EXCLUDED.english_name,
        org = EXCLUDED.org,
        profile_img_url = EXCLUDED.profile_img_url,
        banner_img_url = COALESCE(EXCLUDED.banner_img_url, oshilive.channels.banner_img_url),
        twitter_id = EXCLUDED.twitter_id,
        subscriber_count = EXCLUDED.subscriber_count,
        video_count = EXCLUDED.video_count,
        published_at = COALESCE(EXCLUDED.published_at, oshilive.channels.published_at),
        is_active = EXCLUDED.is_active,
        updated_at = CURRENT_TIMESTAMP;
    """

    try:
        with get_db_cursor() as cur:
            execute_values(cur, upsert_sql, upsert_tuples, page_size=200)
        logger.info(f"✅ 채널 데이터 총 {len(upsert_tuples)}건 DB Bulk Upsert 성공!")
    except Exception as e:
        logger.error(f"❌ 채널 DB 저장 실패: {e}")
        raise

def run_channel_sync():
    logger.info("=== 채널 정보 동기화 배치 시작 ===")
    try:
        channels = fetch_all_channels_from_holodex()
        process_and_save_channels(channels)
    finally:
        # 1GB RAM 환경 메모리 방어를 위해 즉시 GC 호출
        if 'channels' in locals():
            del channels
        gc.collect()
        logger.info("=== 채널 정보 동기화 배치 종료 ===")

if __name__ == "__main__":
    run_channel_sync()
