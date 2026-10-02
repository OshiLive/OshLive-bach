import argparse
import gc
import sys
from datetime import datetime, timezone
import requests
from psycopg2.extras import execute_values
from core.config import Config
from core.logger import get_logger
from core.db import get_db_cursor

logger = get_logger("LiveSync")

HOLODEX_LIVE_URL = "https://holodex.net/api/v2/live"

def fetch_live_streams(mode: str = "short"):
    """
    Holodex API live 엔드포인트를 호출하여 방송 정보 수집
    - short 모드 (1분 주기): 당일 남은 시간 내 live, upcoming 방송 수집
    - long 모드 (1시간 주기): 향후 30일(720시간) 치 방송 일괄 수집
    """
    headers = {"X-APIKEY": Config.HOLODEX_API_KEY}
    streams = []

    if mode == "short":
        now_local = datetime.now()
        end_of_today = datetime(now_local.year, now_local.month, now_local.day, 23, 59, 59)
        hours_left_today = max(1, int((end_of_today - now_local).total_seconds() / 3600) + 1)
        max_hours = hours_left_today
    else:
        max_hours = 720

    offset = 0
    limit = 100

    while True:
        params = {
            "type": "stream",
            "status": "live,upcoming",
            "limit": limit,
            "offset": offset,
            "max_upcoming_hours": max_hours,
            "include_membersonly": "true"
        }
        try:
            resp = requests.get(HOLODEX_LIVE_URL, headers=headers, params=params, timeout=15)
            if resp.status_code != 200:
                logger.error(f"Holodex API 호출 에러 (status={resp.status_code}): {resp.text[:150]}")
                break

            chunk = resp.json()
            if not chunk:
                break

            streams.extend(chunk)
            if len(chunk) < limit:
                break
            offset += limit
        except Exception as e:
            logger.error(f"Holodex API 통신 예외 발생 (offset={offset}): {e}")
            break

    logger.info(f"[{mode.upper()} 모드] 총 {len(streams)}건의 방송 데이터 수집 완료")
    return streams

def process_and_save_streams(streams):
    if not streams:
        return

    # 1. 미등록 채널 선별 및 기본 채널 추가 (Foreign Key 위반 방지)
    channel_map = {}
    stream_map = {}

    for s in streams:
        channel_info = s.get('channel', {})
        ch_id = channel_info.get('id')
        stream_id = s.get('id')

        if not ch_id or not stream_id:
            continue

        # 채널 정보 중복 제거 (dict 기반)
        if ch_id not in channel_map:
            channel_map[ch_id] = (
                ch_id,
                channel_info.get('name'),
                channel_info.get('english_name'),
                channel_info.get('org'),
                channel_info.get('photo'),
                channel_info.get('twitter'),
                True
            )

        # 스트림 정보 중복 제거 (dict 기반)
        if stream_id not in stream_map:
            start_scheduled = s.get('start_scheduled')
            start_actual = s.get('start_actual')
            end_actual = s.get('end_actual')
            current_viewers = int(s.get('live_viewers') or 0)
            topic_id = s.get('topic_id')

            stream_map[stream_id] = (
                stream_id,
                ch_id,
                s.get('title'),
                s.get('status'),
                topic_id,
                start_scheduled,
                start_actual,
                end_actual,
                current_viewers
            )

    channel_tuples = list(channel_map.values())
    stream_tuples = list(stream_map.values())

    channel_upsert_sql = """
    INSERT INTO oshilive.channels (channel_id, name, english_name, org, profile_img_url, twitter_id, is_active)
    VALUES %s
    ON CONFLICT (channel_id) DO UPDATE SET
        name = EXCLUDED.name,
        english_name = COALESCE(EXCLUDED.english_name, oshilive.channels.english_name),
        org = COALESCE(EXCLUDED.org, oshilive.channels.org),
        profile_img_url = COALESCE(EXCLUDED.profile_img_url, oshilive.channels.profile_img_url),
        updated_at = CURRENT_TIMESTAMP;
    """

    stream_upsert_sql = """
    INSERT INTO oshilive.streams (
        stream_id, channel_id, title, status, topic_id,
        start_scheduled, start_actual, end_actual, current_viewers
    ) VALUES %s
    ON CONFLICT (stream_id) DO UPDATE SET
        title = EXCLUDED.title,
        status = EXCLUDED.status,
        topic_id = COALESCE(EXCLUDED.topic_id, oshilive.streams.topic_id),
        start_scheduled = COALESCE(EXCLUDED.start_scheduled, oshilive.streams.start_scheduled),
        start_actual = COALESCE(EXCLUDED.start_actual, oshilive.streams.start_actual),
        end_actual = COALESCE(EXCLUDED.end_actual, oshilive.streams.end_actual),
        current_viewers = EXCLUDED.current_viewers,
        updated_at = CURRENT_TIMESTAMP;
    """

    try:
        with get_db_cursor() as cur:
            # 채널 배치 등록
            if channel_tuples:
                execute_values(cur, channel_upsert_sql, channel_tuples, page_size=200)
            # 스트림 배치 등록
            if stream_tuples:
                execute_values(cur, stream_upsert_sql, stream_tuples, page_size=200)
                
        logger.info(f"✅ 방송 데이터 총 {len(stream_tuples)}건 DB 동기화 완료!")
    except Exception as e:
        logger.error(f"❌ 방송 데이터 DB 저장 실패: {e}")
        raise

def run_live_sync(mode: str):
    logger.info(f"=== 실시간 방송 동기화 배치 시작 ({mode.upper()} 모드) ===")
    try:
        streams = fetch_live_streams(mode)
        process_and_save_streams(streams)
    finally:
        if 'streams' in locals():
            del streams
        gc.collect()
        logger.info(f"=== 실시간 방송 동기화 배치 종료 ({mode.upper()} 모드) ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OshiLive Live Stream Sync Batch")
    parser.add_argument("--mode", choices=["short", "long"], default="short", help="Sync mode: short (daily) or long (30 days)")
    args = parser.parse_args()
    
    run_live_sync(args.mode)
