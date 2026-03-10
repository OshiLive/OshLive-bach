def update_streams():
    # [1] 메인 스트림 데이터 가져오기 (DB 연결 전)
    now_utc = datetime.now(timezone.utc)
    from_date = (now_utc - timedelta(hours=6)).strftime('%Y-%m-%dT%H:%M:%SZ')
    to_date = (now_utc + timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')

    url = "https://holodex.net/api/v2/live"
    params = {"type": "stream", "status": "live,upcoming", "from": from_date, "to": to_date, "org": "Hololive", "limit": 100}
    headers = {"X-APIKEY": API_KEY}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=40)
        resp.raise_for_status() 
        streams = resp.json()
    except Exception as e:
        logging.error(f"❌ 홀로덱스 메인 API 호출 실패 (서버 점검 가능성): {e}")
        return # 여기서 종료하면 DB 커넥션은 0개 유지됨

    if not streams:
        logging.info("ℹ️ 현재 수집된 활성 스트림이 없습니다.")
        return

    # [2] 필요한 채널 ID 정보 수집
    all_ids = {s['channel']['id'] for s in streams}
    for s in streams:
        if s.get('mentions'):
            for m in s['mentions']: all_ids.add(m['id'])

    # [3] DB에서 기존 채널 확인 (잠깐 열고 닫기)
    existing_ids = set()
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT channel_id FROM oshilive.channels WHERE channel_id = ANY(%s)", (list(all_ids),))
            existing_ids = {r[0] for r in cur.fetchall()}
    except Exception as e:
        logging.error(f"❌ 기존 채널 조회 실패: {e}")
        return
    finally:
        if conn: conn.close() # 1차 반납

    # [4] 신규 채널 상세 정보 가져오기 (DB 연결 없음 - 가장 안전한 구간)
    missing_ids = all_ids - existing_ids
    new_channels_data = []
    for c_id in missing_ids:
        c_data = fetch_channel_info(c_id) # 여기서 타임아웃이 나도 DB는 안전함
        if c_data:
            new_channels_data.append(c_data)

    # [5] 최종 데이터 UPSERT (진짜 DB 작업만 빠르게 수행)
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # 5-1. 신규 채널 등록
            if new_channels_data:
                channel_values = [(
                    c['id'], c['name'], c.get('english_name'), c.get('org'),
                    c.get('photo'), c.get('banner'), c.get('description'), 
                    c.get('twitter'), c.get('lang')
                ) for c in new_channels_data]
                
                channel_query = """
                    INSERT INTO oshilive.channels (
                        channel_id, name, english_name, org, profile_img_url, 
                        banner_img_url, description, twitter_id, lang, updated_at
                    ) VALUES %s ON CONFLICT (channel_id) DO UPDATE SET updated_at = CURRENT_TIMESTAMP;
                """
                execute_values(cur, channel_query, channel_values)
                logging.info(f"✨ 신규 채널 {len(new_channels_data)}개 등록 완료")

            # 5-2. 스트림 데이터 UPSERT
            stream_values = [(
                s['id'], s['channel']['id'], s['title'], s.get('topic_id'), s['status'],
                s.get('start_scheduled'), s.get('start_actual'), s.get('end_actual'),
                f"https://i.ytimg.com/vi/{s['id']}/maxresdefault.jpg", s.get('live_viewers', 0)
            ) for s in streams]

            upsert_query = """
                INSERT INTO oshilive.streams (
                    stream_id, channel_id, title, topic_id, status, 
                    start_scheduled, start_actual, end_actual, thumbnail_url, current_viewers
                ) VALUES %s ON CONFLICT (stream_id) DO UPDATE SET
                    title = EXCLUDED.title, status = EXCLUDED.status,
                    start_actual = COALESCE(oshilive.streams.start_actual, EXCLUDED.start_actual),
                    end_actual = EXCLUDED.end_actual, current_viewers = EXCLUDED.current_viewers,
                    updated_at = CURRENT_TIMESTAMP;
            """
            execute_values(cur, upsert_query, stream_values)

            # 5-3. 종료 상태 업데이트
            active_ids = [s['id'] for s in streams]
            cur.execute("""
                UPDATE oshilive.streams SET status = 'past', 
                end_actual = COALESCE(end_actual, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP
                WHERE status IN ('live', 'upcoming') AND NOT (stream_id = ANY(%s));
            """, (active_ids,))

            conn.commit()
            logging.info(f"🚀 배치 완료: {len(stream_values)}개 갱신")

    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"💥 최종 DB 저장 중 에러: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("🔒 DB 커넥션 최종 반납 완료")