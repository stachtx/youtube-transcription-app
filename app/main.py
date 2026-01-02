import os
import time
import hashlib
import logging
import uuid
from typing import List, Optional, Dict, Any, Tuple

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from psycopg_pool import ConnectionPool
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.proxies import WebshareProxyConfig

# =========================
# Logging
# =========================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("yt-transcript-cache")

# =========================
# ENV
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL env var")

YT_MIN_INTERVAL_SECONDS = float(os.getenv("YT_MIN_INTERVAL_SECONDS", "25"))

app = FastAPI(title="YouTube Transcript Cache")

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=5,
    kwargs={"autocommit": True},
)

_last_yt_call_ts = 0.0

# =========================
# Middleware: Request ID + timing
# =========================
@app.middleware("http")
async def add_request_id_and_log(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    start = time.time()

    try:
        response = await call_next(request)
    except Exception:
        logger.exception(
            "request_failed path=%s method=%s request_id=%s",
            request.url.path,
            request.method,
            request_id,
        )
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal Server Error", "request_id": request_id},
        )

    elapsed_ms = int((time.time() - start) * 1000)
    response.headers["X-Request-ID"] = request_id

    logger.info(
        "request_done path=%s method=%s status=%s ms=%s request_id=%s",
        request.url.path,
        request.method,
        response.status_code,
        elapsed_ms,
        request_id,
    )
    return response

# =========================
# DB init
# =========================
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS transcript_cache (
  video_id TEXT NOT NULL,
  languages_key TEXT NOT NULL,
  format TEXT NOT NULL,
  content TEXT NOT NULL,
  transcript_hash TEXT NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (video_id, languages_key, format)
);

CREATE INDEX IF NOT EXISTS idx_transcript_fetched_at
ON transcript_cache(fetched_at);
"""


@app.on_event("startup")
def startup() -> None:
    logger.info("startup begin")
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLES_SQL)
    logger.info("startup done (db initialized)")


# =========================
# Helpers
# =========================
def normalize_languages(languages: List[str]) -> Tuple[List[str], str]:
    cleaned: List[str] = []
    seen = set()
    for l in languages or []:
        l = (l or "").strip()
        if not l:
            continue
        if l not in seen:
            seen.add(l)
            cleaned.append(l)

    if not cleaned:
        cleaned = ["pl", "en"]

    key = ",".join(cleaned)
    return cleaned, key


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def yt_throttle(request_id: str) -> None:
    """Minimalny odstęp między realnymi requestami do YouTube (pomaga na blokady)."""
    global _last_yt_call_ts
    now = time.time()
    wait = (_last_yt_call_ts + YT_MIN_INTERVAL_SECONDS) - now
    if wait > 0:
        logger.info("yt_throttle wait_seconds=%.2f request_id=%s", wait, request_id)
        time.sleep(wait)
    _last_yt_call_ts = time.time()


def db_get_transcript(video_id: str, languages_key: str, fmt: str, request_id: str) -> Optional[Dict[str, Any]]:
    t0 = time.time()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT content, transcript_hash, fetched_at
                FROM transcript_cache
                WHERE video_id=%s AND languages_key=%s AND format=%s
                """,
                (video_id, languages_key, fmt),
            )
            row = cur.fetchone()

    ms = int((time.time() - t0) * 1000)
    logger.info(
        "db_get done hit=%s ms=%s video_id=%s languages_key=%s format=%s request_id=%s",
        bool(row),
        ms,
        video_id,
        languages_key,
        fmt,
        request_id,
    )

    if not row:
        return None
    return {"content": row[0], "hash": row[1], "fetched_at": row[2]}


def db_upsert_transcript(video_id: str, languages_key: str, fmt: str, content: str, h: str, request_id: str) -> None:
    t0 = time.time()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO transcript_cache(video_id, languages_key, format, content, transcript_hash)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (video_id, languages_key, format)
                DO UPDATE SET content=EXCLUDED.content, transcript_hash=EXCLUDED.transcript_hash, fetched_at=now()
                """,
                (video_id, languages_key, fmt, content, h),
            )
    ms = int((time.time() - t0) * 1000)
    logger.info(
        "db_upsert done ms=%s video_id=%s languages_key=%s format=%s len=%s request_id=%s",
        ms,
        video_id,
        languages_key,
        fmt,
        len(content),
        request_id,
    )


def fetch_transcript_from_youtube(video_id: str, languages: List[str], request_id: str) -> str:
    """
    youtube-transcript-api >=1.x:
    - tworzymy instancję i używamy .fetch(video_id, languages=[...])
    """
    yt_throttle(request_id)

    t0 = time.time()

    ytt_api = YouTubeTranscriptApi(
        proxy_config=WebshareProxyConfig(
            proxy_username=os.getenv("PROXY_USER"),
            proxy_password=os.getenv("PROXY_PASSWORD"),
        )
    )
    try:
        fetched = ytt_api.fetch(video_id, languages=languages)
        # fetched jest iterable po snippetach z .text
        text = "\n".join([snip.text for snip in fetched]).strip()
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        logger.exception(
            "yt_fetch failed ms=%s video_id=%s languages=%s request_id=%s err=%s",
            ms,
            video_id,
            languages,
            request_id,
            repr(e),
        )
        # 429 bo w praktyce to często blokady/rate-limit, ale może być też brak napisów
        raise HTTPException(status_code=429, detail=f"YouTube transcript blocked/rate-limited: {e}")

    ms = int((time.time() - t0) * 1000)
    logger.info(
        "yt_fetch done ms=%s video_id=%s languages=%s len=%s request_id=%s",
        ms,
        video_id,
        languages,
        len(text),
        request_id,
    )

    if not text:
        raise HTTPException(status_code=404, detail="Transcript is empty or not available")
    return text


# =========================
# Endpoints
# =========================
@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/transcript")
def transcript(
    request: Request,
    video: str,
    languages: List[str] = Query(default=["pl", "en"]),
    format: str = "text",
    force: bool = False,
) -> Dict[str, Any]:
    request_id = request.headers.get("X-Request-ID") or "no-request-id"
    langs, langs_key = normalize_languages(languages)
    fmt = (format or "").strip().lower()

    logger.info(
        "transcript_request video_id=%s languages=%s languages_key=%s format=%s force=%s request_id=%s",
        video,
        langs,
        langs_key,
        fmt,
        force,
        request_id,
    )

    if fmt != "text":
        raise HTTPException(status_code=400, detail="Only format=text is supported for now")

    if not force:
        cached = db_get_transcript(video, langs_key, fmt, request_id)
        if cached:
            return {
                "videoId": video,
                "languages": langs,
                "languages_key": langs_key,
                "format": fmt,
                "cached": True,
                "transcript_hash": cached["hash"],
                "fetched_at": str(cached["fetched_at"]),
                "content": cached["content"],
                "request_id": request_id,
            }

    # cache miss / force
    content = fetch_transcript_from_youtube(video, langs, request_id)
    h = sha256_text(content)
    db_upsert_transcript(video, langs_key, fmt, content, h, request_id)

    return {
        "videoId": video,
        "languages": langs,
        "languages_key": langs_key,
        "format": fmt,
        "cached": False,
        "transcript_hash": h,
        "content": content,
        "request_id": request_id,
    }


@app.post("/cleanup")
def cleanup(request: Request, keep_days: int = 60) -> Dict[str, Any]:
    request_id = request.headers.get("X-Request-ID") or "no-request-id"
    t0 = time.time()

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM transcript_cache WHERE fetched_at < now() - (%s || ' days')::interval",
                (keep_days,),
            )
            deleted = cur.rowcount

    ms = int((time.time() - t0) * 1000)
    logger.info("cleanup done deleted=%s keep_days=%s ms=%s request_id=%s", deleted, keep_days, ms, request_id)
    return {"status": "ok", "deleted": deleted, "request_id": request_id}
