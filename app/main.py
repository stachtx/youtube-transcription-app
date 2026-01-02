import os
import time
import hashlib
from typing import List, Optional, Dict, Any, Tuple

from fastapi import FastAPI, HTTPException, Query, Header
from psycopg_pool import ConnectionPool
from youtube_transcript_api import YouTubeTranscriptApi


# =========================
# ENV
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL env var")

# Opcjonalny klucz do zabezpieczenia endpointów
API_KEY = os.getenv("API_KEY")  # jeśli ustawisz, wymagamy nagłówka X-API-Key

# Throttle (żeby rzadziej łapać blokady YT)
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
# DB init
# =========================
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS transcript_cache (
  video_id TEXT NOT NULL,
  languages_key TEXT NOT NULL,      -- np. "pl,en"
  format TEXT NOT NULL,             -- "text"
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
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLES_SQL)


# =========================
# Helpers
# =========================
def require_api_key(x_api_key: Optional[str]) -> None:
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


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


def yt_throttle() -> None:
    """Minimalny odstęp między realnymi requestami do YouTube (pomaga na blokady)."""
    global _last_yt_call_ts
    now = time.time()
    wait = (_last_yt_call_ts + YT_MIN_INTERVAL_SECONDS) - now
    if wait > 0:
        time.sleep(wait)
    _last_yt_call_ts = time.time()


def db_get_transcript(video_id: str, languages_key: str, fmt: str) -> Optional[Dict[str, Any]]:
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
            if not row:
                return None
            return {"content": row[0], "hash": row[1], "fetched_at": row[2]}


def db_upsert_transcript(video_id: str, languages_key: str, fmt: str, content: str, h: str) -> None:
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


def fetch_transcript_from_youtube(video_id: str, languages: List[str]) -> str:
    yt_throttle()
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=languages)
    except Exception as e:
        # najczęściej: rate/anti-bot / brak napisów / ograniczenia
        raise HTTPException(status_code=429, detail=f"YouTube transcript blocked/rate-limited: {e}")

    text = "\n".join([seg.get("text", "") for seg in transcript]).strip()
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
    video: str,
    languages: List[str] = Query(default=["pl", "en"]),
    format: str = "text",
    force: bool = False,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> Dict[str, Any]:
    """
    Cache transkryptów w Postgres (Railway).
    - Jeśli jest w DB -> cached=true
    - Jeśli brak -> pobiera z YT, zapisuje, cached=false
    """
    require_api_key(x_api_key)

    langs, langs_key = normalize_languages(languages)
    fmt = (format or "").strip().lower()

    if fmt != "text":
        raise HTTPException(status_code=400, detail="Only format=text is supported for now")

    if not force:
        cached = db_get_transcript(video, langs_key, fmt)
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
            }

    # cache miss / force
    content = fetch_transcript_from_youtube(video, langs)
    h = sha256_text(content)
    db_upsert_transcript(video, langs_key, fmt, content, h)

    return {
        "videoId": video,
        "languages": langs,
        "languages_key": langs_key,
        "format": fmt,
        "cached": False,
        "transcript_hash": h,
        "content": content,
    }


@app.post("/cleanup")
def cleanup(
    keep_days: int = 60,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> Dict[str, Any]:
    """Opcjonalnie: czyści stare transkrypty."""
    require_api_key(x_api_key)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM transcript_cache WHERE fetched_at < now() - (%s || ' days')::interval",
                (keep_days,),
            )
            deleted = cur.rowcount

    return {"status": "ok", "deleted": deleted}