from __future__ import annotations

import os
import re
from typing import List, Optional, Literal

from fastapi import FastAPI, Header, HTTPException, Query
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api import (
    VideoUnavailable,
    VideoUnplayable,
    TranscriptsDisabled,
    NoTranscriptFound,
    RequestBlocked,
    IpBlocked,
    InvalidVideoId,
    AgeRestricted,
    PoTokenRequired,
    YouTubeRequestFailed,
    YouTubeDataUnparsable,
    FailedToCreateConsentCookie,
)
from youtube_transcript_api.formatters import FormatterLoader

app = FastAPI(title="YouTube Transcript Microservice", version="1.0.0")

API_KEY = os.getenv("TRANSCRIPT_API_KEY", "")  # jeśli puste -> brak auth
ytt = YouTubeTranscriptApi()
fmt_loader = FormatterLoader()

VIDEO_ID_RE = re.compile(r"(?:v=|/shorts/|youtu\.be/)([A-Za-z0-9_-]{11})")


def require_key(x_api_key: Optional[str]):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid X-API-Key")


def extract_video_id(video_id_or_url: str) -> str:
    if len(video_id_or_url) == 11 and re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id_or_url):
        return video_id_or_url
    m = VIDEO_ID_RE.search(video_id_or_url)
    if not m:
        raise HTTPException(status_code=400, detail="Invalid video id / url")
    return m.group(1)


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/available")
def available(
    video: str = Query(..., description="videoId (11) albo URL"),
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False, alias="X-API-Key"),
):
    require_key(x_api_key)
    video_id = extract_video_id(video)

    try:
        transcript_list = ytt.list(video_id)
        transcripts = []
        for t in transcript_list:
            transcripts.append(
                {
                    "language": t.language,
                    "language_code": t.language_code,
                    "is_generated": t.is_generated,
                    "is_translatable": t.is_translatable,
                }
            )
        return {"video_id": video_id, "transcripts": transcripts}

    except (RequestBlocked, IpBlocked):
        raise HTTPException(429, "Request blocked by YouTube (rate/anti-bot)")
    except (VideoUnavailable, TranscriptsDisabled):
        raise HTTPException(404, "No transcripts / video unavailable")
    except Exception as e:
        raise HTTPException(500, f"Unexpected error: {type(e).__name__}")


@app.get("/transcript")
def transcript(
    video: str = Query(..., description="videoId (11) albo URL"),
    languages: List[str] = Query(default=["pl", "en"], description="kolejność priorytetów"),
    format: Literal["text", "json", "srt", "webvtt", "pretty", "raw"] = Query(default="text"),
    translate_to: Optional[str] = Query(default=None, description="np. 'pl' / 'en' / 'fr'"),
    preserve_formatting: bool = Query(default=False, description="zachowaj tagi formatowania"),
    x_api_key: Optional[str] = Header(default=None, convert_underscores=False, alias="X-API-Key"),
):
    require_key(x_api_key)
    video_id = extract_video_id(video)

    try:
        # Najpierw wybieramy transcript (manual preferowany), potem fetch
        transcript_list = ytt.list(video_id)
        tr = transcript_list.find_transcript(languages)

        if translate_to:
            tr = tr.translate(translate_to)

        fetched = tr.fetch(preserve_formatting=preserve_formatting)

        if format == "raw":
            return {
                "video_id": fetched.video_id,
                "language": fetched.language,
                "language_code": fetched.language_code,
                "is_generated": fetched.is_generated,
                "snippets": fetched.to_raw_data(),
            }

        formatter = fmt_loader.load(format)
        payload = formatter.format_transcript(fetched)

        return {
            "video_id": fetched.video_id,
            "language": fetched.language,
            "language_code": fetched.language_code,
            "is_generated": fetched.is_generated,
            "format": format,
            "content": payload,
        }

    except InvalidVideoId:
        raise HTTPException(400, "Invalid video id")
    except NoTranscriptFound:
        raise HTTPException(404, "No transcript found for requested languages")
    except (TranscriptsDisabled, VideoUnavailable):
        raise HTTPException(404, "Transcripts disabled or video unavailable")
    except (AgeRestricted, PoTokenRequired):
        raise HTTPException(403, "Video requires authentication / special token")
    except (FailedToCreateConsentCookie,):
        # w EU czasem wyskakuje temat consent cookie
        raise HTTPException(502, "Consent cookie issue (try later / different network)")
    except (RequestBlocked, IpBlocked):
        raise HTTPException(429, "Request blocked by YouTube (rate/anti-bot)")
    except (VideoUnplayable, YouTubeRequestFailed, YouTubeDataUnparsable):
        raise HTTPException(502, "YouTube response error (blocked/unparsable/unplayable)")
    except Exception as e:
        raise HTTPException(500, f"Unexpected error: {type(e).__name__}")
