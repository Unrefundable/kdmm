import json
import time

import xbmc
import xbmcaddon

try:
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError, URLError
except ImportError:
    from urllib2 import Request, urlopen, HTTPError, URLError


ADDON = xbmcaddon.Addon()
THEINTRODB_API_BASE = "https://api.theintrodb.org/v2"
INTRODB_API_BASE = "https://api.introdb.app"
MIN_REQUEST_GAP = 0.4
SEGMENT_TYPES = ("intro", "recap", "credits", "preview")

_last_request_time = 0.0
_rate_limit_until = 0.0


def _debug_logging():
    return ADDON.getSetting("segment_debug_logging") == "true"


def _log(message, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM IntroDB] {message}", level)


def _get_api_key():
    return (ADDON.getSetting("introdb_api_key") or "").strip()


def _wait_rate_limit():
    global _last_request_time
    now = time.time()
    if now < _rate_limit_until:
        _log(f"Rate-limited until {_rate_limit_until:.0f}")
        return False
    gap = now - _last_request_time
    if gap < MIN_REQUEST_GAP:
        time.sleep(MIN_REQUEST_GAP - gap)
    _last_request_time = time.time()
    return True


def _do_request(url, api_key):
    global _rate_limit_until
    req = Request(url)
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "KDMM IntroDB Client/1.0")
    if api_key:
        req.add_header("Authorization", f"Bearer {api_key}")

    try:
        resp = urlopen(req, timeout=8)
        body = resp.read().decode("utf-8")
        data = json.loads(body)
        if _debug_logging():
            _log(f"Response: {body[:500]}")
        return data
    except HTTPError as exc:
        if exc.code == 429:
            retry = 300
            for header in ("X-UsageLimit-Reset", "X-RateLimit-Reset", "Retry-After"):
                value = exc.headers.get(header)
                if value:
                    try:
                        retry = int(value)
                    except ValueError:
                        pass
                    break
            _rate_limit_until = time.time() + retry
            _log(f"HTTP 429 rate limited for {retry}s", xbmc.LOGWARNING)
        elif exc.code == 404:
            _log(f"404 for {url}")
        else:
            _log(f"HTTP {exc.code} for {url}", xbmc.LOGWARNING)
        return None
    except URLError as exc:
        _log(f"Network error: {exc.reason}", xbmc.LOGWARNING)
        return None
    except Exception as exc:
        _log(f"Request failed: {exc}", xbmc.LOGERROR)
        return None


def _normalize_imdb(imdb_id):
    if not imdb_id:
        return None
    value = str(imdb_id).strip()
    if not value.startswith("tt"):
        return None
    return value


def _valid_tmdb(tmdb_id):
    try:
        return int(str(tmdb_id)) > 0
    except (ValueError, TypeError):
        return False


def _episode_nums(season, episode):
    try:
        season_num = int(season)
        episode_num = int(episode)
        return season_num, episode_num
    except (TypeError, ValueError):
        return None, None


def _build_theintrodb_urls(tmdb_id, imdb_id, season, episode, is_movie):
    urls = []
    if tmdb_id and _valid_tmdb(tmdb_id):
        tid = str(tmdb_id).strip()
        if is_movie:
            urls.append((f"{THEINTRODB_API_BASE}/media?tmdb_id={tid}", "theintrodb:tmdb"))
        else:
            season_num, episode_num = _episode_nums(season, episode)
            if season_num and episode_num:
                urls.append((
                    f"{THEINTRODB_API_BASE}/media?tmdb_id={tid}&season={season_num}&episode={episode_num}",
                    "theintrodb:tmdb",
                ))

    imdb = _normalize_imdb(imdb_id)
    if imdb:
        if is_movie:
            urls.append((f"{THEINTRODB_API_BASE}/media?imdb_id={imdb}", "theintrodb:imdb"))
        else:
            season_num, episode_num = _episode_nums(season, episode)
            if season_num and episode_num:
                urls.append((
                    f"{THEINTRODB_API_BASE}/media?imdb_id={imdb}&season={season_num}&episode={episode_num}",
                    "theintrodb:imdb",
                ))
    return urls


def _build_introdb_url(imdb_id, season, episode, is_movie):
    if is_movie:
        return None
    imdb = _normalize_imdb(imdb_id)
    season_num, episode_num = _episode_nums(season, episode)
    if not imdb or not season_num or not episode_num:
        return None
    return f"{INTRODB_API_BASE}/segments?imdb_id={imdb}&season={season_num}&episode={episode_num}"


def _normalize_segment_payload(segment):
    if not isinstance(segment, dict):
        return []
    start_ms = segment.get("start_ms")
    end_ms = segment.get("end_ms")
    if start_ms is None and segment.get("start_sec") is not None:
        try:
            start_ms = int(float(segment.get("start_sec")) * 1000.0)
        except (TypeError, ValueError):
            start_ms = None
    if end_ms is None and segment.get("end_sec") is not None:
        try:
            end_ms = int(float(segment.get("end_sec")) * 1000.0)
        except (TypeError, ValueError):
            end_ms = None
    normalized = dict(segment)
    normalized["start_ms"] = start_ms
    normalized["end_ms"] = end_ms
    return [normalized]


def _merge_source_payload(merged, data, source):
    if not isinstance(data, dict):
        return

    if source == "introdb":
        merged["intro"].extend(_normalize_segment_payload(data.get("intro")))
        merged["recap"].extend(_normalize_segment_payload(data.get("recap")))
        merged["credits"].extend(_normalize_segment_payload(data.get("outro")))
        return

    for segment_type in SEGMENT_TYPES:
        merged[segment_type].extend(data.get(segment_type, []) or [])


def _pick_best_segments_all_types(segments, segment_type):
    if not segments:
        return []

    valid_segments = []
    for segment in segments:
        if not isinstance(segment, dict):
            continue

        start = segment.get("start_ms")
        end = segment.get("end_ms")

        if segment_type in ("intro", "recap"):
            if end is None:
                continue
            if start is None:
                start = 0
        elif segment_type in ("credits", "preview"):
            if start is None:
                continue

        if end is not None and end <= start:
            continue

        confidence = segment.get("confidence") if segment.get("confidence") is not None else 0.5
        count = segment.get("submission_count", 1)
        score = float(confidence) + count * 0.001
        valid_segments.append({
            "start_ms": start,
            "end_ms": end,
            "score": score,
        })

    valid_segments.sort(key=lambda item: item["score"], reverse=True)

    seen = set()
    results = []
    for segment in valid_segments:
        key = (segment["start_ms"], segment["end_ms"])
        if key in seen:
            continue
        seen.add(key)
        results.append({
            "start": None if segment["start_ms"] is None else segment["start_ms"] / 1000.0,
            "end": None if segment["end_ms"] is None else segment["end_ms"] / 1000.0,
            "score": segment["score"],
            "type": segment_type,
        })
    return results


def query_all_segments(tmdb_id=None, imdb_id=None, season=None, episode=None, is_movie=False):
    if ADDON.getSetting("segment_lookups_enabled") != "true":
        return {}

    urls = _build_theintrodb_urls(tmdb_id, imdb_id, season, episode, is_movie)
    introdb_url = _build_introdb_url(imdb_id, season, episode, is_movie)
    if introdb_url:
        urls.append((introdb_url, "introdb"))

    if not urls:
        _log("No TMDb/IMDb identifiers available for segment lookup")
        return {}
    if not _wait_rate_limit():
        return {}

    api_key = _get_api_key()
    merged = {segment_type: [] for segment_type in SEGMENT_TYPES}

    for url, mode in urls:
        _log(f"Query ({mode}): {url}")
        data = _do_request(url, api_key)
        if not data:
            continue
        _merge_source_payload(merged, data, mode)

    results = {}
    for segment_type in SEGMENT_TYPES:
        processed = _pick_best_segments_all_types(merged.get(segment_type), segment_type)
        if processed:
            results[segment_type] = processed
    if _debug_logging():
        _log(f"Resolved segment types: {list(results.keys())}")
    return results
