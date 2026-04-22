"""
KDMM – lib/dmm.py
Debrid Media Manager torrent lookup + Real-Debrid stream resolver.

Flow:
  1.  Generate a DMM proof-of-work token (port of their JS generateTokenAndHash).
  2.  GET  debridmediamanager.com/api/torrents/movie (or tv)
      → returns every known torrent hash for the IMDB ID.
  3.  Sort candidates by preferred release groups + file size.
  4.  For each candidate, check RD cache via direct-add:
        POST /torrents/addMagnet → GET /torrents/info →
        POST /torrents/selectFiles → check status == 'downloaded' →
        POST /unrestrict/link → direct CDN URL.
      (RD's instantAvailability endpoint is permanently disabled.)
"""

import html
import math
import re
import sys
import os
import time as _time
import unicodedata

import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

_DMM_SALT = "debridmediamanager.com%%fe7#td00rA3vHz%VmI"
_DMM_BASE = "https://debridmediamanager.com"
_RD_BASE = "https://api.real-debrid.com/rest/1.0"

# Minimum file size to distinguish real content from RD error clips.
_MIN_STREAM_BYTES = 50 * 1024 * 1024  # 50 MB

# Sentinel returned by _try_resolve_one when RD rejects with 401 (bad token)
_RD_AUTH_FAILURE = object()


# ------------------------------------------------------------------ #
# Title parser — extract quality metadata from torrent names
# ------------------------------------------------------------------ #

# HDR tiers (lower = better)
_HDR_DV = 0       # Dolby Vision (may include DV + HDR10 combo)
_HDR_HDR10P = 1   # HDR10+
_HDR_HDR10 = 2    # HDR10
_HDR_HDR = 3      # Generic HDR
_HDR_SDR = 4      # No HDR info → SDR

# Resolution tiers
_RES_2160 = 0
_RES_1080 = 1
_RES_720 = 2
_RES_SD = 3

# Source tiers
_SRC_REMUX = 0
_SRC_BLURAY = 1   # BluRay encode (not remux)
_SRC_WEB = 2      # WEB-DL / WEBRip
_SRC_HDTV = 3
_SRC_OTHER = 4

_GENERIC_PRE_TITLE_TOKENS = {
    "the", "a", "an", "complete", "collection", "series", "season",
    "show", "tv", "all",
}

_RELEASE_CONTEXT_TOKENS = {
    "complete", "season", "series", "episode", "ep", "pack", "multi",
    "proper", "repack", "remastered", "remaster", "extended", "uncut",
    "imax", "criterion", "hdr", "hdr10", "dv", "dovi", "sdr", "uhd",
    "4k", "2160p", "1080p", "720p", "480p", "web", "webrip", "webdl",
    "bluray", "bdrip", "remux", "hdtv", "nf", "amzn", "atvp", "dsnp",
    "hmax", "dd", "ddp", "aac", "ac3", "hevc", "x264", "x265", "h264",
    "h265", "av1",
}


def _parse_title(title):
    """
    Parse a torrent title and return a dict of quality attributes.
    All matching is case-insensitive against the raw title.
    """
    t = title.lower()

    # --- HDR format ---
    if "dovi" in t or "dolby.vision" in t or "dolbyvision" in t or \
       re.search(r'\bdo?v\b', t) or "dolby vision" in t:
        hdr = _HDR_DV
    elif "hdr10+" in t or "hdr10plus" in t or "hdr10 plus" in t:
        hdr = _HDR_HDR10P
    elif "hdr10" in t:
        hdr = _HDR_HDR10
    elif re.search(r'\bhdr\b', t):
        hdr = _HDR_HDR
    else:
        hdr = _HDR_SDR

    # --- Resolution ---
    if "2160p" in t or "4k" in t or "uhd" in t:
        res = _RES_2160
    elif "1080p" in t or "1080i" in t:
        res = _RES_1080
    elif "720p" in t:
        res = _RES_720
    else:
        res = _RES_SD

    # --- Source ---
    if "remux" in t:
        src = _SRC_REMUX
    elif re.search(r'\bblu[\-\.]?ray\b', t) or "bdremux" in t or "bd full" in t \
            or re.search(r'complete.*bluray', t) or ".iso" in t:
        src = _SRC_BLURAY
    elif re.search(r'web[\-\.]?dl', t) or re.search(r'webrip', t) or re.search(r'\bweb\b', t):
        src = _SRC_WEB
    elif "hdtv" in t:
        src = _SRC_HDTV
    else:
        src = _SRC_OTHER

    # --- Release group (last segment after hyphen) ---
    group_match = re.search(r'-([A-Za-z0-9]+)(?:\.[a-z]{2,4})?$', title)
    group = group_match.group(1).lower() if group_match else ""

    return {
        "hdr": hdr,
        "res": res,
        "src": src,
        "group": group,
    }


def _normalize_text(text):
    text = html.unescape(text or "")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower()


def _tokenize_text(text):
    return re.findall(r"[a-z0-9]+", _normalize_text(text))


def _find_token_sequence(haystack, needle):
    if not haystack or not needle or len(needle) > len(haystack):
        return []
    matches = []
    width = len(needle)
    for idx in range(0, len(haystack) - width + 1):
        if haystack[idx:idx + width] == needle:
            matches.append(idx)
    return matches


def _is_year_token(token):
    return len(token) == 4 and token.isdigit() and token.startswith(("19", "20"))


def _looks_like_release_token(token):
    if not token:
        return True
    if token in _RELEASE_CONTEXT_TOKENS:
        return True
    if _is_year_token(token) or token.isdigit():
        return True
    if re.match(r"^s\d{1,2}(?:e\d{1,3})?$", token):
        return True
    if re.match(r"^\d{1,2}x\d{1,3}$", token):
        return True
    if re.match(r"^\d{3,4}p$", token):
        return True
    return False


def _title_sequence_rank(title, expected_title):
    """
    Return a rank tuple for how well the torrent title matches the requested
    show title, or None when the match is too weak to trust.

    One-word titles like "FROM" are treated specially: the token must look
    like the actual release title, not just appear inside another title such
    as "From Scratch" or "Wind Blows From Longxi".
    """
    query_tokens = _tokenize_text(expected_title)
    if not query_tokens:
        return (1, 99)

    title_tokens = _tokenize_text(title)
    positions = _find_token_sequence(title_tokens, query_tokens)
    if not positions:
        return None

    best_rank = None
    single_token_title = len(query_tokens) == 1
    for start in positions:
        end = start + len(query_tokens)
        prev_token = title_tokens[start - 1] if start > 0 else ""
        next_token = title_tokens[end] if end < len(title_tokens) else ""

        if single_token_title:
            prev_ok = start == 0 or prev_token in _GENERIC_PRE_TITLE_TOKENS or _is_year_token(prev_token)
            next_ok = _looks_like_release_token(next_token)
            if not (prev_ok and next_ok):
                continue
            boundary_rank = 0 if start == 0 else 1
        else:
            if start == 0:
                boundary_rank = 0
            elif prev_token in _GENERIC_PRE_TITLE_TOKENS or _is_year_token(prev_token):
                boundary_rank = 1
            else:
                boundary_rank = 2

        rank = (boundary_rank, min(start, 99))
        if best_rank is None or rank < best_rank:
            best_rank = rank

    return best_rank


def _season_match_rank(title, season):
    """0 = requested season present, 1 = no explicit season, 2 = conflicting season."""
    if not season:
        return 1

    text = _normalize_text(title)
    season_num = int(season)
    season_hits = set()

    for value in re.findall(r"\bs0*(\d{1,2})(?:e\d{1,3})?\b", text):
        season_hits.add(int(value))
    for value, _ in re.findall(r"\b(\d{1,2})x(\d{1,3})\b", text):
        season_hits.add(int(value))
    for value in re.findall(r"\bseason[ ._-]*0*(\d{1,2})\b", text):
        season_hits.add(int(value))
    for value in re.findall(r"\bseries[ ._-]*0*(\d{1,2})\b", text):
        season_hits.add(int(value))

    if season_num in season_hits:
        return 0
    if season_hits:
        return 2
    return 1


def _episode_match_rank(title, season, episode):
    """0 = requested episode present, 1 = no explicit episode, 2 = conflicting episode."""
    if not season or not episode:
        return 1

    text = _normalize_text(title)
    season_num = int(season)
    episode_num = int(episode)
    exact_patterns = [
        rf"\bs0*{season_num}[ ._-]*e0*{episode_num}\b",
        rf"\b{season_num}x0*{episode_num}\b",
        rf"\bepisode[ ._-]*0*{episode_num}\b",
        rf"\bep[ ._-]*0*{episode_num}\b",
        rf"\be0*{episode_num}\b",
    ]
    if any(re.search(pattern, text) for pattern in exact_patterns):
        return 0

    season_episode_hits = set()
    for value_season, value_episode in re.findall(r"\bs0*(\d{1,2})[ ._-]*e0*(\d{1,3})\b", text):
        if int(value_season) == season_num:
            season_episode_hits.add(int(value_episode))
    for value_season, value_episode in re.findall(r"\b(\d{1,2})x(\d{1,3})\b", text):
        if int(value_season) == season_num:
            season_episode_hits.add(int(value_episode))

    if season_episode_hits and episode_num not in season_episode_hits:
        return 2
    return 1


def _filter_tv_results(results, show_title, season, episode):
    """
    Progressively tighten noisy DMM TV results.

    Stage 1 keeps only titles that plausibly match the requested show title.
    Stage 2 rejects explicit conflicting seasons.
    Stage 3 rejects explicit conflicting episodes.

    Each stage falls back to the previous list if it would remove everything,
    which keeps unusual foreign/localized naming schemes playable.
    """
    filtered = list(results)

    if show_title:
        title_matches = [
            result for result in filtered
            if _title_sequence_rank(result.get("title", ""), show_title) is not None
        ]
        if title_matches:
            _log(f"TV title filter: {len(filtered)} -> {len(title_matches)} results matching {show_title!r}")
            filtered = title_matches
        else:
            _log(f"TV title filter removed all {len(filtered)} results for {show_title!r} - keeping broader set",
                 xbmc.LOGWARNING)

    if season:
        season_matches = [
            result for result in filtered
            if _season_match_rank(result.get("title", ""), season) < 2
        ]
        if season_matches:
            _log(f"Season filter: {len(filtered)} -> {len(season_matches)} results for season {int(season)}")
            filtered = season_matches
        else:
            _log(f"Season filter removed all {len(filtered)} results for season {int(season)} - keeping broader set",
                 xbmc.LOGWARNING)

    if season and episode:
        episode_matches = [
            result for result in filtered
            if _episode_match_rank(result.get("title", ""), season, episode) < 2
        ]
        if episode_matches:
            _log(f"Episode filter: {len(filtered)} -> {len(episode_matches)} results for E{int(episode):02d}")
            filtered = episode_matches
        else:
            _log(f"Episode filter removed all {len(filtered)} results for E{int(episode):02d} - keeping broader set",
                 xbmc.LOGWARNING)

    return filtered


def _build_tv_sort_key(quality_sort_key, show_title, season, episode):
    def _sort_key(entry):
        title = entry.get("title") or ""
        title_rank = _title_sequence_rank(title, show_title) or (9, 99)
        season_rank = _season_match_rank(title, season)
        episode_rank = _episode_match_rank(title, season, episode)
        return title_rank + (episode_rank, season_rank) + quality_sort_key(entry)

    return _sort_key


def _get_quality_preferences():
    """Read quality preferences from addon settings."""
    addon = xbmcaddon.Addon()

    # Preferred groups
    groups_raw = addon.getSetting("preferred_groups") or "FraMeSToR,Cinephiles,TRITON"
    preferred_groups = [g.strip().lower() for g in groups_raw.split(",") if g.strip()]

    # HDR preference (0=DV, 1=HDR10+, 2=HDR10, 3=Any HDR, 4=SDR only)
    hdr_pref = int(addon.getSetting("hdr_priority") or "0")

    # Resolution preference (0=4K, 1=1080p, 2=720p)
    res_pref = int(addon.getSetting("resolution_priority") or "0")

    # Source preference (0=Remux, 1=BluRay, 2=WEB, 3=Any)
    src_pref = int(addon.getSetting("source_priority") or "0")

    return preferred_groups, hdr_pref, res_pref, src_pref


def _build_sort_key(preferred_groups, hdr_pref, res_pref, src_pref):
    """
    Return a sort-key function for DMM results that respects user prefs.

    Sort priority (lower = better):
      1. Preferred release group (0 = match, 1 = no match)
      2. HDR tier (mapped so user's preferred HDR is tier 0)
      3. Resolution tier (mapped so user's preferred res is tier 0)
      4. Source tier (mapped so user's preferred source is tier 0)
      5. File size descending (larger = better quality)

    This ensures preferred group always wins. Within same group,
    the best HDR → resolution → source → size is picked.
    """
    # Build HDR remap: user's preference gets score 0, others ranked after
    hdr_order = {
        0: [_HDR_DV, _HDR_HDR10P, _HDR_HDR10, _HDR_HDR, _HDR_SDR],      # DV first
        1: [_HDR_HDR10P, _HDR_DV, _HDR_HDR10, _HDR_HDR, _HDR_SDR],      # HDR10+ first
        2: [_HDR_HDR10, _HDR_HDR10P, _HDR_DV, _HDR_HDR, _HDR_SDR],      # HDR10 first
        3: [_HDR_DV, _HDR_HDR10P, _HDR_HDR10, _HDR_HDR, _HDR_SDR],      # Any HDR (DV > 10+ > 10)
        4: [_HDR_SDR, _HDR_DV, _HDR_HDR10P, _HDR_HDR10, _HDR_HDR],      # SDR only
    }.get(hdr_pref, [_HDR_DV, _HDR_HDR10P, _HDR_HDR10, _HDR_HDR, _HDR_SDR])
    hdr_rank = {v: i for i, v in enumerate(hdr_order)}

    # Resolution remap
    res_order = {
        0: [_RES_2160, _RES_1080, _RES_720, _RES_SD],   # 4K first
        1: [_RES_1080, _RES_2160, _RES_720, _RES_SD],   # 1080p first
        2: [_RES_720, _RES_1080, _RES_2160, _RES_SD],   # 720p first
    }.get(res_pref, [_RES_2160, _RES_1080, _RES_720, _RES_SD])
    res_rank = {v: i for i, v in enumerate(res_order)}

    # Source remap
    src_order = {
        0: [_SRC_REMUX, _SRC_BLURAY, _SRC_WEB, _SRC_HDTV, _SRC_OTHER],
        1: [_SRC_BLURAY, _SRC_REMUX, _SRC_WEB, _SRC_HDTV, _SRC_OTHER],
        2: [_SRC_WEB, _SRC_REMUX, _SRC_BLURAY, _SRC_HDTV, _SRC_OTHER],
        3: [_SRC_REMUX, _SRC_BLURAY, _SRC_WEB, _SRC_HDTV, _SRC_OTHER],  # Any = default order
    }.get(src_pref, [_SRC_REMUX, _SRC_BLURAY, _SRC_WEB, _SRC_HDTV, _SRC_OTHER])
    src_rank = {v: i for i, v in enumerate(src_order)}

    def _sort_key(entry):
        title = entry.get("title") or ""
        parsed = _parse_title(title)
        size = entry.get("fileSize") or entry.get("filesize") or 0

        # Is this a preferred group?
        group_prio = 1
        for g in preferred_groups:
            if g in parsed["group"] or g in title.lower():
                group_prio = 0
                break

        return (
            group_prio,
            hdr_rank.get(parsed["hdr"], 99),
            res_rank.get(parsed["res"], 99),
            src_rank.get(parsed["src"], 99),
            -size,  # larger = better
        )

    return _sort_key


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM] {msg}", level)


def _get_requests():
    """Import requests with all Kodi addon module paths on sys.path."""
    addon_dir = xbmcvfs.translatePath("special://home/addons")
    for mod in ("script.module.requests", "script.module.urllib3",
                "script.module.chardet", "script.module.certifi",
                "script.module.idna"):
        lib = os.path.join(addon_dir, mod, "lib")
        if os.path.isdir(lib) and lib not in sys.path:
            sys.path.insert(0, lib)
    import requests
    try:
        import certifi
        os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
    except Exception:
        pass
    return requests


# Module-level session — reuses TCP/SSL connections across all RD calls.
_rd_session = None


def _get_session():
    """Return a shared requests.Session (created once, reused across calls)."""
    global _rd_session
    if _rd_session is None:
        requests = _get_requests()
        _rd_session = requests.Session()
    return _rd_session


# ------------------------------------------------------------------ #
# DMM token generation  (port of src/utils/token.ts)
# ------------------------------------------------------------------ #

def _dmm_hash(s):
    """Port of DMM's custom 32-bit hash function."""
    h1 = 0xDEADBEEF ^ len(s)
    h2 = 0x41C6CE57 ^ len(s)
    for ch in s:
        c = ord(ch)
        h1 = _imul(h1 ^ c, 0x9E3779B1) & 0xFFFFFFFF
        h2 = _imul(h2 ^ c, 0x5F356495) & 0xFFFFFFFF
        h1 = ((h1 << 5) | (h1 >> 27)) & 0xFFFFFFFF
        h2 = ((h2 << 5) | (h2 >> 27)) & 0xFFFFFFFF

    h1 = (h1 + _imul(h2, 0x5D588B65)) & 0xFFFFFFFF
    h2 = (h2 + _imul(h1, 0x78A76A79)) & 0xFFFFFFFF
    return format((h1 ^ h2) & 0xFFFFFFFF, "x")


def _imul(a, b):
    """Emulate JavaScript Math.imul (signed 32-bit multiply)."""
    a &= 0xFFFFFFFF
    b &= 0xFFFFFFFF
    result = (a * b) & 0xFFFFFFFF
    if result >= 0x80000000:
        result -= 0x100000000
    # we need unsigned for bit shifts later
    return result & 0xFFFFFFFF


def _combine_hashes(h1, h2):
    """Port of DMM's combineHashes (interleave + reverse)."""
    half = len(h1) // 2
    fp1, sp1 = h1[:half], h1[half:]
    fp2, sp2 = h2[:half], h2[half:]

    obfuscated = ""
    for i in range(half):
        obfuscated += fp1[i] + fp2[i]
    obfuscated += sp2[::-1] + sp1[::-1]
    return obfuscated


def _generate_token_and_hash(api_token=None):
    """
    Generate a (tokenWithTimestamp, combinedHash) pair accepted by DMM's API.
    Uses the local system clock for the timestamp — a Mac is NTP-synced so
    it matches DMM's server clock.  No RD API call needed here.
    """
    import random
    token = format(random.getrandbits(32), "x")
    timestamp = int(_time.time())
    token_with_ts = f"{token}-{timestamp}"
    ts_hash = _dmm_hash(token_with_ts)
    salt_hash = _dmm_hash(f"{_DMM_SALT}-{token}")
    return token_with_ts, _combine_hashes(ts_hash, salt_hash)


# ------------------------------------------------------------------ #
# Real-Debrid helpers
# ------------------------------------------------------------------ #

def _rd_key():
    """Return a valid RD access token (OAuth), refreshing if needed."""
    from rd_auth import get_access_token
    return get_access_token()


def _validate_rd_token(api_token):
    """
    Verify the token is accepted by RD by calling GET /user.
    Returns True if valid, False if 401/403, None on network error.
    """
    requests = _get_session()
    try:
        resp = requests.get(f"{_RD_BASE}/user", headers=_rd_headers(api_token), timeout=10)
        if resp.status_code in (401, 403):
            _log(f"Token validation failed: HTTP {resp.status_code} – {resp.text[:80]}",
                 xbmc.LOGWARNING)
            return False
        return resp.status_code == 200
    except Exception as exc:
        _log(f"Token validation network error: {exc}", xbmc.LOGWARNING)
        return None  # can't confirm either way


def _rd_headers(api_token):
    return {"Authorization": f"Bearer {api_token}"}


def _rd_get(path, api_token, timeout=6):
    s = _get_session()
    r = s.get(f"{_RD_BASE}{path}", headers=_rd_headers(api_token), timeout=timeout)
    r.raise_for_status()
    text = r.text.strip()
    if not text:
        return {}
    return r.json()


def _rd_post(path, api_token, data=None, timeout=6):
    s = _get_session()
    r = s.post(f"{_RD_BASE}{path}", headers=_rd_headers(api_token),
               data=data or {}, timeout=timeout)
    r.raise_for_status()
    text = r.text.strip()
    if not text:
        return {}
    return r.json()


def _rd_delete(path, api_token, timeout=5):
    s = _get_session()
    try:
        s.delete(f"{_RD_BASE}{path}", headers=_rd_headers(api_token), timeout=timeout)
    except Exception:
        pass  # delete is best-effort cleanup


# ------------------------------------------------------------------ #
# DMM hash database query
# ------------------------------------------------------------------ #

def _fetch_dmm_hashes(imdb_id, media_type="movie", max_size=0, page=0, api_token=None, season=None):
    """
    Query DMM's torrent database for all known hashes for an IMDB ID.
    Returns list of dicts: [{hash, title, fileSize, files, ...}, ...]
    """
    token_ts, solution = _generate_token_and_hash(api_token)
    endpoint = "movie" if media_type == "movie" else "tv"
    url = (
        f"{_DMM_BASE}/api/torrents/{endpoint}"
        f"?imdbId={imdb_id}"
        f"&dmmProblemKey={token_ts}"
        f"&solution={solution}"
        f"&onlyTrusted=false"
        f"&maxSize={max_size}"
        f"&page={page}"
    )
    # TV endpoint requires seasonNum — returns 400 without it
    if endpoint == "tv":
        season_num = int(season) if season else 1
        url += f"&seasonNum={season_num}"

    _log(f"Querying DMM hash DB for {imdb_id} ({media_type})")
    s = _get_session()
    resp = s.get(url, timeout=20)
    if not resp.ok:
        body = resp.text[:500] if resp.text else "<empty>"
        _log(f"DMM {endpoint} error {resp.status_code}: {body}", xbmc.LOGERROR)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results") or []
    _log(f"DMM returned {len(results)} torrent(s) for {imdb_id}")
    return results


# ------------------------------------------------------------------ #
# RD instant availability check
# ------------------------------------------------------------------ #

def _check_rd_availability(hashes, api_token):
    """
    Check which hashes are instantly available (cached) on Real-Debrid.
    Returns a dict {hash: files_list} for cached hashes, and
    raises PermissionError on 401/403 so the caller can handle auth failures.
    """
    requests = _get_session()
    cached = {}
    video_exts = (".mkv", ".mp4", ".avi", ".m4v", ".webm", ".ts")

    for i in range(0, len(hashes), 100):
        batch = hashes[i:i + 100]
        hash_path = "/".join(batch)
        url = f"{_RD_BASE}/torrents/instantAvailability/{hash_path}"
        try:
            resp = requests.get(url, headers=_rd_headers(api_token), timeout=20)
            if resp.status_code in (401, 403):
                # Endpoint may be deprecated; caller decides if this is auth failure
                _log(f"instantAvailability returned {resp.status_code}: {resp.text[:80]}",
                     xbmc.LOGWARNING)
                return cached  # return what we have so far (probably empty)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            _log(f"RD availability check failed for batch {i}: {exc}", xbmc.LOGWARNING)
            continue

        for h in batch:
            # RD may return keys in upper or lower case
            info = data.get(h) or data.get(h.lower()) or data.get(h.upper()) or {}
            # Some RD responses wrap in a list
            if isinstance(info, list):
                info = info[0] if info else {}
            rd_entries = info.get("rd") or []
            if not rd_entries:
                continue
            best_variant = None
            best_size = 0
            for variant in rd_entries:
                for fid, finfo in variant.items():
                    fname = finfo.get("filename", "").lower()
                    fsize = finfo.get("filesize", 0)
                    if any(fname.endswith(e) for e in video_exts) and fsize > best_size:
                        best_variant = variant
                        best_size = fsize
            if best_variant:
                files = []
                for fid, finfo in best_variant.items():
                    files.append({
                        "file_id": int(fid),
                        "filename": finfo.get("filename", ""),
                        "filesize": finfo.get("filesize", 0),
                    })
                cached[h] = files

    return cached


def _availability_is_usable(api_token, hashes):
    """
    Returns (cached_dict, available) where available=False means the
    instantAvailability endpoint is broken/deprecated and we should skip it.
    """
    try:
        cached = _check_rd_availability(hashes, api_token)
        return cached, True
    except Exception as exc:
        _log(f"instantAvailability unavailable: {exc}", xbmc.LOGWARNING)
        return {}, False


def _cancelled(cancel_event):
    return cancel_event and cancel_event.is_set()


def _try_resolve_one(candidate, api_token, season, episode, cancel_event):
    """
    Try to resolve a single candidate hash via RD direct-add.
    Returns a {"url", "headers", "name"} dict on success, None on failure.
    Runs in a worker thread — must be thread-safe.
    """
    video_exts = (".mkv", ".mp4", ".avi", ".m4v", ".webm", ".ts")
    ep_markers = []
    if season and episode:
        ep_markers = [
            f"s{int(season):02d}e{int(episode):02d}",
            f"{season}x{int(episode):02d}",
        ]

    h8 = candidate['hash'][:8]
    rd_id = None
    ep_link_idx = 0  # which link index to unrestrict (used for cached season packs)
    try:
        if _cancelled(cancel_event):
            return None

        # addMagnet with 429 retry
        magnet = f"magnet:?xt=urn:btih:{candidate['hash']}"
        for attempt in range(3):
            try:
                resp = _rd_post("/torrents/addMagnet", api_token, data={"magnet": magnet})
                break
            except Exception as e:
                if "429" in str(e) and attempt < 2:
                    _time.sleep(1.0 * (attempt + 1))
                    continue
                raise
        rd_id = resp.get("id")
        if not rd_id:
            _log(f"{h8} addMagnet returned no id")
            return None

        if _cancelled(cancel_event):
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            return None

        info = _rd_get(f"/torrents/info/{rd_id}", api_token)
        status = info.get("status", "")
        _log(f"{h8} status: {status!r}")

        if _cancelled(cancel_event):
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            return None

        if status == "downloaded":
            # Season pack already cached: find the right link for this episode.
            # RD returns files and links in the same order (selected files only).
            if ep_markers:
                files = info.get("files") or []
                selected = [f for f in files if f.get("selected") == 1]
                for idx, f in enumerate(selected):
                    fname = f.get("path", "").lower()
                    if any(m in fname for m in ep_markers):
                        ep_link_idx = idx
                        _log(f"{h8} season pack: using link[{idx}] for {ep_markers[0]}")
                        break
        elif status == "waiting_files_selection":
            files = info.get("files") or []
            best_file_id = None
            best_size = 0
            # First pass: match episode if applicable
            for f in files:
                fname = f.get("path", "").lower()
                fsize = f.get("bytes", 0)
                if not any(fname.endswith(e) for e in video_exts):
                    continue
                if ep_markers and not any(m in fname for m in ep_markers):
                    continue
                if fsize > best_size:
                    best_size = fsize
                    best_file_id = f.get("id")
            # Second pass: any video file
            if not best_file_id:
                for f in files:
                    fname = f.get("path", "").lower()
                    fsize = f.get("bytes", 0)
                    if any(fname.endswith(e) for e in video_exts) and fsize > best_size:
                        best_size = fsize
                        best_file_id = f.get("id")
            if not best_file_id:
                _log(f"{h8} no video file in {len(files)} files")
                _rd_delete(f"/torrents/delete/{rd_id}", api_token)
                return None

            _rd_post(f"/torrents/selectFiles/{rd_id}", api_token,
                     data={"files": str(best_file_id)})

            if _cancelled(cancel_event):
                _rd_delete(f"/torrents/delete/{rd_id}", api_token)
                return None

            info = _rd_get(f"/torrents/info/{rd_id}", api_token)
            if info.get("status") != "downloaded":
                _log(f"{h8} not cached (status={info.get('status')!r})")
                _rd_delete(f"/torrents/delete/{rd_id}", api_token)
                return None
        else:
            _log(f"{h8} not instantly cached")
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            return None

        links = info.get("links") or []
        if not links:
            _log(f"{h8} no links")
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            return None

        if _cancelled(cancel_event):
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            return None

        link_to_use = links[min(ep_link_idx, len(links) - 1)]
        unrestrict = _rd_post("/unrestrict/link", api_token,
                               data={"link": link_to_use})
        url = unrestrict.get("download")
        filename = unrestrict.get("filename", candidate.get("title", "Stream"))
        _rd_delete(f"/torrents/delete/{rd_id}", api_token)

        if url:
            _log(f"{h8} resolved: {filename!r}")
            return {"url": url, "headers": {}, "name": filename}
        return None

    except Exception as exc:
        # 401 means the token is invalid/expired — signal this distinctly so
        # the caller can stop immediately and prompt for re-authorization.
        if "401" in str(exc):
            _log(f"{h8} RD 401 – token rejected", xbmc.LOGWARNING)
            return _RD_AUTH_FAILURE
        _log(f"{h8} failed: {exc}", xbmc.LOGWARNING)
        if rd_id:
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
        return None


def _resolve_by_direct_add(candidates_info, api_token, season=None, episode=None,
                           max_resolve=1, cancel_event=None):
    """
    Resolve streams by adding magnets to RD and checking for instant cache.
    Runs candidates in PARALLEL in batches of 3 (to avoid RD 429 rate-limits),
    returns as soon as max_resolve streams are found.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    resolved = []
    enough_event = threading.Event()

    class _CombinedEvent:
        def is_set(self):
            return _cancelled(cancel_event) or enough_event.is_set()
        def set(self):
            enough_event.set()

    combined = _CombinedEvent()

    _log(f"Resolving {len(candidates_info)} candidates in batches of 3 (need {max_resolve})")

    # Process in batches of 3 to avoid RD rate limits
    batch_size = 3
    for batch_start in range(0, len(candidates_info), batch_size):
        if _cancelled(cancel_event) or enough_event.is_set():
            break

        batch = candidates_info[batch_start:batch_start + batch_size]
        _log(f"Batch {batch_start // batch_size + 1}: candidates {batch_start + 1}-{batch_start + len(batch)}")

        pool = ThreadPoolExecutor(max_workers=batch_size)
        futures = {
            pool.submit(
                _try_resolve_one, c, api_token, season, episode, combined
            ): c
            for c in batch
        }
        try:
            for future in as_completed(futures):
                result = future.result()
                if result is _RD_AUTH_FAILURE:
                    # Count auth failures; if the whole first batch fails with 401
                    # the token is bad — abort immediately and signal re-auth needed.
                    pass  # counted below after batch drains
                elif result:
                    resolved.append(result)
                    if len(resolved) >= max_resolve:
                        enough_event.set()
                        break
                if _cancelled(cancel_event):
                    enough_event.set()
                    break
        finally:
            for f in futures:
                f.cancel()
            pool.shutdown(wait=False)

        # If every result in this batch was a 401, the token is rejected — stop now.
        # IMPORTANT: use 'non_none' guard to avoid vacuous truth — all(empty) is True in
        # Python, so a batch where every candidate was simply not cached (all None) would
        # incorrectly trigger auth failure without this check.
        batch_results = []
        for f in list(futures.keys()):
            try:
                batch_results.append(f.result() if f.done() else None)
            except Exception:
                pass
        non_none = [r for r in batch_results if r is not None]
        if non_none and all(r is _RD_AUTH_FAILURE for r in non_none):
            _log("All RD calls returned 401 – token is invalid, raising auth error", xbmc.LOGWARNING)
            raise PermissionError("rd_token_rejected")

        if enough_event.is_set():
            break

        # Small stagger between batches to avoid 429
        if batch_start + batch_size < len(candidates_info):
            _time.sleep(0.3)

    return resolved


# ------------------------------------------------------------------ #
# ------------------------------------------------------------------ #
# RD stream resolution (hash → playable URL)
# ------------------------------------------------------------------ #

def _resolve_rd_stream(torrent_hash, file_id, api_token):
    """
    Turn a cached torrent hash into a direct-play URL via Real-Debrid.
    Steps:  addMagnet → selectFiles → torrentInfo → unrestrictLink
    Returns (url, filename) or (None, None) on failure.
    """
    rd_id = None
    try:
        # 1. Add the magnet (using hash directly; RD accepts bare hashes)
        magnet = f"magnet:?xt=urn:btih:{torrent_hash}"
        resp = _rd_post("/torrents/addMagnet", api_token, data={"magnet": magnet})
        rd_id = resp.get("id")
        if not rd_id:
            _log("addMagnet returned no id", xbmc.LOGERROR)
            return None, None

        # 2. Select the target file
        _rd_post(f"/torrents/selectFiles/{rd_id}", api_token,
                 data={"files": str(file_id)})

        # 3. Wait briefly then get torrent info (with links)
        import time
        time.sleep(0.5)
        info = _rd_get(f"/torrents/info/{rd_id}", api_token)

        if info.get("status") != "downloaded":
            _log(f"Torrent status={info.get('status')!r}, expected 'downloaded'", xbmc.LOGWARNING)
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            return None, None

        links = info.get("links") or []
        if not links:
            _log("No links in torrent info", xbmc.LOGWARNING)
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            return None, None

        # 4. Unrestrict the first link to get a direct CDN URL
        unrestrict = _rd_post("/unrestrict/link", api_token, data={"link": links[0]})
        download_url = unrestrict.get("download")
        filename = unrestrict.get("filename", "Stream")

        # 5. Clean up – delete the torrent from RD library
        _rd_delete(f"/torrents/delete/{rd_id}", api_token)

        if not download_url:
            _log("unrestrict/link returned no download URL", xbmc.LOGERROR)
            return None, None

        return download_url, filename

    except Exception as exc:
        _log(f"RD resolve failed: {exc}", xbmc.LOGERROR)
        if rd_id:
            try:
                _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            except Exception:
                pass
        return None, None


# ------------------------------------------------------------------ #
# Public API
# ------------------------------------------------------------------ #

def is_stream_accessible(url, headers):
    """
    Return True when the stream URL points to real video content.
    Returns False only when Content-Length is known and too small.
    """
    _req = _get_session()
    try:
        resp = _req.head(url, headers=headers, timeout=6, allow_redirects=True)
        cl = int(resp.headers.get("content-length", -1))
        if cl < 0:
            resp2 = _req.get(url, headers={**headers, "Range": "bytes=0-0"},
                             timeout=6, stream=True, allow_redirects=True)
            cr = resp2.headers.get("content-range", "")
            if "/" in cr:
                cl = int(cr.split("/")[-1])
        if cl < 0:
            return True  # unknown → allow
        accessible = cl >= _MIN_STREAM_BYTES
        if not accessible:
            _log(
                f"Stream rejected: {cl // 1024 // 1024} MB < "
                f"{_MIN_STREAM_BYTES // 1024 // 1024} MB threshold ({url[:60]}…)",
                xbmc.LOGWARNING,
            )
        return accessible
    except Exception:
        return True


def fetch_all_cached_streams(catalog_type, video_id, cancel_event=None, query_title=None):
    """
    Main entry point.  Queries DMM's hash database, then resolves cached
    streams via RD direct-add.  Returns a sorted list of
    {"url", "headers", "name"} candidates.

    Note: RD's instantAvailability endpoint is permanently disabled
    (error_code 37), so we skip it entirely and use the direct-add
    approach: addMagnet → selectFiles → check status == 'downloaded'.

    catalog_type: "movie" or "series"
    video_id:     "tt1234567" for movies, "tt1234567:1:2" for episodes
    """
    api_token = _rd_key()
    if not api_token:
        _log("No RD access token — authorization required", xbmc.LOGWARNING)
        raise PermissionError("no_rd_token")

    # Parse video_id: for series it's "imdb:season:episode"
    parts = video_id.split(":")
    imdb_id = parts[0]
    season = parts[1] if len(parts) > 1 else None
    episode = parts[2] if len(parts) > 2 else None

    # 1. Get all hashes from DMM
    try:
        dmm_results = _fetch_dmm_hashes(
            imdb_id,
            media_type="movie" if catalog_type == "movie" else "tv",
            api_token=api_token,
            season=season,
        )
    except Exception as exc:
        _log(f"DMM hash fetch failed: {exc}", xbmc.LOGERROR)
        xbmcgui.Dialog().notification(
            "KDMM", f"DMM error: {type(exc).__name__}: {str(exc)[:120]}",
            xbmcgui.NOTIFICATION_ERROR, 8000)
        return []

    if not dmm_results:
        _log(f"No torrents in DMM database for {video_id}")
        xbmcgui.Dialog().notification(
            "KDMM", f"DMM: no torrents found for {imdb_id}",
            xbmcgui.NOTIFICATION_WARNING, 5000)
        return []

    if catalog_type != "movie":
        dmm_results = _filter_tv_results(dmm_results, query_title, season, episode)

    # Build hash → result map
    hash_map = {}
    for r in dmm_results:
        h = r.get("hash", "").lower()
        if h and len(h) == 40:
            hash_map[h] = r

    if not hash_map:
        _log("No valid hashes from DMM results")
        xbmcgui.Dialog().notification(
            "KDMM", "DMM returned results but no valid hashes",
            xbmcgui.NOTIFICATION_WARNING, 5000)
        return []

    # 2. Sort candidates using quality preferences from settings
    preferred_groups, hdr_pref, res_pref, src_pref = _get_quality_preferences()
    sort_key = _build_sort_key(preferred_groups, hdr_pref, res_pref, src_pref)

    if catalog_type != "movie":
        sorted_dmm = sorted(dmm_results, key=_build_tv_sort_key(sort_key, query_title, season, episode))
    else:
        sorted_dmm = sorted(dmm_results, key=sort_key)

    # Log the top picks so user can verify ranking
    for i, r in enumerate(sorted_dmm[:5]):
        parsed = _parse_title(r.get("title", ""))
        extra = ""
        if catalog_type != "movie":
            title_rank = _title_sequence_rank(r.get("title", ""), query_title) or (9, 99)
            episode_rank = _episode_match_rank(r.get("title", ""), season, episode)
            season_rank = _season_match_rank(r.get("title", ""), season)
            extra = f" match={title_rank} ep={episode_rank} season={season_rank}"
        _log(f"  #{i+1}: {r.get('title','?')[:80]} "
             f"[hdr={parsed['hdr']} res={parsed['res']} src={parsed['src']} grp={parsed['group']}{extra}]")

    candidates = [
        {"hash": (r.get("hash") or "").lower(), "title": r.get("title", "Unknown")}
        for r in sorted_dmm if len((r.get("hash") or "")) == 40
    ][:20]  # top 20 by quality

    _log(f"DMM returned {len(hash_map)} hashes, checking top {len(candidates)} in parallel on RD")

    resolved = _resolve_by_direct_add(
        candidates, api_token, season=season, episode=episode,
        max_resolve=1, cancel_event=cancel_event,
    )

    if not resolved:
        xbmcgui.Dialog().notification(
            "KDMM", "No cached streams found for this title on RD",
            xbmcgui.NOTIFICATION_WARNING, 6000)
    else:
        _log(f"Resolved {len(resolved)} stream(s), top: {resolved[0]['name']!r}")

    return resolved
