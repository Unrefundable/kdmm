"""
KDMM – lib/dmm.py
Debrid Media Manager torrent lookup + debrid stream resolver.

Flow:
  1.  Generate a DMM proof-of-work token (port of their JS generateTokenAndHash).
  2.  GET  debridmediamanager.com/api/torrents/movie (or tv)
      → returns every known torrent hash for the IMDB ID.
  3.  Sort candidates by preferred release groups + file size.
  4.  For each candidate, check debrid cache via direct-add:
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
_AD_BASE = "https://api.alldebrid.com/v4"
_AD_BASE_41 = "https://api.alldebrid.com/v4.1"

# Minimum file size to distinguish real content from RD error clips.
_MIN_STREAM_BYTES = 50 * 1024 * 1024  # 50 MB

_AUTH_FAILURE = object()
_RD_AUTH_FAILURE = _AUTH_FAILURE
_RD_BLOCKED_HASH_KEY = "blocked_hash"
_PROVIDER_RD = "realdebrid"
_PROVIDER_AD = "alldebrid"
_AUTH_NOTICE_SHOWN = set()

_VIDEO_EXTS = (".mkv", ".mp4", ".avi", ".m4v", ".webm", ".ts")
_NON_MAIN_VIDEO_DIRS = {
    "extras", "extra", "featurettes", "sample", "samples", "screens",
    "screenshot", "screenshots", "sub", "subs", "subtitle", "subtitles",
}
_GENERIC_VIDEO_BASENAMES = {
    "video", "movie", "film", "file", "default", "stream", "download",
}
_AV1_TOKEN_RE = re.compile(r"(^|[^a-z0-9])(?:av1|av01)([^a-z0-9]|$)", re.I)
_CODEC_PROBE_BYTES = 2 * 1024 * 1024
_AV1_CODEC_MARKERS = (b"av01", b"v_av1")
_NON_AV1_CODEC_MARKERS = (
    b"avc1", b"avc3", b"hvc1", b"hev1", b"dvh1", b"dvhe",
    b"vp09", b"vp08", b"v_mpeg4/iso/avc", b"v_mpegh/iso/hevc",
    b"v_mpeg4/iso/asp", b"v_mpeg2", b"v_vp8", b"v_vp9",
)
_AUDIO_PROBE_BYTES = 4 * 1024 * 1024
_EPISODE_PROBE_BYTES = 4 * 1024 * 1024
_AUDIO_LANG_ALIASES = {
    "en": "en", "eng": "en", "english": "en",
    "ja": "ja", "jpn": "ja", "jap": "ja", "japanese": "ja",
    "ko": "ko", "kor": "ko", "korean": "ko",
    "es": "es", "spa": "es", "esp": "es", "esl": "es", "spanish": "es",
    "castellano": "es", "castilian": "es",
    "fr": "fr", "fre": "fr", "fra": "fr", "french": "fr",
    "de": "de", "ger": "de", "deu": "de", "german": "de",
    "it": "it", "ita": "it", "italian": "it",
    "pt": "pt", "por": "pt", "portuguese": "pt", "br": "pt", "brazilian": "pt",
    "ru": "ru", "rus": "ru", "russian": "ru",
    "zh": "zh", "chi": "zh", "zho": "zh", "cmn": "zh", "mandarin": "zh",
    "yue": "zh", "cantonese": "zh", "chinese": "zh",
    "hi": "hi", "hin": "hi", "hindi": "hi",
    "ta": "ta", "tam": "ta", "tamil": "ta",
    "te": "te", "tel": "te", "telugu": "te",
    "ar": "ar", "ara": "ar", "arabic": "ar",
    "nl": "nl", "dut": "nl", "nld": "nl", "dutch": "nl",
    "pl": "pl", "pol": "pl", "polish": "pl",
    "sv": "sv", "swe": "sv", "swedish": "sv",
    "da": "da", "dan": "da", "danish": "da",
    "no": "no", "nor": "no", "norwegian": "no",
    "fi": "fi", "fin": "fi", "finnish": "fi",
    "tr": "tr", "tur": "tr", "turkish": "tr",
}
_AUDIO_LANGUAGE_CHOICES = (
    "", "en", "ja", "ko", "es", "fr", "de", "it", "pt", "zh",
    "hi", "ar", "ru", "nl", "pl", "sv", "da", "no", "fi", "tr",
)
_AUDIO_LANGUAGE_LABELS = {
    "any": "",
    "english": "en",
    "japanese": "ja",
    "korean": "ko",
    "spanish": "es",
    "french": "fr",
    "german": "de",
    "italian": "it",
    "portuguese": "pt",
    "chinese": "zh",
    "hindi": "hi",
    "arabic": "ar",
    "russian": "ru",
    "dutch": "nl",
    "polish": "pl",
    "swedish": "sv",
    "danish": "da",
    "norwegian": "no",
    "finnish": "fi",
    "turkish": "tr",
}
_ENGLISH_DEFAULT_AUDIO_LABELS = {"default", "standard"}
_RD_ADD_MAGNET_BATCH_SIZE = 3
_RD_ADD_MAGNET_BATCH_PAUSE = 0.35
_RD_ADD_MAGNET_429_BACKOFFS = (1.5, 3.0, 6.0)
_INSTALLMENT_TOKENS = {
    "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x",
    "2", "3", "4", "5", "6", "7", "8", "9", "10",
}


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


def is_av1_stream(value):
    """
    Return True when a title, filename, URL, or candidate dict advertises AV1.

    This intentionally matches release tokens like "AV1" and "AV01" without
    treating unrelated words containing those letters as codec metadata.
    """
    if isinstance(value, dict):
        if value.get("av1_probe") == "av1":
            return True
        if value.get("av1_probe") == "not_av1":
            return False
        parts = [
            value.get("name"),
            value.get("title"),
            value.get("filename"),
            value.get("url"),
        ]
        return any(is_av1_stream(part) for part in parts if part)
    return bool(_AV1_TOKEN_RE.search(str(value or "")))


def _actual_av1_probe_enabled():
    return _setting_bool("probe_av1_codec", True)


def _skip_advertised_av1_before_resolve(value):
    # When probing is enabled, text is only a fallback after the direct URL is available.
    return not _actual_av1_probe_enabled() and is_av1_stream(value)


def _av1_text_rank(value):
    return 1 if is_av1_stream(value) else 0


def _read_url_sample(url, headers=None, start=0, size=_CODEC_PROBE_BYTES, timeout=4):
    if not url or size <= 0:
        return b""
    req_headers = dict(headers or {})
    req_headers["Range"] = f"bytes={start}-{start + size - 1}"
    resp = None
    try:
        resp = _get_session().get(
            url, headers=req_headers, timeout=timeout, stream=True, allow_redirects=True
        )
        if resp.status_code not in (200, 206):
            return b""
        chunks = []
        remaining = size
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            chunks.append(chunk[:remaining])
            remaining -= len(chunks[-1])
            if remaining <= 0:
                break
        return b"".join(chunks)
    except Exception as exc:
        _log(f"Codec probe sample failed: {exc}", xbmc.LOGWARNING)
        return b""
    finally:
        if resp is not None:
            try:
                resp.close()
            except Exception:
                pass


def _remote_content_length(url, headers=None, timeout=4):
    try:
        resp = _get_session().head(
            url, headers=headers or {}, timeout=timeout, allow_redirects=True
        )
        length = int(resp.headers.get("content-length", -1))
        if length >= 0:
            return length
    except Exception:
        pass

    resp = None
    try:
        resp = _get_session().get(
            url,
            headers={**(headers or {}), "Range": "bytes=0-0"},
            timeout=timeout,
            stream=True,
            allow_redirects=True,
        )
        cr = resp.headers.get("content-range", "")
        if "/" in cr:
            return int(cr.split("/")[-1])
    except Exception:
        return -1
    finally:
        if resp is not None:
            try:
                resp.close()
            except Exception:
                pass
    return -1


def _detect_av1_from_codec_bytes(data):
    lower = (data or b"").lower()
    if any(marker in lower for marker in _AV1_CODEC_MARKERS):
        return True
    if any(marker in lower for marker in _NON_AV1_CODEC_MARKERS):
        return False
    return None


def _probe_actual_av1_stream(url, headers=None):
    """
    Inspect real container metadata from a direct URL.

    Returns True for AV1, False for a known non-AV1 video codec, and None when
    the remote file cannot be determined quickly. This avoids full downloads.
    """
    if not _actual_av1_probe_enabled():
        return None

    start_sample = _read_url_sample(url, headers)
    result = _detect_av1_from_codec_bytes(start_sample)
    if result is not None:
        return result

    length = _remote_content_length(url, headers)
    if length > _CODEC_PROBE_BYTES:
        end_start = max(0, length - _CODEC_PROBE_BYTES)
        end_sample = _read_url_sample(url, headers, start=end_start)
        result = _detect_av1_from_codec_bytes(end_sample)
        if result is not None:
            return result

    return None


def _sample_stream_edges(url, headers=None, size=_EPISODE_PROBE_BYTES):
    samples = []
    start_sample = _read_url_sample(url, headers, size=size)
    if start_sample:
        samples.append(start_sample)

    length = _remote_content_length(url, headers)
    if length > size:
        end_sample = _read_url_sample(
            url, headers, start=max(0, length - size), size=size
        )
        if end_sample:
            samples.append(end_sample)
    return samples


def _sample_text(samples):
    chunks = []
    for sample in samples or []:
        if not sample:
            continue
        chunks.append(sample.decode("utf-8", "ignore"))
        chunks.append(sample.decode("latin-1", "ignore"))
    return _normalize_text(" ".join(chunks))


def _probe_episode_identity_stream(url, headers=None, season=None, episode=None,
                                   episode_title=None):
    """
    Return True when direct metadata points at the requested episode, False
    when it points at a different episode in the same season, and None when
    no trustworthy identity metadata is visible quickly.
    """
    if not season or not episode:
        return True

    wanted = (_safe_int(season), _safe_int(episode))
    if not wanted[0] or not wanted[1]:
        return None

    text = _sample_text(_sample_stream_edges(url, headers))
    if not text:
        return None

    keys = _extract_episode_keys(text)
    if wanted in keys:
        return True

    conflicting = sorted(
        key for key in keys
        if key[0] == wanted[0] and key[1] != wanted[1]
    )
    if conflicting:
        label = ", ".join(f"S{s:02d}E{e:02d}" for s, e in conflicting[:3])
        _log(
            f"Episode identity probe found conflicting metadata {label}; "
            f"wanted {_episode_label(season, episode)}",
            xbmc.LOGWARNING,
        )
        return False

    if episode_title and _title_sequence_rank(text, episode_title) is not None:
        return True
    return None


def _setting_text(key, default=""):
    try:
        value = xbmcaddon.Addon().getSetting(key)
    except Exception:
        value = default
    return default if value is None else str(value)


def _canonical_audio_language(value):
    text = _normalize_text(value).strip()
    if not text:
        return ""
    if text in _AUDIO_LANG_ALIASES:
        return _AUDIO_LANG_ALIASES[text]
    for token in _tokenize_text(text):
        if token in _AUDIO_LANG_ALIASES:
            return _AUDIO_LANG_ALIASES[token]
    return text


def _preferred_audio_language():
    raw = _setting_text("preferred_audio_language", "").strip()
    if raw.isdigit():
        idx = int(raw)
        if 0 <= idx < len(_AUDIO_LANGUAGE_CHOICES):
            return _AUDIO_LANGUAGE_CHOICES[idx]
    normalized = _normalize_text(raw).strip()
    if normalized in _AUDIO_LANGUAGE_LABELS:
        return _AUDIO_LANGUAGE_LABELS[normalized]
    return _canonical_audio_language(raw)


def _audio_track_matches_preference(track, preferred):
    if not preferred:
        return True
    values = [
        track.get("language", ""),
        track.get("language_ietf", ""),
        track.get("name", ""),
    ]
    for value in values:
        if _canonical_audio_language(value) == preferred:
            return True

    if preferred == "en":
        for value in values:
            tokens = set(_tokenize_text(value))
            if tokens & _ENGLISH_DEFAULT_AUDIO_LABELS:
                return True
    return False


def _read_ebml_id(data, pos):
    if pos >= len(data):
        return None, pos
    first = data[pos]
    mask = 0x80
    length = 1
    while length <= 4 and not (first & mask):
        mask >>= 1
        length += 1
    if length > 4 or pos + length > len(data):
        return None, pos
    return int.from_bytes(data[pos:pos + length], "big"), pos + length


def _read_ebml_size(data, pos):
    if pos >= len(data):
        return None, pos
    first = data[pos]
    mask = 0x80
    length = 1
    while length <= 8 and not (first & mask):
        mask >>= 1
        length += 1
    if length > 8 or pos + length > len(data):
        return None, pos
    value = first & (mask - 1)
    for byte in data[pos + 1:pos + length]:
        value = (value << 8) | byte
    max_value = (1 << (7 * length)) - 1
    if value == max_value:
        value = len(data) - (pos + length)
    return value, pos + length


def _iter_ebml_children(data, start, end):
    pos = start
    end = min(end, len(data))
    while pos < end:
        element_id, payload_start = _read_ebml_id(data, pos)
        if element_id is None or payload_start <= pos:
            break
        size, payload_start = _read_ebml_size(data, payload_start)
        if size is None:
            break
        payload_end = payload_start + size
        if payload_end > len(data):
            break
        yield element_id, payload_start, payload_end
        pos = payload_end


def _decode_ebml_uint(value):
    if not value:
        return 0
    return int.from_bytes(value, "big")


def _decode_ebml_text(value):
    for encoding in ("utf-8", "latin-1"):
        try:
            return value.decode(encoding, "ignore").strip("\x00 ")
        except Exception:
            continue
    return ""


def _parse_mkv_track_entry(data, start, end):
    track = {
        "container": "mkv",
        "language": "",
        "language_ietf": "",
        "name": "",
        "default": False,
        "type": 0,
    }
    for element_id, payload_start, payload_end in _iter_ebml_children(data, start, end):
        value = data[payload_start:payload_end]
        if element_id == 0x83:  # TrackType
            track["type"] = _decode_ebml_uint(value)
        elif element_id == 0x22B59C:  # Language
            track["language"] = _decode_ebml_text(value)
        elif element_id == 0x22B59D:  # LanguageIETF
            track["language_ietf"] = _decode_ebml_text(value)
        elif element_id == 0x536E:  # Name
            track["name"] = _decode_ebml_text(value)
        elif element_id == 0x88:  # FlagDefault
            track["default"] = bool(_decode_ebml_uint(value))
    return track if track.get("type") == 2 else None


def _parse_mkv_audio_tracks(data):
    tracks = []
    search_from = 0
    tracks_id = b"\x16\x54\xAE\x6B"
    while True:
        idx = data.find(tracks_id, search_from)
        if idx < 0:
            break
        element_id, payload_start = _read_ebml_id(data, idx)
        if element_id != 0x1654AE6B:
            search_from = idx + 1
            continue
        size, payload_start = _read_ebml_size(data, payload_start)
        if size is None:
            break
        payload_end = min(payload_start + size, len(data))
        for child_id, child_start, child_end in _iter_ebml_children(data, payload_start, payload_end):
            if child_id == 0xAE:
                track = _parse_mkv_track_entry(data, child_start, child_end)
                if track:
                    tracks.append(track)
        search_from = max(idx + 1, payload_end)
    return tracks


def _iter_mp4_atoms(data, start=0, end=None):
    end = len(data) if end is None else min(end, len(data))
    pos = start
    while pos + 8 <= end:
        size = int.from_bytes(data[pos:pos + 4], "big")
        atom_type = data[pos + 4:pos + 8]
        header_size = 8
        if size == 1:
            if pos + 16 > end:
                break
            size = int.from_bytes(data[pos + 8:pos + 16], "big")
            header_size = 16
        elif size == 0:
            size = end - pos
        if size < header_size or pos + size > end:
            break
        if not all(32 <= byte <= 126 for byte in atom_type):
            break
        yield atom_type, pos + header_size, pos + size
        pos += size


def _find_mp4_child(data, start, end, wanted):
    for atom_type, payload_start, payload_end in _iter_mp4_atoms(data, start, end):
        if atom_type == wanted:
            return payload_start, payload_end
    return None, None


def _decode_mp4_language(packed):
    chars = []
    for shift in (10, 5, 0):
        value = ((packed >> shift) & 0x1F) + 0x60
        if value < 0x61 or value > 0x7A:
            return ""
        chars.append(chr(value))
    lang = "".join(chars)
    return "" if lang == "und" else lang


def _parse_mp4_track(data, start, end):
    mdia_start, mdia_end = _find_mp4_child(data, start, end, b"mdia")
    if mdia_start is None:
        return None

    handler = ""
    language = ""
    name = ""
    for atom_type, payload_start, payload_end in _iter_mp4_atoms(data, mdia_start, mdia_end):
        value = data[payload_start:payload_end]
        if atom_type == b"hdlr" and len(value) >= 24:
            handler = value[8:12].decode("latin-1", "ignore")
            name = value[24:].decode("utf-8", "ignore").strip("\x00 ")
        elif atom_type == b"mdhd" and len(value) >= 24:
            version = value[0]
            lang_offset = 32 if version == 1 else 20
            if len(value) >= lang_offset + 2:
                language = _decode_mp4_language(
                    int.from_bytes(value[lang_offset:lang_offset + 2], "big")
                )
    if handler != "soun":
        return None
    return {
        "container": "mp4",
        "language": language,
        "language_ietf": "",
        "name": name,
        "default": False,
    }


def _parse_mp4_audio_tracks(data):
    tracks = []
    starts = [0]
    search_from = 0
    while True:
        idx = data.find(b"moov", search_from)
        if idx < 4:
            break
        atom_start = idx - 4
        if atom_start not in starts:
            starts.append(atom_start)
        search_from = idx + 4

    for start in starts:
        for atom_type, payload_start, payload_end in _iter_mp4_atoms(data, start, len(data)):
            if atom_type != b"moov":
                continue
            for child_type, child_start, child_end in _iter_mp4_atoms(data, payload_start, payload_end):
                if child_type == b"trak":
                    track = _parse_mp4_track(data, child_start, child_end)
                    if track:
                        tracks.append(track)
    return tracks


def _dedupe_audio_tracks(tracks):
    deduped = []
    seen = set()
    for track in tracks:
        key = (
            _canonical_audio_language(track.get("language", "")),
            _canonical_audio_language(track.get("language_ietf", "")),
            _normalize_text(track.get("name", "")),
            track.get("container", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(track)
    return deduped


def _format_audio_tracks(tracks):
    labels = []
    for track in tracks:
        parts = []
        for key in ("language_ietf", "language", "name"):
            value = track.get(key)
            if value and value not in parts:
                parts.append(value)
        labels.append("/".join(parts) if parts else "unknown")
    return ", ".join(labels)


def _probe_audio_language_stream(url, headers=None, preferred=None):
    """
    Return True when the preferred language is present, False when audio
    tracks were read and none matched, and None when metadata is inconclusive.
    """
    preferred = preferred or _preferred_audio_language()
    if not preferred:
        return True

    samples = _sample_stream_edges(url, headers, size=_AUDIO_PROBE_BYTES)

    tracks = []
    for sample in samples:
        tracks.extend(_parse_mkv_audio_tracks(sample))
        tracks.extend(_parse_mp4_audio_tracks(sample))

    tracks = _dedupe_audio_tracks(tracks)
    if not tracks:
        return None
    if any(_audio_track_matches_preference(track, preferred) for track in tracks):
        _log(f"Audio language probe matched {preferred}: {_format_audio_tracks(tracks)}")
        return True
    _log(
        f"Audio language probe found no {preferred} track: {_format_audio_tracks(tracks)}",
        xbmc.LOGWARNING,
    )
    return False


def candidate_matches_audio_preference(candidate):
    preferred = _preferred_audio_language()
    if not preferred:
        return True
    if not isinstance(candidate, dict):
        return False
    if candidate.get("audio_language_preference") != preferred:
        return False
    return candidate.get("audio_language_probe") in ("matched", "unknown")


def candidate_matches_episode_identity(candidate, season=None, episode=None,
                                       episode_title=None):
    return _episode_stream_identity_is_safe(
        candidate, season=season, episode=episode, episode_title=episode_title
    )


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


def _is_release_boundary_token(token):
    if not token:
        return True
    if token in _RELEASE_CONTEXT_TOKENS:
        return True
    if _is_year_token(token):
        return True
    if re.match(r"^s\d{1,2}(?:e\d{1,3})?$", token):
        return True
    if re.match(r"^\d{1,2}x\d{1,3}$", token):
        return True
    if re.match(r"^\d{3,4}p$", token):
        return True
    return False


def _trailing_title_tokens(title_tokens, end):
    trailing = []
    for token in title_tokens[end:]:
        if _is_release_boundary_token(token):
            break
        trailing.append(token)
    return trailing


def _has_conflicting_instalment(trailing_tokens, expected_tokens):
    expected = set(expected_tokens or [])
    for idx, token in enumerate(trailing_tokens):
        if token in _INSTALLMENT_TOKENS and token not in expected:
            return True
        if token in ("part", "chapter", "volume", "vol") and idx + 1 < len(trailing_tokens):
            next_token = trailing_tokens[idx + 1]
            if next_token in _INSTALLMENT_TOKENS and next_token not in expected:
                return True
    return False


def _year_rank(title, expected_year):
    if not expected_year:
        return 1
    expected = str(expected_year).strip()
    if not expected.isdigit():
        return 1
    years = set(re.findall(r"\b(19\d{2}|20\d{2})\b", _normalize_text(title)))
    if expected in years:
        return 0
    if years:
        return 2
    return 1


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
        trailing_tokens = _trailing_title_tokens(title_tokens, end)
        if _has_conflicting_instalment(trailing_tokens, query_tokens):
            continue

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

    season_episode_hits = set()
    for value_season, value_episode in re.findall(r"\bs0*(\d{1,2})[ ._-]*e0*(\d{1,3})\b", text):
        season_episode_hits.add((int(value_season), int(value_episode)))
    for value_season, value_episode in re.findall(r"\b(\d{1,2})x(\d{1,3})\b", text):
        season_episode_hits.add((int(value_season), int(value_episode)))
    for value_season, value_episode in re.findall(r"\bs0*(\d{1,2})[ ._-]+0*(\d{1,3})\b", text):
        season_episode_hits.add((int(value_season), int(value_episode)))

    if (season_num, episode_num) in season_episode_hits:
        return 0
    if season_episode_hits:
        return 2

    exact_patterns = [
        rf"\bseason[ ._-]*0*{season_num}[ ._-]*episode[ ._-]*0*{episode_num}\b",
        rf"\bepisode[ ._-]*0*{episode_num}\b",
        rf"\bep[ ._-]*0*{episode_num}\b",
        rf"\be0*{episode_num}\b",
    ]
    if any(re.search(pattern, text) for pattern in exact_patterns):
        return 0
    return 1


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _setting_bool(key, default=False):
    try:
        value = xbmcaddon.Addon().getSetting(key)
    except Exception:
        value = "true" if default else "false"
    if value == "":
        return default
    return value.lower() == "true"


def _setting_int(key, default=0):
    try:
        value = xbmcaddon.Addon().getSetting(key)
        return int(value) if value not in (None, "") else default
    except Exception:
        return default


def _extract_episode_keys(text):
    text = _normalize_text(text)
    keys = set()
    for season, episode in re.findall(r"\bs0*(\d{1,2})[ ._-]*e0*(\d{1,3})\b", text):
        keys.add((int(season), int(episode)))
    for season, episode in re.findall(r"\b(\d{1,2})x0*(\d{1,3})\b", text):
        keys.add((int(season), int(episode)))
    for season, episode in re.findall(r"\bs0*(\d{1,2})[ ._-]+0*(\d{1,3})\b", text):
        keys.add((int(season), int(episode)))
    return keys


def _episode_file_matches(file_info, season, episode):
    if not season or not episode:
        return True
    path = _video_file_path(file_info)
    if _video_path_is_non_main(path):
        return False
    return (
        _episode_match_rank(path, season, episode) == 0
        and _season_match_rank(path, season) < 2
    )


def _episode_label(season, episode):
    return f"S{int(season):02d}E{int(episode):02d}"


def _video_file_path(file_info):
    if isinstance(file_info, str):
        return file_info
    if not isinstance(file_info, dict):
        return ""
    return (
        file_info.get("path")
        or file_info.get("filename")
        or file_info.get("name")
        or file_info.get("file")
        or ""
    )


def _video_path_components(path):
    path = str(path or "").replace("\\", "/")
    return [part for part in path.split("/") if part]


def _video_path_is_non_main(path):
    parts = _video_path_components(path)
    for part in parts[:-1]:
        normalized = _normalize_text(part).strip(" ._-")
        if normalized in _NON_MAIN_VIDEO_DIRS:
            return True
    return False


def _video_basename(path):
    parts = _video_path_components(path)
    return parts[-1] if parts else str(path or "")


def _video_stem(path):
    basename = _video_basename(path)
    stem = basename.rsplit(".", 1)[0] if "." in basename else basename
    return _normalize_text(stem).strip(" ._-")


def _video_filename_has_episode_identity(path, season, episode, episode_title=None):
    basename = _video_basename(path)
    if not basename:
        return False
    if season and episode and _episode_match_rank(basename, season, episode) == 0:
        return True
    return bool(episode_title and _title_sequence_rank(basename, episode_title) is not None)


def _video_filename_is_generic(path):
    stem = _video_stem(path)
    if not stem:
        return True
    if stem in _GENERIC_VIDEO_BASENAMES:
        return True
    return bool(re.fullmatch(r"(?:video|movie|film|file|stream|download)[ ._-]*\d{0,3}", stem))


def _episode_stream_identity_is_safe(candidate, season=None, episode=None, episode_title=None):
    if not season or not episode or not isinstance(candidate, dict):
        return True
    if candidate.get("episode_identity_probe") == "matched":
        return True
    source_file = candidate.get("source_file") or ""
    filename = candidate.get("name") or candidate.get("filename") or ""
    if _video_path_is_non_main(source_file):
        return False
    if _video_filename_has_episode_identity(filename, season, episode, episode_title):
        return True
    if _video_filename_has_episode_identity(source_file, season, episode, episode_title):
        return not _video_filename_is_generic(source_file)
    if _video_filename_is_generic(filename) or _video_filename_is_generic(source_file):
        return False
    return False


def _video_file_size(file_info):
    if not isinstance(file_info, dict):
        return 0
    return (
        file_info.get("bytes")
        or file_info.get("filesize")
        or file_info.get("fileSize")
        or file_info.get("size")
        or 0
    )


def _candidate_video_files(entry):
    files = entry.get("files") or entry.get("fileList") or entry.get("file_list") or []
    video_files = []
    if isinstance(files, dict):
        files = files.values()
    for f in files:
        path = _video_file_path(f)
        if path and path.lower().endswith(_VIDEO_EXTS) and not _video_path_is_non_main(path):
            video_files.append(f)
    return video_files


def _candidate_episode_keys(entry):
    keys = set()
    for file_info in _candidate_video_files(entry):
        keys.update(_extract_episode_keys(_video_file_path(file_info)))
    if not keys:
        keys.update(_extract_episode_keys(entry.get("title") or ""))
    return keys


def _candidate_episode_match_kind(entry, season, episode):
    if not season or not episode:
        return "none"
    wanted = (_safe_int(season), _safe_int(episode))
    keys = _candidate_episode_keys(entry)
    if keys:
        return "exact" if wanted in keys else "conflict"

    title_rank = _episode_match_rank(entry.get("title", ""), season, episode)
    if title_rank == 0:
        return "exact"
    if title_rank == 2:
        return "conflict"
    if _title_has_season_pack_signal(entry.get("title", ""), season):
        return "ambiguous_pack"
    return "unknown"


def _candidate_contains_episode(entry, season, episode):
    if not season or not episode:
        return True
    return _candidate_episode_match_kind(entry, season, episode) in (
        "exact", "ambiguous_pack", "unknown",
    )


def _season_episode_coverage(entry, season):
    season_num = _safe_int(season)
    return sorted({
        episode for hit_season, episode in _candidate_episode_keys(entry)
        if hit_season == season_num
    })


def _title_has_season_pack_signal(title, season):
    if not season:
        return False
    text = _normalize_text(title)
    season_num = _safe_int(season)
    if _extract_episode_keys(text):
        return False
    season_patterns = (
        rf"\bs0*{season_num}\b",
        rf"\bseason[ ._-]*0*{season_num}\b",
        rf"\bseries[ ._-]*0*{season_num}\b",
    )
    return any(re.search(pattern, text) for pattern in season_patterns)


def _consecutive_from_one(episodes):
    expected = 1
    for episode in sorted(set(episodes)):
        if episode == expected:
            expected += 1
        elif episode > expected:
            break
    return expected - 1


def _tv_pack_rank(entry, season, episode):
    """
    Lower is better: complete series/season packs, incomplete multi packs,
    single-episode releases, then unknown. Uses file lists when DMM provides
    them and title heuristics otherwise.
    """
    title = _normalize_text(entry.get("title") or "")
    files = _candidate_video_files(entry)
    coverage = _season_episode_coverage(entry, season)
    current_ep = _safe_int(episode)
    contains_current = not current_ep or current_ep in coverage or _candidate_contains_episode(entry, season, episode)
    if not contains_current:
        return (5, "missing")

    has_multiple_seasons = len({s for s, _ in _candidate_episode_keys(entry)}) > 1
    explicit_complete = any(token in title for token in (
        "complete", "full season", "season complete", "complete season",
        "collection", "series pack",
    ))
    if has_multiple_seasons and len(files) > 1:
        return (0, "series")
    if coverage:
        consecutive = _consecutive_from_one(coverage)
        if explicit_complete or consecutive >= max(current_ep, 3) and len(coverage) >= 3:
            return (1, "season")
        if len(coverage) > 1:
            return (2, "multi")
        return (3, "single")
    if _title_has_season_pack_signal(title, season):
        return (1, "season")
    if explicit_complete:
        return (1, "season")
    if len(files) > 1:
        return (2, "multi")
    if len(files) == 1:
        return (3, "single")
    return (4, "unknown")


def _pack_sort_rank(entry, season, episode, preference):
    pack_rank, _scope = _tv_pack_rank(entry, season, episode)
    if preference == 2:  # Allow singles: quality is more important.
        return 0
    if preference == 1:  # Balanced: prefer packs, but do not over-penalize.
        return min(pack_rank, 2)
    return pack_rank


def _episode_specificity_rank(entry, season, episode):
    kind = _candidate_episode_match_kind(entry, season, episode)
    if kind == "exact":
        return 0
    if kind == "ambiguous_pack":
        return 2
    if kind == "unknown":
        return 3
    if kind == "conflict":
        return 9
    return 4


def _filter_movie_results(results, movie_title, year, strict=False):
    filtered = list(results)

    if movie_title:
        title_matches = [
            result for result in filtered
            if _title_sequence_rank(result.get("title", ""), movie_title) is not None
        ]
        if title_matches:
            _log(f"Movie title filter: {len(filtered)} -> {len(title_matches)} results matching {movie_title!r}")
            filtered = title_matches
        elif strict:
            _log(f"Movie title filter removed all {len(filtered)} results for {movie_title!r}", xbmc.LOGWARNING)
            return []
        else:
            _log(f"Movie title filter removed all {len(filtered)} results for {movie_title!r} - keeping broader set",
                 xbmc.LOGWARNING)

    if year:
        year_matches = [
            result for result in filtered
            if _year_rank(result.get("title", ""), year) < 2
        ]
        if year_matches:
            _log(f"Movie year filter: {len(filtered)} -> {len(year_matches)} results for {year}")
            filtered = year_matches
        elif strict:
            _log(f"Movie year filter removed all {len(filtered)} results for {year}", xbmc.LOGWARNING)
            return []
        else:
            _log(f"Movie year filter removed all {len(filtered)} results for {year} - keeping broader set",
                 xbmc.LOGWARNING)

    return filtered


def _filter_tv_results(results, show_title, season, episode, strict=False):
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
        elif strict:
            _log(f"TV title filter removed all {len(filtered)} results for {show_title!r}", xbmc.LOGWARNING)
            return []
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
        elif strict:
            _log(f"Season filter removed all {len(filtered)} results for season {int(season)}", xbmc.LOGWARNING)
            return []
        else:
            _log(f"Season filter removed all {len(filtered)} results for season {int(season)} - keeping broader set",
                 xbmc.LOGWARNING)

    if season and episode:
        possible_matches = [
            result for result in filtered
            if _episode_match_rank(result.get("title", ""), season, episode) < 2
            and _candidate_contains_episode(result, season, episode)
        ]
        exact_matches = [
            result for result in possible_matches
            if _candidate_episode_match_kind(result, season, episode) == "exact"
        ]
        if exact_matches:
            _log(
                f"Episode filter: {len(filtered)} -> {len(exact_matches)} "
                f"exact result(s) for E{int(episode):02d}"
            )
            filtered = exact_matches
        elif possible_matches:
            _log(
                f"Episode filter: {len(filtered)} -> {len(possible_matches)} "
                f"possible result(s) for E{int(episode):02d}"
            )
            filtered = possible_matches
        elif strict:
            _log(f"Episode filter removed all {len(filtered)} results for E{int(episode):02d}", xbmc.LOGWARNING)
            return []
        else:
            _log(f"Episode filter removed all {len(filtered)} results for E{int(episode):02d} - keeping broader set",
                 xbmc.LOGWARNING)

    return filtered


def _filter_av1_results(results):
    filtered = [
        result for result in results
        if not _skip_advertised_av1_before_resolve(result.get("title", ""))
    ]
    removed = len(results) - len(filtered)
    if removed:
        _log(f"AV1 filter: removed {removed} unsupported result(s)")
    return filtered


def _build_movie_sort_key(quality_sort_key, movie_title, year):
    def _sort_key(entry):
        title = entry.get("title") or ""
        title_rank = _title_sequence_rank(title, movie_title) or (9, 99)
        return title_rank + (_year_rank(title, year),) + quality_sort_key(entry)

    return _sort_key


def _build_tv_sort_key(quality_sort_key, show_title, season, episode,
                       pack_preference, episode_title=None):
    def _sort_key(entry):
        title = entry.get("title") or ""
        title_rank = _title_sequence_rank(title, show_title) or (9, 99)
        season_rank = _season_match_rank(title, season)
        episode_rank = _episode_match_rank(title, season, episode)
        specificity_rank = _episode_specificity_rank(entry, season, episode)
        episode_title_rank = 1
        if episode_title and _title_sequence_rank(title, episode_title) is not None:
            episode_title_rank = 0
        pack_rank = _pack_sort_rank(entry, season, episode, pack_preference)
        return title_rank + (
            specificity_rank, episode_title_rank, episode_rank, season_rank,
            pack_rank,
        ) + quality_sort_key(entry)

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


def _get_matching_preferences():
    strict_matching = _setting_bool("strict_title_matching", False)
    tv_pack_preference = _setting_int("tv_pack_preference", 0)
    return strict_matching, tv_pack_preference


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


def _ad_key():
    """Return a configured AllDebrid API key, if available."""
    from ad_auth import get_access_token
    return get_access_token()


def _provider_label(provider):
    return "AllDebrid" if provider == _PROVIDER_AD else "Real-Debrid"


def _provider_short(provider):
    return "AD" if provider == _PROVIDER_AD else "RD"


def _provider_auth_notice(provider, reason=None):
    return f"{_provider_label(provider)} authorization failed"


def _notify_provider_auth_failure(provider, reason=None):
    key = (provider, reason or "")
    if key in _AUTH_NOTICE_SHOWN:
        return
    _AUTH_NOTICE_SHOWN.add(key)
    message = _provider_auth_notice(provider, reason)
    _log(message, xbmc.LOGWARNING)
    try:
        xbmcgui.Dialog().notification(
            "KDMM authorization",
            message,
            xbmcgui.NOTIFICATION_ERROR,
            10000,
        )
    except Exception as exc:
        _log(f"Auth failure notification failed: {exc}", xbmc.LOGWARNING)


def _get_debrid_accounts():
    """
    Return configured debrid accounts in user-selected priority order.
    Each entry is {"provider": ..., "token": ...}.
    """
    accounts = []
    rd_token = _rd_key()
    ad_token = _ad_key()
    if rd_token:
        accounts.append({"provider": _PROVIDER_RD, "token": rd_token})
    if ad_token:
        accounts.append({"provider": _PROVIDER_AD, "token": ad_token})

    if _setting_int("debrid_provider_order", 0) == 1:
        accounts.sort(key=lambda item: 0 if item["provider"] == _PROVIDER_AD else 1)
    else:
        accounts.sort(key=lambda item: 0 if item["provider"] == _PROVIDER_RD else 1)
    return accounts


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


def _ad_headers(api_token):
    return {"Authorization": f"Bearer {api_token}"}


def _ad_raise_for_payload(payload):
    if isinstance(payload, dict) and payload.get("status") == "success":
        return payload.get("data") or {}
    error = (payload or {}).get("error") if isinstance(payload, dict) else {}
    code = (error or {}).get("code", "")
    message = (error or {}).get("message", "AllDebrid API error")
    if code.startswith("AUTH_"):
        raise PermissionError(code)
    raise RuntimeError(f"{code}: {message}" if code else message)


def _ad_get(path, api_token, timeout=6):
    s = _get_session()
    r = s.get(f"{_AD_BASE}{path}", headers=_ad_headers(api_token), timeout=timeout)
    r.raise_for_status()
    return _ad_raise_for_payload(r.json())


def _ad_post(path, api_token, data=None, timeout=6, base=None):
    s = _get_session()
    r = s.post(
        f"{base or _AD_BASE}{path}",
        headers=_ad_headers(api_token),
        data=data or {},
        timeout=timeout,
    )
    r.raise_for_status()
    return _ad_raise_for_payload(r.json())


def _ad_delete_magnet(magnet_id, api_token):
    if not magnet_id:
        return
    try:
        _ad_post("/magnet/delete", api_token, data={"id": str(magnet_id)}, timeout=5)
    except Exception:
        pass


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


def _episode_file_sort_key(file_info, query_title, season, episode, year=None):
    path = _video_file_path(file_info)
    title_rank = _title_sequence_rank(path, query_title) if query_title else (1, 99)
    if title_rank is None:
        title_rank = (9, 99)
    episode_rank = _episode_match_rank(path, season, episode)
    season_rank = _season_match_rank(path, season)
    year_score = _year_rank(path, year)
    size = _video_file_size(file_info)
    return title_rank + (episode_rank, season_rank, year_score, -size)


def _flatten_ad_files(files, prefix=""):
    flattened = []
    for item in files or []:
        if not isinstance(item, dict):
            continue
        name = item.get("n") or item.get("name") or ""
        path = f"{prefix}/{name}" if prefix and name else (name or prefix)
        if item.get("e"):
            flattened.extend(_flatten_ad_files(item.get("e") or [], path))
            continue
        link = item.get("l") or item.get("link") or ""
        size = item.get("s") or item.get("size") or 0
        if link:
            flattened.append({
                "path": path,
                "filename": name,
                "bytes": size,
                "link": link,
            })
    return flattened


def _pick_best_file(files, candidate, season, episode, query_title=None, year=None):
    video_files = []
    for f in files or []:
        fname = _video_file_path(f).lower()
        if not any(fname.endswith(e) for e in _VIDEO_EXTS):
            continue
        if _skip_advertised_av1_before_resolve(fname):
            continue
        video_files.append(f)

    episode_files = []
    if season and episode:
        episode_files = [
            f for f in video_files
            if _episode_file_matches(f, season, episode)
        ]
        if not episode_files:
            return None

    pool = episode_files or video_files
    if not pool:
        return None

    if season and episode:
        return min(
            pool,
            key=lambda f: (_av1_text_rank(_video_file_path(f)),)
            + _episode_file_sort_key(f, query_title, season, episode, year),
        )
    return min(
        pool,
        key=lambda f: (
            _av1_text_rank(_video_file_path(f)),
            -_video_file_size(f),
            _title_sequence_rank(_video_file_path(f), candidate.get("title", "")) or (9, 99),
        ),
    )


def _try_resolve_one(candidate, api_token, season, episode, cancel_event,
                     query_title=None, year=None, episode_title=None):
    """
    Try to resolve a single candidate hash via RD direct-add.
    Returns a {"url", "headers", "name"} dict on success, None on failure.
    Runs in a worker thread — must be thread-safe.
    """
    h8 = candidate['hash'][:8]
    rd_id = None
    ep_link_idx = 0  # which link index to unrestrict (used for cached season packs)
    source_file = ""
    try:
        if _cancelled(cancel_event):
            return None

        # addMagnet is RD's tightest rate-limited endpoint; keep retries slow.
        magnet = f"magnet:?xt=urn:btih:{candidate['hash']}"
        for attempt in range(len(_RD_ADD_MAGNET_429_BACKOFFS) + 1):
            try:
                resp = _rd_post("/torrents/addMagnet", api_token, data={"magnet": magnet})
                break
            except Exception as e:
                if "429" in str(e) and attempt < len(_RD_ADD_MAGNET_429_BACKOFFS):
                    delay = _RD_ADD_MAGNET_429_BACKOFFS[attempt]
                    _log(f"{h8} RD 429 - backing off {delay:.0f}s before retry", xbmc.LOGWARNING)
                    _time.sleep(delay)
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

        if _skip_advertised_av1_before_resolve(candidate.get("title")):
            _log(f"{h8} skipped AV1 candidate: {candidate.get('title')!r}", xbmc.LOGWARNING)
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            return None

        if _cancelled(cancel_event):
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            return None

        if status == "downloaded":
            # Season pack already cached: find the right link for this episode.
            # RD returns files and links in the same order (selected files only).
            if season and episode:
                files = info.get("files") or []
                selected = [f for f in files if f.get("selected") == 1]
                matches = []
                for idx, f in enumerate(selected):
                    fname = f.get("path", "").lower()
                    if _skip_advertised_av1_before_resolve(fname):
                        continue
                    if _episode_file_matches(f, season, episode):
                        matches.append((idx, f))
                if not matches:
                    _log(f"{h8} no non-AV1 {_episode_label(season, episode)} file in selected season pack")
                    _rd_delete(f"/torrents/delete/{rd_id}", api_token)
                    return None
                if matches:
                    ep_link_idx, best_file = min(
                        matches,
                        key=lambda item: (_av1_text_rank(item[1].get("path", "")),)
                        + _episode_file_sort_key(
                            item[1], query_title, season, episode, year
                        ),
                    )
                    _log(f"{h8} season pack: using link[{ep_link_idx}] "
                         f"for {_episode_label(season, episode)} ({best_file.get('path', '')})")
                    source_file = _video_file_path(best_file)
        elif status == "waiting_files_selection":
            files = info.get("files") or []
            best_file_id = None
            best_file = None
            # First pass: match episode if applicable
            episode_files = []
            for f in files:
                fname = f.get("path", "").lower()
                if not any(fname.endswith(e) for e in _VIDEO_EXTS):
                    continue
                if _skip_advertised_av1_before_resolve(fname):
                    continue
                if season and episode and not _episode_file_matches(f, season, episode):
                    continue
                episode_files.append(f)
            if episode_files:
                best_file = min(
                    episode_files,
                    key=lambda f: (_av1_text_rank(f.get("path", "")),)
                    + _episode_file_sort_key(
                        f, query_title, season, episode, year
                    ),
                )
                best_file_id = best_file.get("id")
                source_file = _video_file_path(best_file)
            elif season and episode:
                _log(f"{h8} no non-AV1 {_episode_label(season, episode)} file in {len(files)} files")
                _rd_delete(f"/torrents/delete/{rd_id}", api_token)
                return None
            # Second pass: any video file
            if not best_file_id:
                best_rank = None
                for f in files:
                    fname = f.get("path", "").lower()
                    fsize = f.get("bytes", 0)
                    if (any(fname.endswith(e) for e in _VIDEO_EXTS)
                            and not _skip_advertised_av1_before_resolve(fname)
                            and (best_rank is None or (_av1_text_rank(fname), -fsize) < best_rank)):
                        best_rank = (_av1_text_rank(fname), -fsize)
                        best_file_id = f.get("id")
                        best_file = f
            if not best_file_id:
                _log(f"{h8} no non-AV1 video file in {len(files)} files")
                _rd_delete(f"/torrents/delete/{rd_id}", api_token)
                return None
            source_file = source_file or _video_file_path(best_file)

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

        av1_probe = _probe_actual_av1_stream(url)
        if av1_probe is True:
            _log(f"{h8} resolved AV1 stream skipped by codec probe: {filename!r}", xbmc.LOGWARNING)
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            rd_id = None
            return None
        if av1_probe is None and (is_av1_stream(filename) or is_av1_stream(url)):
            _log(f"{h8} resolved AV1 stream skipped: {filename!r}", xbmc.LOGWARNING)
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            rd_id = None
            return None

        episode_probe = _probe_episode_identity_stream(
            url, season=season, episode=episode, episode_title=episode_title
        )
        if episode_probe is False:
            _log(
                f"{h8} resolved stream skipped; identity metadata does not match "
                f"{_episode_label(season, episode)}: {filename!r}",
                xbmc.LOGWARNING,
            )
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            rd_id = None
            return None
        if not _episode_stream_identity_is_safe(
            {
                "name": filename,
                "source_file": source_file,
                "episode_identity_probe": "matched" if episode_probe is True else "unknown",
            },
            season=season,
            episode=episode,
            episode_title=episode_title,
        ):
            _log(
                f"{h8} resolved stream skipped; generic/unverified episode "
                f"identity for {_episode_label(season, episode)}: "
                f"{source_file or filename!r}",
                xbmc.LOGWARNING,
            )
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            rd_id = None
            return None

        audio_preference = _preferred_audio_language()
        audio_probe = _probe_audio_language_stream(url, preferred=audio_preference)
        if audio_probe is False:
            _log(
                f"{h8} resolved stream skipped; no preferred audio language "
                f"{audio_preference!r}: {filename!r}",
                xbmc.LOGWARNING,
            )
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            rd_id = None
            return None

        if url:
            url_refreshed = False
            if _actual_av1_probe_enabled() or bool(audio_preference) or episode_probe is not None:
                # Some RD direct URLs do not survive a metadata range probe.
                # Get a fresh playback URL after probing so Kodi opens cleanly.
                fresh = _rd_post("/unrestrict/link", api_token, data={"link": link_to_use})
                url = fresh.get("download") or url
                filename = fresh.get("filename", filename)
                url_refreshed = True
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            rd_id = None
            if av1_probe is False:
                _log(f"{h8} codec probe verified non-AV1: {filename!r}")
            else:
                _log(f"{h8} codec probe inconclusive; allowing: {filename!r}")
            return {
                "url": url,
                "headers": {},
                "name": filename,
                "source_file": source_file,
                "hash": candidate.get("hash"),
                "title": candidate.get("title", ""),
                "pack_scope": candidate.get("pack_scope", ""),
                "pack_rank": candidate.get("pack_rank"),
                "episode_identity_probe": "matched" if episode_probe is True else "unknown",
                "av1_probe": "not_av1" if av1_probe is False else "unknown",
                "audio_language_preference": audio_preference,
                "audio_language_probe": "matched" if audio_probe is True else "unknown",
                "url_refreshed_after_probe": url_refreshed,
                "provider": _PROVIDER_RD,
                "provider_label": _provider_label(_PROVIDER_RD),
            }
        _rd_delete(f"/torrents/delete/{rd_id}", api_token)
        rd_id = None
        return None

    except Exception as exc:
        # 401 means the token is invalid/expired — signal this distinctly so
        # the caller can stop immediately and prompt for re-authorization.
        if "401" in str(exc):
            _log(f"{h8} RD 401 - token rejected", xbmc.LOGWARNING)
            return _RD_AUTH_FAILURE
        if "451" in str(exc):
            _log(f"{h8} RD 451 - marking hash as blocked", xbmc.LOGWARNING)
            if rd_id:
                _rd_delete(f"/torrents/delete/{rd_id}", api_token)
            return {_RD_BLOCKED_HASH_KEY: candidate.get("hash")}
        _log(f"{h8} failed: {exc}", xbmc.LOGWARNING)
        if rd_id:
            _rd_delete(f"/torrents/delete/{rd_id}", api_token)
        return None


def _try_resolve_one_ad(candidate, api_token, season, episode, cancel_event,
                        query_title=None, year=None, episode_title=None):
    """
    Try to resolve a single candidate hash via AllDebrid.
    Returns a playable candidate dict on success, None on failure.
    """
    h8 = candidate['hash'][:8]
    ad_id = None
    try:
        if _cancelled(cancel_event):
            return None

        magnet = f"magnet:?xt=urn:btih:{candidate['hash']}"
        upload = _ad_post(
            "/magnet/upload",
            api_token,
            data=[("magnets[]", magnet)],
            timeout=10,
        )
        magnets = upload.get("magnets") or []
        item = magnets[0] if magnets else {}
        if item.get("error"):
            _log(f"{h8} AD upload error: {item.get('error')}", xbmc.LOGWARNING)
            return None
        ad_id = item.get("id")
        if not ad_id:
            _log(f"{h8} AD upload returned no id")
            return None
        if not item.get("ready"):
            _log(f"{h8} not instantly cached on AD")
            _ad_delete_magnet(ad_id, api_token)
            ad_id = None
            return None

        if _skip_advertised_av1_before_resolve(candidate.get("title")):
            _log(f"{h8} skipped AV1 candidate on AD: {candidate.get('title')!r}", xbmc.LOGWARNING)
            _ad_delete_magnet(ad_id, api_token)
            ad_id = None
            return None

        if _cancelled(cancel_event):
            _ad_delete_magnet(ad_id, api_token)
            ad_id = None
            return None

        files_data = _ad_post(
            "/magnet/files",
            api_token,
            data=[("id[]", str(ad_id))],
            timeout=10,
        )
        magnet_files = files_data.get("magnets") or []
        files_entry = magnet_files[0] if magnet_files else {}
        files = _flatten_ad_files(files_entry.get("files") or [])
        best_file = _pick_best_file(files, candidate, season, episode, query_title, year)
        if not best_file:
            _log(f"{h8} no non-AV1 video file in AD magnet")
            _ad_delete_magnet(ad_id, api_token)
            ad_id = None
            return None

        link_to_use = best_file.get("link")
        source_file = _video_file_path(best_file)
        unlocked = _ad_post("/link/unlock", api_token, data={"link": link_to_use}, timeout=10)
        url = unlocked.get("link")
        filename = unlocked.get("filename") or best_file.get("filename") or candidate.get("title", "Stream")

        av1_probe = _probe_actual_av1_stream(url)
        if av1_probe is True:
            _log(f"{h8} resolved AD AV1 stream skipped by codec probe: {filename!r}", xbmc.LOGWARNING)
            _ad_delete_magnet(ad_id, api_token)
            ad_id = None
            return None
        if av1_probe is None and (is_av1_stream(filename) or is_av1_stream(url)):
            _log(f"{h8} resolved AD AV1 stream skipped: {filename!r}", xbmc.LOGWARNING)
            _ad_delete_magnet(ad_id, api_token)
            ad_id = None
            return None

        episode_probe = _probe_episode_identity_stream(
            url, season=season, episode=episode, episode_title=episode_title
        )
        if episode_probe is False:
            _log(
                f"{h8} resolved AD stream skipped; identity metadata does not match "
                f"{_episode_label(season, episode)}: {filename!r}",
                xbmc.LOGWARNING,
            )
            _ad_delete_magnet(ad_id, api_token)
            ad_id = None
            return None
        if not _episode_stream_identity_is_safe(
            {
                "name": filename,
                "source_file": source_file,
                "episode_identity_probe": "matched" if episode_probe is True else "unknown",
            },
            season=season,
            episode=episode,
            episode_title=episode_title,
        ):
            _log(
                f"{h8} resolved AD stream skipped; generic/unverified episode "
                f"identity for {_episode_label(season, episode)}: "
                f"{source_file or filename!r}",
                xbmc.LOGWARNING,
            )
            _ad_delete_magnet(ad_id, api_token)
            ad_id = None
            return None

        audio_preference = _preferred_audio_language()
        audio_probe = _probe_audio_language_stream(url, preferred=audio_preference)
        if audio_probe is False:
            _log(
                f"{h8} resolved AD stream skipped; no preferred audio language "
                f"{audio_preference!r}: {filename!r}",
                xbmc.LOGWARNING,
            )
            _ad_delete_magnet(ad_id, api_token)
            ad_id = None
            return None

        if url:
            url_refreshed = False
            if _actual_av1_probe_enabled() or bool(audio_preference) or episode_probe is not None:
                fresh = _ad_post("/link/unlock", api_token, data={"link": link_to_use}, timeout=10)
                url = fresh.get("link") or url
                filename = fresh.get("filename", filename)
                url_refreshed = True
            # Keep the AllDebrid magnet while Kodi opens the unlocked CDN URL.
            # Deleting it here can invalidate the freshly unlocked link before
            # VideoPlayer starts, producing an immediate OpenInputStream error.
            if av1_probe is False:
                _log(f"{h8} AD codec probe verified non-AV1: {filename!r}")
            else:
                _log(f"{h8} AD codec probe inconclusive; allowing: {filename!r}")
            return {
                "url": url,
                "headers": {},
                "name": filename,
                "source_file": source_file,
                "hash": candidate.get("hash"),
                "title": candidate.get("title", ""),
                "pack_scope": candidate.get("pack_scope", ""),
                "pack_rank": candidate.get("pack_rank"),
                "episode_identity_probe": "matched" if episode_probe is True else "unknown",
                "av1_probe": "not_av1" if av1_probe is False else "unknown",
                "audio_language_preference": audio_preference,
                "audio_language_probe": "matched" if audio_probe is True else "unknown",
                "url_refreshed_after_probe": url_refreshed,
                "provider": _PROVIDER_AD,
                "provider_label": _provider_label(_PROVIDER_AD),
            }
        _ad_delete_magnet(ad_id, api_token)
        ad_id = None
        return None

    except PermissionError as exc:
        _log(f"{h8} AD auth rejected: {exc}", xbmc.LOGWARNING)
        if ad_id:
            _ad_delete_magnet(ad_id, api_token)
        return _AUTH_FAILURE
    except Exception as exc:
        _log(f"{h8} AD failed: {exc}", xbmc.LOGWARNING)
        if ad_id:
            _ad_delete_magnet(ad_id, api_token)
        return None


def _resolve_by_direct_add(candidates_info, api_token, season=None, episode=None,
                           max_resolve=1, cancel_event=None, query_title=None,
                           year=None, blocked_hashes=None, provider=_PROVIDER_RD,
                           episode_title=None):
    """
    Resolve streams by adding magnets to a debrid provider and checking cache.
    Runs candidates in small batches to avoid provider rate-limits,
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

    batch_size = _RD_ADD_MAGNET_BATCH_SIZE if provider == _PROVIDER_RD else 3
    short = _provider_short(provider)
    resolver = _try_resolve_one_ad if provider == _PROVIDER_AD else _try_resolve_one
    _log(f"Resolving {len(candidates_info)} candidates on {short} in batches of {batch_size} (need {max_resolve})")

    # RD often rate-limits addMagnet when probed in parallel, especially after
    # many 451 responses, so this intentionally defaults to serial probing.
    for batch_start in range(0, len(candidates_info), batch_size):
        if _cancelled(cancel_event) or enough_event.is_set():
            break

        batch = candidates_info[batch_start:batch_start + batch_size]
        _log(f"{short} batch {batch_start // batch_size + 1}: candidates {batch_start + 1}-{batch_start + len(batch)}")

        pool = ThreadPoolExecutor(max_workers=batch_size)
        futures = {
            pool.submit(
                resolver, c, api_token, season, episode, combined,
                query_title, year, episode_title
            ): c
            for c in batch
        }
        try:
            for future in as_completed(futures):
                result = future.result()
                if result is _AUTH_FAILURE:
                    # Count auth failures; if the whole first batch fails with 401
                    # the token is bad — abort immediately and signal re-auth needed.
                    pass  # counted below after batch drains
                elif isinstance(result, dict) and result.get(_RD_BLOCKED_HASH_KEY):
                    if blocked_hashes is not None:
                        blocked_hashes.append(result[_RD_BLOCKED_HASH_KEY])
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
        if non_none and all(r is _AUTH_FAILURE for r in non_none):
            _log(f"All {short} calls returned auth failure – token is invalid", xbmc.LOGWARNING)
            raise PermissionError(f"{provider}_token_rejected")

        if resolved:
            break

        if enough_event.is_set():
            break

        # Stagger addMagnet calls to stay below provider burst throttles.
        if batch_start + batch_size < len(candidates_info):
            _time.sleep(_RD_ADD_MAGNET_BATCH_PAUSE if provider == _PROVIDER_RD else 0.2)

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


def fetch_all_cached_streams(catalog_type, video_id, cancel_event=None,
                             query_title=None, year=None, userdata_path=None,
                             ignore_pack_binding=False, episode_title=None):
    """
    Main entry point.  Queries DMM's hash database, then resolves cached
    streams via configured debrid providers.  Returns a sorted list of
    {"url", "headers", "name"} candidates.

    Note: RD's instantAvailability endpoint is permanently disabled
    (error_code 37), so RD uses direct-add. AllDebrid also uses direct-add
    because upload returns whether the magnet is ready.

    catalog_type: "movie" or "series"
    video_id:     "tt1234567" for movies, "tt1234567:1:2" for episodes
    """
    debrid_accounts = _get_debrid_accounts()
    if not debrid_accounts:
        _log("No debrid access token — authorization required", xbmc.LOGWARNING)
        raise PermissionError("no_debrid_token")
    api_token = debrid_accounts[0]["token"]

    # Parse video_id: for series it's "imdb:season:episode"
    parts = video_id.split(":")
    imdb_id = parts[0]
    season = parts[1] if len(parts) > 1 else None
    episode = parts[2] if len(parts) > 2 else None
    strict_matching, tv_pack_preference = _get_matching_preferences()
    pack_cache = None
    blocked_cache = None
    if userdata_path:
        try:
            from cache import BlockedHashCache
            blocked_cache = BlockedHashCache(userdata_path)
        except Exception as exc:
            _log(f"Blocked hash cache unavailable: {exc}", xbmc.LOGWARNING)

    if catalog_type != "movie" and userdata_path:
        try:
            from cache import PackBindingCache
            pack_cache = PackBindingCache(userdata_path)
        except Exception as exc:
            _log(f"Pack binding cache unavailable: {exc}", xbmc.LOGWARNING)

    if pack_cache and not ignore_pack_binding:
        binding = pack_cache.get(imdb_id, season)
        if binding:
            if _skip_advertised_av1_before_resolve(binding.get("title")):
                _log(f"Clearing AV1 bound pack for {imdb_id} S{int(season):02d}: "
                     f"{binding.get('title')!r}", xbmc.LOGWARNING)
                pack_cache.clear(imdb_id, season)
                binding = None
        if binding:
            bound_candidate = {
                "hash": (binding.get("hash") or "").lower(),
                "title": binding.get("title") or "Bound pack",
                "pack_scope": binding.get("scope") or "season",
                "pack_rank": 1,
            }
            if len(bound_candidate["hash"]) == 40:
                for account in debrid_accounts:
                    provider = account["provider"]
                    if provider == _PROVIDER_RD and blocked_cache and blocked_cache.is_blocked(bound_candidate["hash"]):
                        _log(f"Bound pack is RD-451 blocked; skipping RD for {imdb_id} S{int(season):02d}",
                             xbmc.LOGWARNING)
                        continue
                    blocked_hashes = []
                    _log(f"Trying bound pack on {_provider_short(provider)} for {imdb_id} S{int(season):02d}: "
                         f"{bound_candidate['hash'][:8]} {bound_candidate['title']!r}")
                    try:
                        resolved = _resolve_by_direct_add(
                            [bound_candidate], account["token"], season=season, episode=episode,
                            max_resolve=1, cancel_event=cancel_event,
                            query_title=query_title, year=year, blocked_hashes=blocked_hashes,
                            provider=provider, episode_title=episode_title,
                        )
                    except PermissionError as exc:
                        _log(f"{_provider_short(provider)} auth failed while trying bound pack: {exc}",
                             xbmc.LOGWARNING)
                        _notify_provider_auth_failure(provider, str(exc))
                        continue
                    if blocked_cache and blocked_hashes:
                        blocked_cache.mark_many(blocked_hashes)
                    if resolved:
                        return resolved
                _log(f"Bound pack failed for {imdb_id} S{int(season):02d}; clearing binding",
                     xbmc.LOGWARNING)
                pack_cache.clear(imdb_id, season)

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
            "KDMM", "DMM lookup failed",
            xbmcgui.NOTIFICATION_ERROR, 8000)
        return []

    if not dmm_results:
        _log(f"No torrents in DMM database for {video_id}")
        xbmcgui.Dialog().notification(
            "KDMM", "No torrents found",
            xbmcgui.NOTIFICATION_WARNING, 5000)
        return []

    if catalog_type != "movie":
        dmm_results = _filter_tv_results(
            dmm_results, query_title, season, episode, strict=strict_matching
        )
    else:
        dmm_results = _filter_movie_results(
            dmm_results, query_title, year, strict=strict_matching
        )

    dmm_results = _filter_av1_results(dmm_results)

    if not dmm_results:
        xbmcgui.Dialog().notification(
            "KDMM",
            "No playable streams",
            xbmcgui.NOTIFICATION_WARNING, 6000)
        return []

    # Build hash → result map
    hash_map = {}
    for r in dmm_results:
        h = r.get("hash", "").lower()
        if h and len(h) == 40:
            hash_map[h] = r

    if not hash_map:
        _log("No valid hashes from DMM results")
        xbmcgui.Dialog().notification(
            "KDMM", "No playable streams",
            xbmcgui.NOTIFICATION_WARNING, 5000)
        return []

    # 2. Sort candidates using quality preferences from settings
    preferred_groups, hdr_pref, res_pref, src_pref = _get_quality_preferences()
    sort_key = _build_sort_key(preferred_groups, hdr_pref, res_pref, src_pref)

    if catalog_type != "movie":
        sorted_dmm = sorted(
            dmm_results,
            key=_build_tv_sort_key(
                sort_key, query_title, season, episode, tv_pack_preference,
                episode_title=episode_title,
            )
        )
    else:
        sorted_dmm = sorted(dmm_results, key=_build_movie_sort_key(sort_key, query_title, year))

    # Log the top picks so user can verify ranking
    for i, r in enumerate(sorted_dmm[:5]):
        parsed = _parse_title(r.get("title", ""))
        extra = ""
        if catalog_type != "movie":
            title_rank = _title_sequence_rank(r.get("title", ""), query_title) or (9, 99)
            episode_rank = _episode_match_rank(r.get("title", ""), season, episode)
            season_rank = _season_match_rank(r.get("title", ""), season)
            pack_rank, pack_scope = _tv_pack_rank(r, season, episode)
            extra = f" match={title_rank} pack={pack_rank}:{pack_scope} ep={episode_rank} season={season_rank}"
        else:
            extra = f" match={_title_sequence_rank(r.get('title', ''), query_title) or (9, 99)} year={_year_rank(r.get('title', ''), year)}"
        _log(f"  #{i+1}: {r.get('title','?')[:80]} "
             f"[hdr={parsed['hdr']} res={parsed['res']} src={parsed['src']} grp={parsed['group']}{extra}]")

    primary_candidates = []
    fallback_candidates = []
    for r in sorted_dmm:
        h = (r.get("hash") or "").lower()
        if len(h) != 40:
            continue
        candidate = {"hash": h, "title": r.get("title", "Unknown")}
        if catalog_type != "movie":
            pack_rank, pack_scope = _tv_pack_rank(r, season, episode)
            candidate["pack_rank"] = pack_rank
            candidate["pack_scope"] = pack_scope
        if is_av1_stream(candidate.get("title")):
            fallback_candidates.append(candidate)
        else:
            primary_candidates.append(candidate)

    candidates = primary_candidates + fallback_candidates
    if fallback_candidates:
        _log(
            f"Deferring {len(fallback_candidates)} title-advertised AV1 candidate(s) "
            "until non-advertised candidates are exhausted"
        )

    provider_names = ", ".join(_provider_short(a["provider"]) for a in debrid_accounts)
    _log(f"DMM returned {len(hash_map)} hashes, checking {len(candidates)} candidate(s) on {provider_names}")

    resolved = []
    page_size = 20
    auth_failed_providers = set()
    for page_start in range(0, len(candidates), page_size):
        if _cancelled(cancel_event):
            break

        page = candidates[page_start:page_start + page_size]
        _log(f"Debrid search page {page_start // page_size + 1}: "
             f"candidates {page_start + 1}-{page_start + len(page)}")
        for account in debrid_accounts:
            provider = account["provider"]
            provider_page = page
            if provider == _PROVIDER_RD and blocked_cache:
                before = len(provider_page)
                provider_page = [
                    c for c in provider_page
                    if not blocked_cache.is_blocked(c.get("hash"))
                ]
                skipped = before - len(provider_page)
                if skipped:
                    _log(f"Skipped {skipped} previously RD-451-blocked hash(es)")
            if not provider_page:
                continue
            blocked_hashes = []
            try:
                resolved = _resolve_by_direct_add(
                    provider_page, account["token"], season=season, episode=episode,
                    max_resolve=3, cancel_event=cancel_event,
                    query_title=query_title, year=year,
                    blocked_hashes=blocked_hashes,
                    provider=provider, episode_title=episode_title,
                )
            except PermissionError as exc:
                _log(f"{_provider_short(provider)} auth failed; trying next provider: {exc}",
                     xbmc.LOGWARNING)
                auth_failed_providers.add(provider)
                _notify_provider_auth_failure(provider, str(exc))
                continue
            if blocked_cache and blocked_hashes:
                blocked_cache.mark_many(blocked_hashes)
                _log(f"Marked {len(blocked_hashes)} RD-451-blocked hash(es)")
            if resolved:
                break
        if resolved:
            break
        if len(auth_failed_providers) == len(debrid_accounts):
            raise PermissionError("all_debrid_tokens_rejected")

    if not resolved:
        xbmcgui.Dialog().notification(
            "KDMM", "No playable streams",
            xbmcgui.NOTIFICATION_WARNING, 6000)
    else:
        top = resolved[0]
        source = top.get("source_file") or top.get("title") or top["name"]
        _log(
            f"Resolved {len(resolved)} stream(s), top: {top['name']!r} "
            f"via {top.get('provider_label', 'debrid')} "
            f"from {source!r} ({(top.get('hash') or '')[:8]})"
        )
        if pack_cache and top.get("pack_rank") in (0, 1):
            pack_cache.set(
                imdb_id, season, top.get("hash"), title=top.get("title", ""),
                scope=top.get("pack_scope", "season"),
            )
            _log(f"Bound {imdb_id} S{int(season):02d} to "
                 f"{(top.get('hash') or '')[:8]} ({top.get('pack_scope', 'season')})")

    return resolved
