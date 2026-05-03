"""
KDMM – lib/cache.py
Persistent JSON caches for:
  • StreamCache   – resolved stream URL + header map, keyed by media_id.
  • ProgressCache – resume time in seconds, keyed by media_id.

media_id convention:
  movie   → "tt1234567"
  episode → "tt1234567:1:2"   (imdb:season:episode)
"""

import json
import os
import time


class StreamCache:
    """Cache for resolved stream URLs so we don't re-query every play."""

    DEFAULT_TTL = 6 * 3600

    def __init__(self, userdata_path, ttl=None):
        self._path = os.path.join(userdata_path, "stream_cache.json")
        self._ttl = ttl if ttl is not None else self.DEFAULT_TTL
        self._data = self._load()

    def _load(self):
        if os.path.isfile(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception:
                pass
        return {}

    def _save(self):
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh, indent=2)

    def get(self, media_id):
        entry = self._data.get(media_id)
        if not entry:
            return None
        age = time.time() - entry.get("timestamp", 0)
        if age > self._ttl:
            return None
        candidates = entry.get("candidates")
        if not candidates:
            return None
        return candidates

    def set(self, media_id, candidates):
        self._data[media_id] = {
            "candidates": candidates,
            "timestamp": time.time(),
        }
        self._save()

    def clear(self, media_id=None):
        if media_id:
            self._data.pop(media_id, None)
        else:
            self._data.clear()
        self._save()


class PackBindingCache:
    """Cache the preferred torrent hash for a show season."""

    DEFAULT_TTL = 30 * 24 * 3600

    def __init__(self, userdata_path, ttl=None):
        self._path = os.path.join(userdata_path, "pack_binding_cache.json")
        self._ttl = ttl if ttl is not None else self.DEFAULT_TTL
        self._data = self._load()

    def _load(self):
        if os.path.isfile(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception:
                pass
        return {}

    def _save(self):
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh, indent=2)

    def _key(self, imdb_id, season):
        if not imdb_id or not season:
            return None
        return f"{imdb_id}:{int(season)}"

    def get(self, imdb_id, season):
        key = self._key(imdb_id, season)
        if not key:
            return None
        entry = self._data.get(key)
        if not entry:
            return None
        age = time.time() - entry.get("timestamp", 0)
        if age > self._ttl:
            self._data.pop(key, None)
            self._save()
            return None
        if not entry.get("hash"):
            return None
        return entry

    def set(self, imdb_id, season, torrent_hash, title="", scope="", file_map=None):
        key = self._key(imdb_id, season)
        if not key or not torrent_hash:
            return
        self._data[key] = {
            "hash": torrent_hash,
            "title": title or "",
            "scope": scope or "",
            "file_map": file_map or {},
            "timestamp": time.time(),
        }
        self._save()

    def clear(self, imdb_id=None, season=None):
        key = self._key(imdb_id, season)
        if key:
            self._data.pop(key, None)
        elif imdb_id:
            prefix = f"{imdb_id}:"
            for existing in list(self._data.keys()):
                if existing.startswith(prefix):
                    self._data.pop(existing, None)
        else:
            self._data.clear()
        self._save()


class ProgressCache:
    """Cache for per-item resume positions (seconds)."""

    def __init__(self, userdata_path):
        self._path = os.path.join(userdata_path, "progress_cache.json")
        self._data = self._load()

    def _load(self):
        if os.path.isfile(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception:
                pass
        return {}

    def _save(self):
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh, indent=2)

    def get_resume_time(self, media_id):
        entry = self._data.get(media_id, {})
        if entry.get("watched"):
            return 0.0
        return float(entry.get("resume_time", 0.0))

    def set_progress(self, media_id, resume_time, total_time=0.0, watched=False):
        self._data[media_id] = {
            "resume_time": resume_time,
            "total_time": total_time,
            "watched": watched,
            "updated": time.time(),
        }
        self._save()
