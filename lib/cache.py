"""
KDMM – lib/cache.py
Persistent JSON caches for:
  • StreamCache   – resolved stream URL + header map, keyed by media_id.
  • ProgressCache – resume time in seconds, keyed by media_id.
  • UserBlockedSourceCache – torrent hashes the user blocked by hand.
  • LastPlayedSource – the source behind the most recent play request.

media_id convention:
  movie   → "tt1234567"
  episode → "tt1234567:1:2"   (imdb:season:episode)
"""

import json
import os
import time

# A stream found ahead of time (see prewarm.py) holds a debrid URL that was
# never opened. Past this age a Real-Debrid entry is looked up again at play
# time instead of handing Kodi a link that may have expired.
PREWARM_MAX_AGE_SECONDS = 30 * 3600


def _write_json(path, data):
    """Write through a temp file so a reader never sees a half-written cache."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = f"{path}.{os.getpid()}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    os.replace(tmp_path, path)


class StreamCache:
    """Cache for resolved stream URLs so we don't re-query every play."""

    DEFAULT_TTL = 14 * 24 * 3600

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
        _write_json(self._path, self._data)

    def _reload(self):
        # The plugin and the service each hold their own copy of this file.
        # Re-read it before every change so one never drops the other's entries.
        self._data = self._load()

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

    def prewarm_age(self, media_id):
        """Seconds since this entry was found ahead of time, else None."""
        entry = self._data.get(media_id) or {}
        if not entry.get("prewarmed"):
            return None
        return time.time() - entry.get("timestamp", 0)

    def set(self, media_id, candidates, label=None, prewarmed=False):
        self._reload()
        if label is None:
            label = (self._data.get(media_id) or {}).get("label", "")
        entry = {
            "candidates": candidates,
            "label": label or "",
            "timestamp": time.time(),
        }
        if prewarmed:
            entry["prewarmed"] = True
        self._data[media_id] = entry
        self._save()

    def clear(self, media_id=None):
        self._reload()
        if media_id:
            self._data.pop(media_id, None)
        else:
            self._data.clear()
        self._save()

    def entries(self):
        """Every cached item, newest first, for the manage-cache list."""
        items = []
        for media_id, entry in self._data.items():
            candidates = entry.get("candidates") or []
            first = candidates[0] if candidates and isinstance(candidates[0], dict) else {}
            items.append({
                "media_id": media_id,
                "label": entry.get("label") or "",
                "file": first.get("name") or "",
                "timestamp": entry.get("timestamp", 0),
            })
        items.sort(key=lambda item: item["timestamp"], reverse=True)
        return items

    def clear_many(self, media_ids):
        self._reload()
        removed = 0
        for media_id in media_ids or []:
            if self._data.pop(media_id, None) is not None:
                removed += 1
        if removed:
            self._save()
        return removed

    def clear_hash(self, torrent_hash):
        """Drop every entry that plays from this torrent; return the count."""
        torrent_hash = (torrent_hash or "").lower()
        if not torrent_hash:
            return 0
        self._reload()
        stale = [
            media_id for media_id, entry in self._data.items()
            if any(
                isinstance(candidate, dict)
                and (candidate.get("hash") or "").lower() == torrent_hash
                for candidate in (entry.get("candidates") or [])
            )
        ]
        for media_id in stale:
            self._data.pop(media_id, None)
        if stale:
            self._save()
        return len(stale)


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
        _write_json(self._path, self._data)

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

    def clear_hash(self, torrent_hash):
        """Drop every season bound to this torrent; return the count."""
        torrent_hash = (torrent_hash or "").lower()
        if not torrent_hash:
            return 0
        stale = [
            key for key, entry in self._data.items()
            if (entry.get("hash") or "").lower() == torrent_hash
        ]
        for key in stale:
            self._data.pop(key, None)
        if stale:
            self._save()
        return len(stale)


class BlockedHashCache:
    """Cache torrent hashes that RD rejects with a hard legal block."""

    DEFAULT_TTL = 30 * 24 * 3600

    def __init__(self, userdata_path, ttl=None):
        self._path = os.path.join(userdata_path, "blocked_hash_cache.json")
        self._ttl = ttl if ttl is not None else self.DEFAULT_TTL
        self._data = self._load()
        self._prune()

    def _load(self):
        if os.path.isfile(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception:
                pass
        return {}

    def _save(self):
        _write_json(self._path, self._data)

    def _prune(self):
        now = time.time()
        changed = False
        for torrent_hash, entry in list(self._data.items()):
            if now - entry.get("timestamp", 0) > self._ttl:
                self._data.pop(torrent_hash, None)
                changed = True
        if changed:
            self._save()

    def is_blocked(self, torrent_hash):
        torrent_hash = (torrent_hash or "").lower()
        return bool(torrent_hash and torrent_hash in self._data)

    def mark(self, torrent_hash, reason="rd_451"):
        torrent_hash = (torrent_hash or "").lower()
        if not torrent_hash:
            return
        self._data[torrent_hash] = {
            "reason": reason,
            "timestamp": time.time(),
        }
        self._save()

    def mark_many(self, torrent_hashes, reason="rd_451"):
        changed = False
        now = time.time()
        for torrent_hash in torrent_hashes or []:
            torrent_hash = (torrent_hash or "").lower()
            if not torrent_hash:
                continue
            self._data[torrent_hash] = {
                "reason": reason,
                "timestamp": now,
            }
            changed = True
        if changed:
            self._save()

    def filter_unblocked(self, candidates):
        return [
            candidate for candidate in candidates
            if not self.is_blocked(candidate.get("hash"))
        ]


class UserBlockedSourceCache:
    """Torrent hashes the user blocked by hand; kept until they unblock them."""

    def __init__(self, userdata_path):
        self._path = os.path.join(userdata_path, "user_blocked_sources.json")
        self._data = self._load()

    def _load(self):
        if os.path.isfile(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
        return {}

    def _save(self):
        _write_json(self._path, self._data)

    def __len__(self):
        return len(self._data)

    def is_blocked(self, torrent_hash):
        torrent_hash = (torrent_hash or "").lower()
        return bool(torrent_hash and torrent_hash in self._data)

    def block(self, torrent_hash, name="", media_id="", label=""):
        torrent_hash = (torrent_hash or "").lower()
        if not torrent_hash:
            return
        self._data[torrent_hash] = {
            "name": name or "",
            "media_id": media_id or "",
            "label": label or "",
            "timestamp": time.time(),
        }
        self._save()

    def entries(self):
        """Every blocked source, newest first, for the manage-blocks list."""
        items = [
            {
                "hash": torrent_hash,
                "name": entry.get("name") or "",
                "label": entry.get("label") or "",
                "media_id": entry.get("media_id") or "",
                "timestamp": entry.get("timestamp", 0),
            }
            for torrent_hash, entry in self._data.items()
        ]
        items.sort(key=lambda item: item["timestamp"], reverse=True)
        return items

    def unblock_many(self, torrent_hashes):
        removed = 0
        for torrent_hash in torrent_hashes or []:
            if self._data.pop((torrent_hash or "").lower(), None) is not None:
                removed += 1
        if removed:
            self._save()
        return removed

    def clear(self):
        self._data.clear()
        self._save()


class LastPlayedSource:
    """Remembers which source the most recent play request used."""

    def __init__(self, userdata_path):
        self._path = os.path.join(userdata_path, "last_played_source.json")

    def get(self):
        if os.path.isfile(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                if isinstance(data, dict) and data.get("hash"):
                    return data
            except Exception:
                pass
        return None

    def set(self, media_id, candidate, label=""):
        torrent_hash = ((candidate or {}).get("hash") or "").lower()
        if not torrent_hash:
            return
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as fh:
            json.dump({
                "media_id": media_id,
                "hash": torrent_hash,
                "label": label or "",
                "release": candidate.get("title") or "",
                "file": candidate.get("name") or "",
                "provider": candidate.get("provider_label") or "",
                "timestamp": time.time(),
            }, fh, indent=2)

    def clear(self):
        try:
            os.remove(self._path)
        except OSError:
            pass


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
        _write_json(self._path, self._data)

    def is_watched(self, media_id):
        return bool((self._data.get(media_id) or {}).get("watched"))

    def episodes_for(self, imdb_id):
        """Every stored episode of a show as (season, episode, entry)."""
        prefix = f"{imdb_id}:"
        found = []
        for media_id, entry in self._data.items():
            if not media_id.startswith(prefix) or not isinstance(entry, dict):
                continue
            parts = media_id.split(":")
            try:
                found.append((int(parts[1]), int(parts[2]), entry))
            except (IndexError, ValueError):
                continue
        return found

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
