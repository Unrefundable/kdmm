"""
KDMM - lib/ad_auth.py
AllDebrid PIN authorization and API-key lookup.
"""

import json
import os
import sys
import threading
import time

import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

_AD_BASE = "https://api.alldebrid.com/v4"
_AD_BASE_41 = "https://api.alldebrid.com/v4.1"
_ADDON_ID = "plugin.video.kdmm"
_SETTING_API_KEY = "ad_api_key"
_TOKEN_API_KEY = "apikey"


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM AllDebrid Auth] {msg}", level)


def _tokens_path():
    return f"special://profile/addon_data/{_ADDON_ID}/ad_tokens.json"


def _load_tokens():
    try:
        path = _tokens_path()
        if not xbmcvfs.exists(path):
            return {}
        real_path = xbmcvfs.translatePath(path)
        with open(real_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        _log(f"Failed to load tokens: {exc}", xbmc.LOGWARNING)
        return {}


def _write_tokens(data):
    try:
        dir_path = f"special://profile/addon_data/{_ADDON_ID}/"
        if not xbmcvfs.exists(dir_path):
            xbmcvfs.mkdirs(dir_path)
        real_path = xbmcvfs.translatePath(_tokens_path())
        with open(real_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        _log(f"Tokens saved to {real_path}")
    except Exception as exc:
        _log(f"Failed to save tokens: {exc}", xbmc.LOGERROR)


def _setting_api_key():
    try:
        return xbmcaddon.Addon(_ADDON_ID).getSetting(_SETTING_API_KEY).strip()
    except Exception as exc:
        _log(f"Could not read addon settings: {exc}", xbmc.LOGWARNING)
        return ""


def _set_setting_api_key(api_key):
    try:
        xbmcaddon.Addon(_ADDON_ID).setSetting(_SETTING_API_KEY, api_key or "")
        return True
    except Exception as exc:
        _log(f"Could not write addon settings: {exc}", xbmc.LOGWARNING)
        return False


def _save_api_key(api_key, source="pin"):
    api_key = (api_key or "").strip()
    if not api_key:
        return False
    _write_tokens({
        _TOKEN_API_KEY: api_key,
        "authorized_at": int(time.time()),
        "source": source,
    })
    _set_setting_api_key(api_key)
    return True


def _get_requests():
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


def _api_status_ok(payload):
    return isinstance(payload, dict) and payload.get("status") == "success"


def authorize():
    """
    Run the AllDebrid PIN flow.
    Returns True on success, False on cancel/error.
    """
    busy = xbmcgui.DialogProgress()
    busy.create("KDMM - AllDebrid", "Connecting to AllDebrid...")

    fetch_result = {}

    def _fetch_pin():
        try:
            requests = _get_requests()
            resp = requests.get(f"{_AD_BASE_41}/pin/get", timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if not _api_status_ok(data):
                raise RuntimeError(data.get("error", {}).get("message", "PIN request failed"))
            fetch_result["data"] = data.get("data") or {}
        except Exception as exc:
            fetch_result["error"] = exc

    t = threading.Thread(target=_fetch_pin, daemon=True)
    t.start()
    while t.is_alive():
        if busy.iscanceled():
            busy.close()
            return False
        busy.update(0, "Connecting to AllDebrid...")
        xbmc.sleep(500)
    busy.close()

    if "error" in fetch_result:
        exc = fetch_result["error"]
        _log(f"Failed to get PIN: {exc}", xbmc.LOGERROR)
        xbmcgui.Dialog().ok("KDMM", f"AllDebrid error: {type(exc).__name__}: {str(exc)[:200]}")
        return False

    data = fetch_result.get("data", {})
    pin = data.get("pin", "")
    check = data.get("check", "")
    user_url = data.get("user_url") or "https://alldebrid.com/pin/"
    expires_in = int(data.get("expires_in") or 600)
    if not pin or not check:
        xbmcgui.Dialog().ok("KDMM", "Unexpected response from AllDebrid.")
        return False

    return _show_auth_dialog(pin, check, user_url, expires_in)


def _show_auth_dialog(pin, check, user_url, expires_in):
    result = {"status": "pending"}
    result_lock = threading.Lock()

    def _poll_thread():
        requests = _get_requests()
        deadline = time.time() + expires_in
        while time.time() < deadline:
            with result_lock:
                if result["status"] != "pending":
                    return
            time.sleep(5)
            try:
                resp = requests.post(
                    f"{_AD_BASE}/pin/check",
                    data={"pin": pin, "check": check},
                    timeout=10,
                )
                resp.raise_for_status()
                payload = resp.json()
            except Exception as exc:
                _log(f"PIN poll failed: {exc}", xbmc.LOGWARNING)
                continue

            if not _api_status_ok(payload):
                code = (payload.get("error") or {}).get("code", "")
                if code in ("PIN_EXPIRED", "PIN_INVALID"):
                    with result_lock:
                        result["status"] = "timeout"
                    return
                continue

            data = payload.get("data") or {}
            if data.get("activated") and data.get("apikey"):
                _save_api_key(data.get("apikey", ""), source="pin")
                with result_lock:
                    result["status"] = "ok"
                return

        with result_lock:
            if result["status"] == "pending":
                result["status"] = "timeout"

    threading.Thread(target=_poll_thread, daemon=True).start()

    dialog = xbmcgui.DialogProgress()
    dialog.create(
        "KDMM - Link AllDebrid",
        f"Go to: [B]{user_url}[/B]\n"
        f"Enter PIN: [B]{pin}[/B]"
    )

    deadline = time.time() + expires_in
    while True:
        with result_lock:
            status = result["status"]
        if status != "pending":
            break
        if dialog.iscanceled():
            with result_lock:
                result["status"] = "cancelled"
            break
        remaining = max(0, int(deadline - time.time()))
        elapsed = expires_in - remaining
        percent = min(99, int((elapsed / expires_in) * 100))
        dialog.update(
            percent,
            f"Go to: [B]{user_url}[/B]\n"
            f"Enter PIN: [B]{pin}[/B]\n"
            f"Waiting... ({remaining}s remaining)"
        )
        xbmc.sleep(1000)

    dialog.close()
    with result_lock:
        status = result["status"]

    if status == "ok":
        xbmcgui.Dialog().notification("KDMM", "AllDebrid authorized!",
                                      xbmcgui.NOTIFICATION_INFO, 3000)
        _log("Authorization successful")
        return True
    if status == "cancelled":
        _log("User cancelled authorization")
        return False
    if status == "timeout":
        xbmcgui.Dialog().ok("KDMM", "AllDebrid authorization timed out. Please try again.")
        return False
    xbmcgui.Dialog().ok("KDMM", "Failed to get API key from AllDebrid.")
    return False


def get_access_token():
    """
    Return an AllDebrid API key from settings or the PIN-flow token store.
    """
    api_key = _setting_api_key()
    if api_key:
        _log("Using API key from addon settings")
        return api_key

    stored_key = (_load_tokens().get(_TOKEN_API_KEY) or "").strip()
    if stored_key:
        _log("Restoring AllDebrid API key from token store into addon settings")
        _set_setting_api_key(stored_key)
        return stored_key
    return None


def revoke():
    _write_tokens({})
    _set_setting_api_key("")
    _log("AllDebrid authorization revoked")
