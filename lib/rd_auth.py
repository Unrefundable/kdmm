"""
KDMM – lib/rd_auth.py
Real-Debrid OAuth2 device-code authorization flow.

Uses the open-source client ID that community addons share.
Shows a QR code + user code dialog, polls for authorization,
then stores the resulting tokens in a JSON file in addon_data
(survives addon reinstalls/updates).
"""

import json
import os
import sys
import time

import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs

_CLIENT_ID = "X245A4XAIBGVM"
_RD_OAUTH_BASE = "https://api.real-debrid.com/oauth/v2"
_ADDON_ID = "plugin.video.kdmm"


def _log(msg, level=xbmc.LOGINFO):
    xbmc.log(f"[KDMM Auth] {msg}", level)


def _tokens_path():
    """Path to the persistent tokens JSON file (in userdata, not addon dir)."""
    userdata = xbmcvfs.translatePath(f"special://profile/addon_data/{_ADDON_ID}/")
    os.makedirs(userdata, exist_ok=True)
    return os.path.join(userdata, "rd_tokens.json")


def _load_tokens():
    """Load tokens dict from JSON file, or empty dict if missing/corrupt."""
    try:
        with open(_tokens_path(), "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_tokens(data):
    """Write tokens dict to JSON file."""
    try:
        with open(_tokens_path(), "w") as f:
            json.dump(data, f)
        _log("Tokens saved to rd_tokens.json")
    except Exception as exc:
        _log(f"Failed to save tokens: {exc}", xbmc.LOGERROR)


def _get_requests():
    """Import requests, ensuring all Kodi addon module paths are on sys.path."""
    addon_dir = xbmcvfs.translatePath("special://home/addons")
    for mod in ("script.module.requests", "script.module.urllib3",
                "script.module.chardet", "script.module.certifi",
                "script.module.idna"):
        lib = os.path.join(addon_dir, mod, "lib")
        if os.path.isdir(lib) and lib not in sys.path:
            sys.path.insert(0, lib)
    import requests
    # Point requests at Kodi's certifi CA bundle so SSL works
    try:
        import certifi
        os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
    except Exception:
        pass
    return requests


def authorize():
    """
    Run the full RD device-code OAuth flow.
    Returns True on success, False on cancel/error.
    """
    try:
        requests = _get_requests()
    except Exception as exc:
        _log(f"Failed to import requests: {exc}", xbmc.LOGERROR)
        xbmcgui.Dialog().ok("KDMM", f"Import error: {type(exc).__name__}: {exc}")
        return False

    # 1. Request a device code
    try:
        resp = requests.get(
            f"{_RD_OAUTH_BASE}/device/code",
            params={"client_id": _CLIENT_ID, "new_credentials": "yes"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        _log(f"Failed to get device code: {exc}", xbmc.LOGERROR)
        xbmcgui.Dialog().ok("KDMM", f"RD error: {type(exc).__name__}: {str(exc)[:200]}")
        return False

    device_code = data["device_code"]
    user_code = data["user_code"]
    interval = data.get("interval", 5)
    expires_in = data.get("expires_in", 600)
    verification_url = data.get("verification_url", "https://real-debrid.com/device")
    direct_url = data.get("direct_verification_url", verification_url)

    # 2. Generate QR code image
    qr_path = _generate_qr_image(direct_url)

    # 3. Show dialog and poll
    success = _show_auth_dialog(
        device_code=device_code,
        user_code=user_code,
        verification_url=verification_url,
        qr_image_path=qr_path,
        interval=interval,
        expires_in=expires_in,
    )

    # Clean up QR image
    if qr_path and os.path.isfile(qr_path):
        try:
            os.remove(qr_path)
        except Exception:
            pass

    return success


def _generate_qr_image(url):
    """Generate a QR code PNG for the given URL. Returns the file path."""
    userdata = xbmcvfs.translatePath(f"special://profile/addon_data/{_ADDON_ID}/")
    os.makedirs(userdata, exist_ok=True)
    qr_path = os.path.join(userdata, "rd_qr.png")

    try:
        import qrcode
        img = qrcode.make(url, box_size=10, border=2)
        img.save(qr_path)
        _log(f"QR code saved to {qr_path}")
        return qr_path
    except Exception as exc:
        _log(f"QR generation failed: {exc}", xbmc.LOGWARNING)
        return None


def _show_auth_dialog(device_code, user_code, verification_url, qr_image_path,
                      interval, expires_in):
    """
    Show a custom dialog with QR code + user code, poll RD until
    the user authorizes or cancels.
    """
    requests = _get_requests()

    dialog = xbmcgui.DialogProgress()
    dialog.create(
        "KDMM – Link Real-Debrid",
        f"Go to: [B]{verification_url}[/B]\n"
        f"Enter code: [B]{user_code}[/B]\n"
        f"Or scan the QR code in your Kodi addon data folder."
    )

    # If we have a QR image, show it as a notification so the user sees it
    if qr_image_path and os.path.isfile(qr_image_path):
        xbmcgui.Dialog().notification(
            "Scan QR Code", f"Code: {user_code}",
            qr_image_path, 10000, False
        )

    deadline = time.time() + expires_in
    poll_url = f"{_RD_OAUTH_BASE}/device/credentials"

    while time.time() < deadline:
        if dialog.iscanceled():
            dialog.close()
            _log("User cancelled authorization")
            return False

        elapsed = int(time.time() + expires_in - deadline)
        remaining = int(deadline - time.time())
        percent = min(99, int((elapsed / expires_in) * 100))
        dialog.update(
            percent,
            f"Go to: [B]{verification_url}[/B]\n"
            f"Enter code: [B]{user_code}[/B]\n"
            f"Waiting for authorization… ({remaining}s remaining)"
        )

        xbmc.sleep(interval * 1000)

        # Poll for credentials
        try:
            resp = requests.get(
                poll_url,
                params={"client_id": _CLIENT_ID, "code": device_code},
                timeout=10,
            )
        except Exception:
            continue

        if resp.status_code == 200:
            creds = resp.json()
            client_id = creds.get("client_id", _CLIENT_ID)
            client_secret = creds.get("client_secret", "")

            # Exchange device code for access token
            token_data = _exchange_code(client_id, client_secret, device_code)
            if token_data:
                _save_tokens(client_id, client_secret, token_data)
                dialog.close()
                xbmcgui.Dialog().notification(
                    "KDMM", "Real-Debrid authorized!",
                    xbmcgui.NOTIFICATION_INFO, 3000
                )
                _log("Authorization successful")
                return True
            else:
                dialog.close()
                xbmcgui.Dialog().ok("KDMM", "Failed to get access token from Real-Debrid.")
                return False

        # 403 = still pending, anything else is an error
        if resp.status_code != 403:
            _log(f"Unexpected poll response: {resp.status_code}", xbmc.LOGWARNING)

    dialog.close()
    xbmcgui.Dialog().ok("KDMM", "Authorization timed out. Please try again.")
    return False


def _exchange_code(client_id, client_secret, device_code):
    """Exchange the device code for an access + refresh token."""
    requests = _get_requests()
    try:
        resp = requests.post(
            f"{_RD_OAUTH_BASE}/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "code": device_code,
                "grant_type": "http://oauth.net/grant_type/device/1.0",
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        _log(f"Token exchange failed: {exc}", xbmc.LOGERROR)
        return None


def _save_tokens(client_id, client_secret, token_data):
    """Persist OAuth tokens to JSON file in addon_data."""
    _write_tokens({
        "client_id": client_id,
        "client_secret": client_secret,
        "access_token": token_data.get("access_token", ""),
        "refresh_token": token_data.get("refresh_token", ""),
        "expiry": int(time.time()) + token_data.get("expires_in", 0),
    })


def refresh_token():
    """
    Refresh the RD access token using the stored refresh token.
    Returns the new access token, or None on failure.
    """
    requests = _get_requests()

    tokens = _load_tokens()
    client_id = tokens.get("client_id", "")
    client_secret = tokens.get("client_secret", "")
    refresh_tok = tokens.get("refresh_token", "")

    if not all([client_id, client_secret, refresh_tok]):
        _log("Missing credentials for token refresh", xbmc.LOGWARNING)
        return None

    try:
        resp = requests.post(
            f"{_RD_OAUTH_BASE}/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "code": refresh_tok,
                "grant_type": "refresh_token",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        _log(f"Token refresh failed: {exc}", xbmc.LOGERROR)
        return None

    new_token = data.get("access_token", "")
    if new_token:
        tokens["access_token"] = new_token
        tokens["refresh_token"] = data.get("refresh_token", refresh_tok)
        tokens["expiry"] = int(time.time()) + data.get("expires_in", 0)
        _write_tokens(tokens)
        _log("Token refreshed successfully")
    return new_token or None


def get_access_token():
    """
    Return a valid RD access token.
    Priority:
      1. API key entered directly in addon settings (rd_api_key) – no expiry
      2. OAuth tokens stored in rd_tokens.json – auto-refreshed if expiring
    Returns None if neither is configured.
    """
    # 1. Check for a directly-entered API key in addon settings
    try:
        addon = xbmcaddon.Addon()
        api_key = addon.getSetting("rd_api_key").strip()
        if api_key:
            _log("Using API key from addon settings")
            return api_key
    except Exception as exc:
        _log(f"Could not read addon settings: {exc}", xbmc.LOGWARNING)

    # 2. Fall back to OAuth JSON tokens
    tokens = _load_tokens()
    token = tokens.get("access_token", "")
    if not token:
        return None

    # Refresh 5 min before expiry
    expiry = tokens.get("expiry", 0)
    if time.time() > (expiry - 300):
        _log("Access token expired or expiring soon, refreshing…")
        token = refresh_token()

    return token or None


def revoke():
    """Clear all stored RD tokens."""
    _write_tokens({})
    _log("RD authorization revoked")
