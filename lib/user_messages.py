"""User-facing error messages for KDMM network and playback failures."""

import socket


_NETWORK_MARKERS = (
    "connection aborted",
    "connection error",
    "connection refused",
    "connection reset",
    "failed to establish a new connection",
    "max retries exceeded",
    "name or service not known",
    "name resolution",
    "network is down",
    "network is unreachable",
    "no route to host",
    "nodename nor servname provided",
    "temporary failure in name resolution",
)
_TIMEOUT_MARKERS = (
    "connect timeout",
    "connection timed out",
    "read timed out",
    "timed out",
    "timeout",
)
_CHECK_NETWORK = "Check the CoreELEC/Kodi network connection, then try again."


def _clean_text(value):
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    return " ".join(text.split())


def _shorten(value, limit=180):
    text = _clean_text(value)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _status_code(exc):
    response = getattr(exc, "response", None)
    code = getattr(response, "status_code", None)
    try:
        return int(code) if code is not None else None
    except (TypeError, ValueError):
        return None


def is_timeout_exception(exc):
    name = type(exc).__name__.lower()
    text = _clean_text(exc).lower()
    return "timeout" in name or any(marker in text for marker in _TIMEOUT_MARKERS)


def is_network_exception(exc):
    if _status_code(exc) is not None:
        return False
    name = type(exc).__name__.lower()
    text = _clean_text(exc).lower()
    if is_timeout_exception(exc):
        return True
    if any(word in name for word in ("connectionerror", "connecttimeout", "timeout")):
        return True
    return any(marker in text for marker in _NETWORK_MARKERS)


def describe_exception(exc, service="network service", action="contacting it"):
    service = service or "network service"
    action = action or "contacting it"
    code = _status_code(exc)
    if code is not None:
        return f"{service} returned HTTP {code} while {action}. Try again later."
    if is_timeout_exception(exc):
        return f"Connection to {service} timed out while {action}. {_CHECK_NETWORK}"
    if is_network_exception(exc):
        return f"Cannot reach {service} while {action}. {_CHECK_NETWORK}"
    details = _shorten(f"{type(exc).__name__}: {exc}", 140)
    return f"{service} failed while {action}: {details}"


def describe_failure(exc, service="KDMM", action="working"):
    text = _clean_text(exc)
    if text.startswith(("Cannot reach ", "Connection to ", "No internet connection")):
        return _shorten(text, 220)
    if " returned HTTP " in text and " while " in text:
        return _shorten(text, 220)
    return describe_exception(exc, service, action)


def internet_is_available(timeout=1.5):
    for host, port in (("1.1.1.1", 443), ("8.8.8.8", 53)):
        sock = None
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
            return True
        except OSError:
            continue
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
    return False


def describe_playback_failure(message):
    if not internet_is_available():
        return f"No internet connection detected. {_CHECK_NETWORK}"
    return message
