"""Multiple-choice settings shown as a pick-from-list dialog.

These used to be spinner ("enum") settings that stored the option's index.
A "select" setting stores the option's label instead, so every reader goes
through choice_index(), which understands both, and the service rewrites
any stored index as its label once (see migrate_choice_settings).
"""

import xml.etree.ElementTree as ET

import xbmc
import xbmcaddon
import xbmcvfs

CHOICES = {
    "debrid_provider_order": ("Real-Debrid first", "AllDebrid first"),
    "hdr_priority": ("Dolby Vision", "HDR10+", "HDR10", "Any HDR", "SDR only"),
    "resolution_priority": ("4K (2160p)", "1080p", "720p"),
    "source_priority": ("Remux", "BluRay encode", "WEB", "Any"),
    "tv_pack_preference": ("Prefer complete packs", "Balanced", "Allow singles"),
    "preferred_audio_language": (
        "Any", "English", "Japanese", "Korean", "Spanish", "French", "German",
        "Italian", "Portuguese", "Chinese", "Hindi", "Arabic", "Russian",
        "Dutch", "Polish", "Swedish", "Danish", "Norwegian", "Finnish",
        "Turkish",
    ),
}


def index_of(key, raw, default=0):
    """Index of a stored value: an old index ("2") or a label ("HDR10")."""
    options = CHOICES[key]
    text = str(raw if raw is not None else "").strip()
    if not text:
        return default
    if text.isdigit():
        idx = int(text)
        return idx if idx < len(options) else default
    lowered = text.lower()
    for idx, label in enumerate(options):
        if label.lower() == lowered:
            return idx
    return default


def choice_index(key, default=0, addon=None):
    try:
        raw = (addon or xbmcaddon.Addon()).getSetting(key)
    except Exception:
        return default
    return index_of(key, raw, default)


def _stored_values(settings_path):
    """Raw values from the add-on's saved settings file, keyed by setting id."""
    try:
        root = ET.parse(settings_path).getroot()
    except Exception:
        return {}
    return {
        node.get("id"): (node.text or "").strip()
        for node in root.iter("setting")
        if node.get("id") in CHOICES
    }


def migrate_choice_settings(addon=None, settings_path=None):
    """Rewrite stored indexes as labels.

    Kodi refuses a stored "1" for a select setting and silently hands back
    the default, so the index is read from the saved settings file itself;
    getSetting() would lose the user's choice.
    """
    try:
        addon = addon or xbmcaddon.Addon()
    except Exception:
        return 0
    if settings_path is None:
        settings_path = xbmcvfs.translatePath(
            f"special://profile/addon_data/{addon.getAddonInfo('id')}/settings.xml"
        )
    changed = 0
    for key, raw in _stored_values(settings_path).items():
        if not raw.isdigit():
            continue
        options = CHOICES[key]
        idx = int(raw)
        label = options[idx] if idx < len(options) else options[0]
        try:
            addon.setSetting(key, label)
            changed += 1
        except Exception as exc:
            xbmc.log(f"[KDMM Settings] Could not migrate {key}: {exc}", xbmc.LOGWARNING)
    if changed:
        xbmc.log(f"[KDMM Settings] Converted {changed} multiple-choice setting(s) to labels", xbmc.LOGINFO)
    return changed
