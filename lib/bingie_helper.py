"""Make TMDb Bingie Helper's Python modules importable from KDMM."""

import os
import sys
import xml.etree.ElementTree as ET

import xbmcaddon

BINGIE_HELPER_ID = "plugin.video.tmdb.bingie.helper"


def addon_path(addon_id):
    try:
        return xbmcaddon.Addon(addon_id).getAddonInfo("path")
    except Exception:
        return None


def add_bingie_helper_paths():
    """Put the helper and its script.module dependencies on sys.path.

    Returns the helper's install path, or None when it is not installed.
    """
    helper_path = addon_path(BINGIE_HELPER_ID)
    if not helper_path:
        return None

    candidate_paths = (
        os.path.join(helper_path, "resources", "modules"),
        os.path.join(helper_path, "resources"),
        os.path.join(helper_path, "resources", "lib"),
        helper_path,
    )
    for path in reversed(candidate_paths):
        if path and path not in sys.path:
            sys.path.insert(0, path)
    _add_dependency_module_paths(helper_path)
    return helper_path


def _add_dependency_module_paths(path, seen=None):
    """Add the python module folders of an add-on's dependencies to sys.path.

    Kodi only does this for KDMM's own dependencies, so importing the Bingie
    helper's code (which needs script.module.bingie, infotagger, ...) fails
    unless we mirror it here.
    """
    seen = set() if seen is None else seen
    try:
        root = ET.parse(os.path.join(path, "addon.xml")).getroot()
    except Exception:
        return
    for imp in root.iter("import"):
        dep_id = imp.get("addon", "")
        if not dep_id.startswith("script.module.") or dep_id in seen:
            continue
        seen.add(dep_id)
        dep_path = addon_path(dep_id)
        if not dep_path:
            continue
        try:
            dep_root = ET.parse(os.path.join(dep_path, "addon.xml")).getroot()
        except Exception:
            continue
        for ext in dep_root.iter("extension"):
            if ext.get("point") == "xbmc.python.module":
                module_path = os.path.join(dep_path, ext.get("library", "lib"))
                if module_path not in sys.path:
                    sys.path.append(module_path)
        _add_dependency_module_paths(dep_path, seen)
