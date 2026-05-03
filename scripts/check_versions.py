#!/usr/bin/env python3
"""Fail if addon.xml version != plugin entry in docs/addons.xml."""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    addon_xml = (ROOT / "addon.xml").read_text(encoding="utf-8")
    m = re.search(r'<addon[^>]*\bid="plugin\.video\.kdmm"[^>]*\bversion="([^"]+)"', addon_xml)
    if not m:
        print("Could not find plugin.video.kdmm version in addon.xml", file=sys.stderr)
        return 1
    addon_ver = m.group(1)

    repo_xml = (ROOT / "docs" / "addons.xml").read_text(encoding="utf-8")
    m2 = re.search(r'<addon[^>]*\bid="plugin\.video\.kdmm"[^>]*\bversion="([^"]+)"', repo_xml)
    if not m2:
        print("Could not find plugin.video.kdmm version in docs/addons.xml", file=sys.stderr)
        return 1
    repo_ver = m2.group(1)

    if addon_ver != repo_ver:
        print(
            f"Version mismatch: addon.xml has {addon_ver!r} but docs/addons.xml has {repo_ver!r}",
            file=sys.stderr,
        )
        return 1
    print(f"OK: plugin.video.kdmm version {addon_ver!r} is consistent")
    return 0


if __name__ == "__main__":
    sys.exit(main())
