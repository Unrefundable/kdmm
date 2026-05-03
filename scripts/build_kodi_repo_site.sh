#!/usr/bin/env bash
# Build Kodi repository layout for https://unrefundable.github.io/kdmm/
# Output: ./release/addons.xml, ./release/addons.xml.md5,
#         ./release/plugin.video.kdmm/plugin.video.kdmm-<version>.zip
#
# GitHub Pages for this repo is served from the main branch /docs folder.
# After running, copy ./release/ into docs/ (keep docs/index.html, repo zip, etc.):
#   cp -f release/addons.xml release/addons.xml.md5 docs/
#   mkdir -p docs/plugin.video.kdmm
#   cp -f release/plugin.video.kdmm/plugin.video.kdmm-*.zip docs/plugin.video.kdmm/
#   : > docs/.nojekyll   # required so GitHub Pages serves .zip files (not 404)
#
# Optional: sync ./release/ to orphan branch gh-pages instead (use rsync
# --exclude='.git/' when cloning into a repo).

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

python3 scripts/check_versions.py

VERSION="$(python3 -c "
import re
from pathlib import Path
t = Path('addon.xml').read_text(encoding='utf-8')
m = re.search(r'<addon[^>]*\bid=\"plugin\.video\.kdmm\"[^>]*\bversion=\"([^\"]+)\"', t)
if not m:
    raise SystemExit('version not found')
print(m.group(1))
")"

OUT="$ROOT/release"
STAGING="$ROOT/.staging_addon_zip"
rm -rf "$OUT" "$STAGING"
mkdir -p "$OUT/plugin.video.kdmm" "$STAGING/plugin.video.kdmm"
# Bypass Jekyll on GitHub Pages so .zip and other assets are served (not 404).
: > "$OUT/.nojekyll"

rsync -a \
  --exclude='.git/' \
  --exclude='release/' \
  --exclude='.staging_addon_zip/' \
  --exclude='.log_extract/' \
  --exclude='docs/' \
  --exclude='__pycache__/' \
  --exclude='*.pyc' \
  ./ "$STAGING/plugin.video.kdmm/"

( cd "$STAGING" && zip -rq "$OUT/plugin.video.kdmm/plugin.video.kdmm-${VERSION}.zip" "plugin.video.kdmm" )

cp "$ROOT/docs/addons.xml" "$OUT/addons.xml"
( cd "$OUT" && md5sum addons.xml | awk '{print $1}' > addons.xml.md5 )

rm -rf "$STAGING"
echo "Built: $OUT/plugin.video.kdmm/plugin.video.kdmm-${VERSION}.zip"
ls -la "$OUT" "$OUT/plugin.video.kdmm"
