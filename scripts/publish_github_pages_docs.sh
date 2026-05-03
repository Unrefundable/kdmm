#!/usr/bin/env bash
# Build release/ and copy Kodi repo artifacts into docs/ for GitHub Pages (main /docs).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
"$(dirname "$0")/build_kodi_repo_site.sh"
VERSION="$(python3 -c "
import re
from pathlib import Path
t = Path('addon.xml').read_text(encoding='utf-8')
m = re.search(r'<addon[^>]*\bid=\"plugin\.video\.kdmm\"[^>]*\bversion=\"([^\"]+)\"', t)
if not m:
    raise SystemExit('version not found')
print(m.group(1))
")"
mkdir -p docs/plugin.video.kdmm
cp -f release/addons.xml release/addons.xml.md5 docs/
cp -f "release/plugin.video.kdmm/plugin.video.kdmm-${VERSION}.zip" docs/plugin.video.kdmm/
: > docs/.nojekyll
echo "Updated docs/ for GitHub Pages (${VERSION}). Commit and push main to deploy."
