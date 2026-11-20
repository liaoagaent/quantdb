#!/usr/bin/env python3
"""把卡片里标记为 NO_TEXT_FORMULA 的图片 URL 抓到本地，供人工/视觉读公式。

用法：
  python backend/scripts/wcd_fetch_images.py            # 按 cards/*.md 里的 URL
  python backend/scripts/wcd_fetch_images.py --limit 30 # 只抓前 30 张
输出：docs/research/wcd_factors/images/<序号>_<n>.<ext>
"""

from __future__ import annotations

import argparse
import re
import sys
import urllib.request
from pathlib import Path

CARDS = Path("docs/research/wcd_factors/cards")
OUT = Path("docs/research/wcd_factors/images")
URL = re.compile(r"https://mmbiz\.qpic\.cn/[^\s)\]\"']+")
HDR = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Referer": "https://mp.weixin.qq.com/",
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--only", default="", help="只处理文件名含该子串的卡片")
    a = ap.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    files = sorted(CARDS.glob("*.md"))
    if a.only:
        files = [f for f in files if a.only in f.name]

    picked: list[tuple[str, str]] = []  # (卡片名, url)
    for f in files:
        txt = f.read_text(encoding="utf-8", errors="ignore")
        # 只抓 NO_TEXT_FORMULA 之后同一节里的 URL
        for block in re.split(r"^##\s*因子[:：]", txt, flags=re.M)[1:]:
            if "NO_TEXT_FORMULA" not in block:
                continue
            for u in URL.findall(block)[:3]:
                picked.append((f.stem, u))
    if a.limit:
        picked = picked[: a.limit]

    ok = fail = 0
    for i, (stem, url) in enumerate(picked, 1):
        ext = ".png"
        dst = OUT / f"{i:03d}_{stem[:40]}{ext}"
        if dst.exists() and dst.stat().st_size > 2000:
            ok += 1
            continue
        try:
            req = urllib.request.Request(url, headers=HDR)
            with urllib.request.urlopen(req, timeout=25) as r:
                data = r.read()
            if len(data) < 1200:
                raise ValueError(f"too small: {len(data)}B")
            dst.write_bytes(data)
            ok += 1
        except Exception as e:  # noqa: BLE001
            fail += 1
            print(f"  失败 {stem[:30]} {url[:60]} -> {type(e).__name__}: {e}")
    print(f"目标 {len(picked)} 张，成功 {ok}，失败 {fail} → {OUT}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(main())
