#!/usr/bin/env python3
"""给抓下来的图片打分，挑出「大概率是公式截图」的那些，避免逐张浪费。

判据：白底占比高（公式截图多为纯白背景）+ 有少量深色像素（文字/符号）+ 尺寸不像整页截图。
输出 docs/research/wcd_factors/images/index.csv，按概率降序。
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

from PIL import Image

IMG = Path("docs/research/wcd_factors/images")


def score(p: Path) -> dict:
    im = Image.open(p).convert("L")
    w, h = im.size
    px = list(im.getdata())
    n = len(px)
    white = sum(1 for v in px if v > 240) / n
    dark = sum(1 for v in px if v < 100) / n
    mid = 1 - white - dark
    # 彩色像素占比：公式截图是纯灰阶（图表带色 → 彩色比高）
    rgb = Image.open(p).convert("RGB")
    small = rgb.resize((min(w, 320), max(1, int(h * min(w, 320) / max(w, 1)))))
    rp = list(small.getdata())
    color = sum(1 for (r, g, b) in rp if abs(r - g) + abs(g - b) + abs(r - b) > 60) / len(rp)
    # 公式：大白底 + 少量黑（1%~12%） + 灰阶少 + 彩色几乎没有
    likely = white > 0.55 and 0.003 < dark < 0.16 and color < 0.02
    return {
        "file": p.name,
        "w": w,
        "h": h,
        "white": round(white, 3),
        "dark": round(dark, 3),
        "mid": round(mid, 3),
        "color": round(color, 4),
        "likely_formula": int(likely),
        "score": round(white * (1 - min(mid * 2, 1)) * (1 - min(color * 20, 1)), 3),
        "bytes": p.stat().st_size,
    }


def main() -> int:
    files = sorted(IMG.glob("*.png"))
    if not files:
        print("没有图片")
        return 1
    rows = [score(p) for p in files]
    rows.sort(key=lambda r: (r["likely_formula"], r["score"]), reverse=True)
    with open(IMG / "index.csv", "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    n = sum(r["likely_formula"] for r in rows)
    print(f"图片 {len(rows)} 张，疑似公式图 {n} 张 → {IMG/'index.csv'}")
    for r in rows[:10]:
        print(f"  {'F' if r['likely_formula'] else '-'} {r['w']}x{r['h']} white={r['white']} dark={r['dark']} {r['file'][:60]}")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(main())
