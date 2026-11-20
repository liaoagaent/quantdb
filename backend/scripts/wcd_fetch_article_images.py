#!/usr/bin/env python3
"""按文章抓全部正文图片（用于补「公式只在图里」的篇目）。

用法：
  python backend/scripts/wcd_fetch_article_images.py --articles 11,15,23,37,39
  python backend/scripts/wcd_fetch_article_images.py --articles 11 --limit 40
输出：docs/research/wcd_factors/imgs/<NN>_<i>.<ext> + map.csv(article,idx,url,file)
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(r"D:\quant\_wcd\4.7\下载")
KEEP = Path("docs/research/wcd_triage/keep.csv")
OUT = Path("docs/research/wcd_factors/imgs")
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
    ap.add_argument("--articles", default="", help="逗号分隔的序号，如 11,15,23")
    ap.add_argument("--limit", type=int, default=0, help="每篇最多抓几张")
    a = ap.parse_args()

    rows = list(csv.DictReader(open(KEEP, encoding="utf-8-sig")))
    want = {int(x) for x in a.articles.split(",") if x.strip().isdigit()}
    OUT.mkdir(parents=True, exist_ok=True)
    mapf = OUT / "map.csv"
    existing: list[dict] = []
    if mapf.exists():
        existing = list(csv.DictReader(open(mapf, encoding="utf-8-sig")))
    seen = {(r["article"], r["url"]) for r in existing}

    new_rows: list[dict] = []
    for i, r in enumerate(rows, 1):
        if want and i not in want:
            continue
        p = ROOT / r["md"]
        if not p.exists():
            print(f"跳过（缺文件）{r['md']}")
            continue
        urls = []
        for u in URL.findall(p.read_text(encoding="utf-8", errors="ignore")):
            if u not in urls:
                urls.append(u)
        if a.limit:
            urls = urls[: a.limit]
        ok = 0
        for j, u in enumerate(urls, 1):
            if (f"{i:02d}", u) in seen:
                continue
            dst = OUT / f"{i:02d}_{j:02d}.png"
            if dst.exists() and dst.stat().st_size > 1500:
                ok += 1
                continue
            try:
                req = urllib.request.Request(u, headers=HDR)
                with urllib.request.urlopen(req, timeout=25) as resp:
                    data = resp.read()
                if len(data) < 800:
                    continue
                dst.write_bytes(data)
                new_rows.append({"article": f"{i:02d}", "idx": j, "url": u, "file": dst.name, "title": r["title"][:40]})
                ok += 1
            except Exception as e:  # noqa: BLE001
                print(f"  {i:02d}-{j:02d} 失败 {type(e).__name__}: {e}")
            time.sleep(0.05)
        print(f"{i:02d} {r['title'][:30]}：{len(urls)} 张，Ok {ok}")

    if new_rows:
        with open(mapf, "w", encoding="utf-8-sig", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["article", "idx", "url", "file", "title"])
            w.writeheader()
            w.writerows(existing + new_rows)
    print(f"新增 {len(new_rows)} 张 → {OUT}")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(main())
