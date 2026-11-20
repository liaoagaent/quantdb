#!/usr/bin/env python3
"""统计 KEEP 文章里公式与图片的分布，判断哪些能直接从正文抽公式。

对每篇 md 统计：字节数、图片数、疑似公式行数（含 = 的短行）、LaTeX 公式数、
表格行数。输出 csv，供决定「直接读正文」还是「需看截图」。
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

ROOT = Path(r"D:\quant\_wcd\4.7\下载")
KEEP = Path("docs/research/wcd_triage/keep.csv")

IMG = re.compile(r"!\[[^\]]*\]\([^)]+\)")
TEX = re.compile(r"\$[^$\n]{2,80}\$")
FORMULA_HINT = re.compile(r"(=|＝|≈|∑|Σ|∫|rank|Rank|IC|z-score|标准化|加权)")


def main() -> int:
    rows = list(csv.DictReader(open(KEEP, encoding="utf-8-sig")))
    out = []
    for r in rows:
        p = ROOT / r["md"]
        if not p.exists():
            continue
        text = p.read_text(encoding="utf-8", errors="ignore")
        lines = text.splitlines()
        imgs = len(IMG.findall(text))
        tex = len(TEX.findall(text))
        short_eq = sum(
            1 for ln in lines if "=" in ln and 8 <= len(ln.strip()) <= 100 and not ln.strip().startswith("![]")
        )
        # 表格行：以 | 开头
        table = sum(1 for ln in lines if ln.strip().startswith("|"))
        out.append(
            {
                "title": r["title"],
                "kb": round(len(text.encode("utf-8")) / 1024, 1),
                "imgs": imgs,
                "eq_lines": short_eq,
                "tex": tex,
                "table_rows": table,
                "md": r["md"],
            }
        )

    out.sort(key=lambda x: (-x["eq_lines"], -x["kb"]))
    dst = Path("docs/research/wcd_triage/formula_density.csv")
    with open(dst, "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["title", "kb", "imgs", "eq_lines", "tex", "table_rows", "md"])
        w.writeheader()
        w.writerows(out)

    print(f"篇数 {len(out)}，输出 {dst}")
    print("公式行数分布：")
    for lo, hi in ((0, 2), (3, 10), (11, 30), (31, 999)):
        n = sum(1 for x in out if lo <= x["eq_lines"] <= hi)
        print(f"  {lo:>3}-{hi:<3} 行: {n} 篇")
    print("\n公式最密集的 10 篇：")
    for x in out[:10]:
        print(f"  {x['eq_lines']:>3} 式 / {x['imgs']:>3} 图 / {x['kb']:>5} KB  {x['title'][:40]}")
    print("\n几乎无文字公式的 10 篇：")
    for x in out[-10:]:
        print(f"  {x['eq_lines']:>3} 式 / {x['imgs']:>3} 图 / {x['kb']:>5} KB  {x['title'][:40]}")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(main())
