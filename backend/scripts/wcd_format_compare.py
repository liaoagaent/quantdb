#!/usr/bin/env python3
"""比较同一篇文章在 md / docx / pdf 三种落地格式里的公式可提取性。

公众号正文的公式大多是截图，md 里只剩图片链接；docx 可能带 OMML 公式，
pdf 可能带文本层。本脚本对 KEEP 清单里的文章逐一统计三格式的「公式行数」，
用来决定抽因子时该读哪个文件。
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

import docx  # type: ignore
import pymupdf  # type: ignore

ROOT = Path(r"D:\quant\_wcd\4.7\下载")
KEEP = Path("docs/research/wcd_triage/keep.csv")

EQ = re.compile(r"[=＝≈∑Σ∫√βγαλμσ]")
CJK = re.compile(r"[\u4e00-\u9fff]")


def score(text: str) -> tuple[int, int]:
    """返回 (疑似公式行数, 总字符数)。"""
    n = 0
    for ln in text.splitlines():
        s = ln.strip()
        if not s:
            continue
        if 4 <= len(s) <= 120 and EQ.search(s) and not s.startswith("!["):
            n += 1
    return n, len(text)


def docx_text(p: Path) -> str:
    d = docx.Document(str(p))
    parts = [para.text for para in d.paragraphs]
    for t in d.tables:
        for row in t.rows:
            parts.append(" | ".join(c.text for c in row.cells))
    return "\n".join(parts)


def pdf_text(p: Path) -> str:
    with pymupdf.open(str(p)) as doc:
        return "\n".join(page.get_text() for page in doc)


def main() -> int:
    rows = list(csv.DictReader(open(KEEP, encoding="utf-8-sig")))
    out = []
    for r in rows:
        md = ROOT / r["md"]
        base = md.with_suffix("")
        rec = {"title": r["title"]}
        for fmt, loader in (("md", lambda p: p.read_text(encoding="utf-8", errors="ignore")), ("docx", docx_text), ("pdf", pdf_text)):
            p = base.with_suffix(f".{fmt}")
            if not p.exists():
                rec[f"{fmt}_eq"] = rec[f"{fmt}_len"] = -1
                continue
            try:
                t = loader(p)
            except Exception as exc:  # noqa: BLE001
                rec[f"{fmt}_eq"] = -2
                rec[f"{fmt}_len"] = 0
                print(f"[warn] {fmt} {p.name}: {exc}")
                continue
            eq, ln = score(t)
            rec[f"{fmt}_eq"], rec[f"{fmt}_len"] = eq, ln
        out.append(rec)

    for key in ("md", "docx", "pdf"):
        best = sum(1 for x in out if x[f"{key}_eq"] == max(x[f"{k}_eq"] for k in ("md", "docx", "pdf")))
        tot = sum(max(0, x[f"{key}_eq"]) for x in out)
        print(f"{key}: 公式行合计 {tot:>5}，{best} 篇为该格式最优")
    print()
    print(f"{'md':>4} {'docx':>5} {'pdf':>4}  title")
    for x in sorted(out, key=lambda z: -z["pdf_eq"])[:12]:
        print(f"{x['md_eq']:>4} {x['docx_eq']:>5} {x['pdf_eq']:>4}  {x['title'][:46]}")

    dst = Path("docs/research/wcd_triage/format_compare.csv")
    with open(dst, "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out[0].keys()))
        w.writeheader()
        w.writerows(out)
    print(f"\n输出 {dst}")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(main())
