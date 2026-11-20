#!/usr/bin/env python3
"""从 KEEP 文章里切出「因子怎么算」的部分，供结构化抽取。

公众号正文的公式多为截图，因此这里保留三样东西：
1. 摘要段（含 RankIC / 超额等业绩口径）
2. 命中「因子构建 / 定义 / 计算 / 模型 / 数据 / 测试」的小节全文
3. 这些小节里出现的图片 URL（后续按需看图补公式）

输出 docs/research/wcd_factors/raw/<序号>_<标题>.md，并打印体积统计。
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

ROOT = Path(r"D:\quant\_wcd\4.7\下载")
KEEP = Path("docs/research/wcd_triage/keep.csv")
OUT = Path("docs/research/wcd_factors/raw")

HEAD = re.compile(r"^\s*(?:#{1,4}\s+\S|\*\*\s*[^*]{1,60}\s*\*\*\s*$|\d+(?:\.\d+)*\s+\S)")
KW = re.compile(
    r"(因子构建|因子定义|因子计算|因子说明|构建方法|计算方法|模型构建|模型结构|指标体系|数据来源|数据处理|测试参数|回测|因子改进|因子优化|计算方式|定义)"
)
IMG = re.compile(r"!\[[^\]]*\]\(([^)]+)\)")
MAX_CHARS = 7000


def split_blocks(lines: list[str]) -> list[tuple[str, list[str]]]:
    """按标题行切成 (标题, 正文行) 的块。"""
    idx = [i for i, ln in enumerate(lines) if HEAD.match(ln)]
    blocks: list[tuple[str, list[str]]] = []
    if not idx:
        return [("(全文)", lines)]
    if idx[0] > 0:
        blocks.append(("(摘要)", lines[: idx[0]]))
    for a, b in zip(idx, idx[1:] + [len(lines)]):
        blocks.append((lines[a].strip(), lines[a + 1 : b]))
    return blocks


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    rows = list(csv.DictReader(open(KEEP, encoding="utf-8-sig")))
    total = 0
    report = []
    for i, r in enumerate(rows, 1):
        p = ROOT / r["md"]
        if not p.exists():
            continue
        lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
        blocks = split_blocks(lines)

        keep: list[str] = []
        imgs: list[str] = []
        # 摘要：开头 40 行里去掉图片链接
        head = [ln for ln in lines[:40] if not IMG.search(ln) and ln.strip()]
        keep.append("## 摘要 / 业绩口径\n" + "\n".join(head[:25]))

        for title, body in blocks:
            text = "\n".join(body)
            if KW.search(title) or (("因子" in text or "模型" in text) and "=" in text):
                body_clean = [ln for ln in body if not IMG.search(ln)]
                seg = "\n".join(body_clean).strip()
                if not seg:
                    continue
                keep.append(f"## {title}\n{seg[:3000]}")
                for ln in body:
                    m = IMG.search(ln)
                    if m:
                        imgs.append(m.group(1))

        doc = f"# {r['title']}\n\n来源：`{r['md']}`\n\n" + "\n\n".join(keep)
        doc = doc[:MAX_CHARS]
        if imgs:
            doc += "\n\n## 小节内图片（按出现顺序，公式常在其中的前 3 张）\n"
            doc += "\n".join(f"{j+1}. {u}" for j, u in enumerate(imgs[:8]))

        safe = re.sub(r'[\\/:*?"<>|]', "_", r["title"])[:60]
        name = f"{i:02d}_{safe}.md"
        (OUT / name).write_text(doc, encoding="utf-8")
        total += len(doc)
        report.append((len(doc), len(imgs), r["title"], name))

    report.sort(reverse=True)
    print(f"篇数 {len(report)}，片段总量 {total/1024:.0f} KB → {OUT}")
    for sz, nimg, title, _ in report[:8]:
        print(f"  {sz:>5} 字 / {nimg:>3} 图  {title[:44]}")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(main())
