#!/usr/bin/env python3
"""把 cards/*.md 的抽取结果合并成总表 + 分层汇总。

输出：
  docs/research/wcd_factors/index.csv     一行一个因子
  docs/research/wcd_factors/SUMMARY.md    按数据依赖分层 + 缺公式清单
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

CARDS = Path("docs/research/wcd_factors/cards")
OUTDIR = Path("docs/research/wcd_factors")

# 数据依赖 → 分层名（顺序即优先级：越靠后越难）
NEEDS = [
    ("逐笔", re.compile(r"逐笔|tick|成交笔数|分笔")),
    ("分钟线", re.compile(r"分钟|minute|5m|1m|15m|日内")),
    ("公告文本", re.compile(r"公告|文本|研报正文|新闻")),
    ("分析师预期", re.compile(r"一致预期|分析师|预测数据|盈利预测|评级")),
    ("财务三表", re.compile(r"财务|年报|季报|资产负债表|现金流量|利润表|财报")),
    ("指数成分", re.compile(r"指数成分|成分股|行业分类|中信|申万")),
    ("日频量价", re.compile(r"行情|收盘|开盘|最高|最低|成交量|成交额|换手|复权")),
]


def parse_card(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    title = ""
    src = ""
    for ln in text.splitlines():
        if ln.startswith("# ") and not title:
            title = ln[2:].strip()
        if ln.startswith("来源：") and not src:
            src = ln[3:].strip().strip("`")
        if title and src:
            break

    recs: list[dict] = []
    cur: dict | None = None
    field: str | None = None
    for ln in text.splitlines():
        m = re.match(r"^##\s*因子[:：]\s*(.+?)\s*$", ln)
        if m:
            cur = {"factor": m.group(1), "formula": "", "data": "", "value": "", "param": ""}
            recs.append(cur)
            field = None
            continue
        if cur is None:
            continue
        s = ln.strip()
        hit = re.match(r"^-\s*\*\*(公式|变量与数据|业绩|参数|逻辑)\*\*", s)
        if hit:
            key = {"公式": "formula", "变量与数据": "data", "业绩": "value", "参数": "param", "逻辑": "logic"}[hit.group(1)]
            field = key
            cur.setdefault(key, "")
            cur[key] += s.replace("**", "").split("：", 1)[-1].strip() + "\n"
            continue
        # 续行：公式的缩进子行、图片 URL、多行表格
        if field and s and not s.startswith("#"):
            cur[field] = cur.get(field, "") + s + "\n"
        elif not s:
            field = field

    for r in recs:
        r["title"] = title
        r["source"] = src or path.name
        r["card"] = str(path)
        r["no_formula"] = "NO_TEXT_FORMULA" in r["formula"]
        need = []
        blob = " ".join([r["data"], r["param"], r["formula"]])
        for name, pat in NEEDS:
            if pat.search(blob):
                need.append(name)
        r["needs"] = "/".join(need) or "日频量价"
    return recs


def main() -> int:
    files = sorted(CARDS.glob("*.md"))
    if not files:
        print(f"没有卡片：{CARDS} 为空")
        return 1
    recs: list[dict] = []
    for f in files:
        recs.extend(parse_card(f))

    OUTDIR.mkdir(parents=True, exist_ok=True)
    with open(OUTDIR / "index.csv", "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=["title", "factor", "needs", "no_formula", "data", "value", "param", "formula", "source", "card"],
            extrasaction="ignore",
        )
        w.writeheader()
        w.writerows(recs)

    # 分层：以最稀缺的那一层归类
    order = [n for n, _ in reversed(NEEDS)]  # 最难的在前
    buckets: dict[str, list[dict]] = {n: [] for n in order}
    for r in recs:
        tier = next((n for n in order if n in r["needs"]), "日频量价")
        buckets[tier].append(r)

    lines = [
        "# 因子抽取总览",
        "",
        f"- 卡片数 {len(files)}，抽出因子 {len(recs)} 个",
        f"- 缺文字公式（公式只在图里）{sum(r['no_formula'] for r in recs)} 个",
        "",
        "## 按稀缺数据分层",
        "",
    ]
    for n in order:
        rows = buckets[n]
        if not rows:
            continue
        lines.append(f"### 需要{n}（{len(rows)} 个）")
        lines.append("")
        lines.append("| 因子 | 报告 | 缺公式 |")
        lines.append("|---|---|---|")
        for r in rows:
            lines.append(f"| {r['factor']} | {r['title'][:34]} | {'是' if r['no_formula'] else '否'} |")
        lines.append("")

    nf = [r for r in recs if r["no_formula"]]
    lines += ["## 缺文字公式清单（需看图补）", ""]
    for r in nf:
        lines.append(f"- {r['factor']}（{r['title'][:30]}）")
    lines.append("")
    (OUTDIR / "SUMMARY.md").write_text("\n".join(lines), encoding="utf-8")

    print(f"卡片 {len(files)} 张 → 因子 {len(recs)} 个，缺公式 {len(nf)} 个")
    for n in order:
        print(f"  {n}: {len(buckets[n])}")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(main())
