#!/usr/bin/env python3
"""按文件名对已抓取的微信研报做「因子可提取性」初筛。

只读文件树，不移动、不删除、不改名任何原文；输出三份清单 + 一份汇总：

    <out>/keep.csv     有明确因子/模型构造，值得进正文抽取
    <out>/review.csv   标题看不出来，需要翻正文才能定性
    <out>/drop.csv     标题已可判定无因子（市场快讯 / 业绩播报 / 招聘 / 观点）

用法：
    python backend/scripts/wcd_article_triage.py --root "D:\\quant\\_wcd\\4.7\\下载"
    python backend/scripts/wcd_article_triage.py --root ... --out docs/research/wcd_triage
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter
from pathlib import Path

# 1) 业绩播报 / 风格轮动周报：只报排名与超额，正文没有因子定义
BULLETIN_RULES = [
    (re.compile(r"Score_v\d"), "因子业绩周报", "自家因子超额播报，无构造"),
    (re.compile(r"因子选股双周报"), "因子表现播报", "只播排名与超额，无定义"),
    (
        re.compile(
            r"(风格|特征|因子).*(领衔|登顶|突出|显著|强势|强劲|焦点|榜首|继续|持续|依旧|回到|成为|表现)"
        ),
        "风格轮动播报",
        "风格周报，无因子定义",
    ),
    (re.compile(r"因子较优"), "因子表现播报", "只播排名与超额，无定义"),
    (re.compile(r"成分股分红如何影响对冲成本"), "对冲成本周报", "分红对冲测算，非选股因子"),
]

# 2) 明确含因子/模型构造的强信号
STRONG_KEEP = [
    "因子挖掘",
    "因子重构",
    "因子的重构",
    "因子改进",
    "多因子",
    "量价因子",
    "选股因子",
    "因子组合",
    "因子加权",
    "alpha模型",
    "alpha预测",
    "DFQ",
    "ABCM",
    "ADWM",
    "KDEnsemble",
    "NeuralODE",
    "ASTGNN",
    "StyleNet",
    "BetaAgent",
    "FHQ",
    "中心度因子",
    "盈余公告异象",
    "相对定价",
    "风险注意力",
    "基本面因子",
    "深度学习选股",
    "在线学习",
    "稀疏自编码",
    "缠论",
    "可转债估值因子",
    "现金流实现率",
    "非线性市值",
    "时点风险模型",
    "周频alpha",
    "行业轮动模型",
    "量价模型",
    "信号合成",
]

# 3) 研究专题前缀：至少进人工复核，不按新闻处理
RESEARCH_PREFIX = ["专题研究", "量化专题", "说深度", "深度研究", "方法论", "系列之"]

# 4) 基金市场快讯
NEWS = re.compile(
    r"(ETF|REITs|QDII|公募|基金|份额|降费|限购|申购|获批|结募|认购|增资|自购|扩募|发行|上市|托管|仓位|重仓|持仓|季报|年报|FOF|基金经理|养老金|指数基金|互认|一级交易商|减仓|增持|配置权重|行业配置|指数发布|合格境外|调仓|资金再配置|潜在影响)"
)

# 5) 非研究内容
MISC = re.compile(
    r"(团队介绍|招聘|实习生|邀请函|沙龙|论坛|培训|恭贺|新春|研究精选|目录摘要|研究合伙|研思共进|敬告|声明|社招|校招|秋招|研究岗|立足研究|AI投研实战应用手册|skill介绍|工具介绍|直播)"
)

# 6) 宏观择时观点 / 策略配置
VIEWS = re.compile(
    r"(权益观点|配置策略|择时观点|拥挤|市场打分|赔率|胜率买点|量化观市|展望|高景气|进攻时刻)"
)


def dedupe_key(title: str) -> str:
    """去掉公众号/栏目前缀，用于识别同一模型被发过两次（专题研究 vs 说深度）。"""
    t = re.sub(r"^(专题研究|量化专题|东方金工说深度第\d+期|华源金工|天风金工)", "", title)
    t = re.sub(
        r"(深度学习系列之[一二三四五六七八九十]|资产定价系列之[一二三四五六七八九十]|价格行为系列之[一二三四五六七八九十])$",
        "",
        t,
    )
    return t[:16]


def classify(name: str) -> tuple[str, str, str]:
    """返回 (标签, 类别, 理由)。"""
    for pat, cat, why in BULLETIN_RULES:
        if pat.search(name):
            return "DROP", cat, why

    # 「…超额2187」这类纯业绩播报（不含专题/深度字样的短标题）
    if re.search(r"超额\d{3,}$", name) and not re.search(r"(专题|说深度)", name):
        return "DROP", "因子业绩周报", "只报超额数值，无因子构造"

    for kw in STRONG_KEEP:
        if kw in name:
            return "KEEP", "因子/模型构造", f"命中「{kw}」"

    if MISC.search(name):
        return "DROP", "非研究内容", "团队/招聘/活动/工具介绍"

    if NEWS.search(name):
        return "DROP", "基金市场快讯", "产品与规模动态，无因子"

    if VIEWS.search(name):
        return "DROP", "择时/配置观点", "观点类，无横截面因子"

    if any(p in name for p in RESEARCH_PREFIX):
        return "REVIEW", "研究专题", "专题标题，需翻正文确认有无可复现构造"

    return "REVIEW", "其他", "标题无明确信号"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="抓取根目录（含各公众号子目录）")
    ap.add_argument("--out", default="docs/research/wcd_triage")
    ap.add_argument("--ext", default=".md", help="以哪种落地文件为每篇文章的代表")
    args = ap.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"目录不存在: {root}")
        return 2

    files = sorted(root.rglob(f"*{args.ext}"))
    if not files:
        print(f"没找到 {args.ext} 文件: {root}")
        return 2

    rows = []
    for f in files:
        rel = f.relative_to(root)
        account = rel.parts[0] if len(rel.parts) > 1 else ""
        label, cat, why = classify(f.stem)
        rows.append(
            {
                "account": account,
                "title": f.stem,
                "label": label,
                "category": cat,
                "reason": why,
                "md": str(rel),
            }
        )

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for label, fname in (("KEEP", "keep.csv"), ("REVIEW", "review.csv"), ("DROP", "drop.csv")):
        sub = [r for r in rows if r["label"] == label]
        with open(out / fname, "w", encoding="utf-8-sig", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["account", "title", "category", "reason", "md"])
            w.writeheader()
            for r in sub:
                w.writerow({k: r[k] for k in w.fieldnames})

    cnt = Counter(r["label"] for r in rows)
    cat_cnt = Counter((r["label"], r["category"]) for r in rows)
    acc_cnt = Counter((r["account"], r["label"]) for r in rows)

    lines = [
        "# 微信研报文章因子可提取性初筛（按文件名）",
        "",
        f"- 扫描根目录：`{root}`",
        f"- 代表文件后缀：`{args.ext}`，文章数：**{len(rows)}**",
        f"- KEEP（可直接进正文抽取）：**{cnt['KEEP']}**",
        f"- REVIEW（需翻正文）：**{cnt['REVIEW']}**",
        f"- DROP（标题即可判定无因子）：**{cnt['DROP']}**",
        "",
        "## 分类计数",
        "",
        "| 标签 | 类别 | 篇数 |",
        "|---|---|---|",
    ]
    for (label, cat), n in sorted(cat_cnt.items(), key=lambda kv: (kv[0][0] != "KEEP", -kv[1])):
        lines.append(f"| {label} | {cat} | {n} |")

    lines += ["", "## 按公众号", "", "| 公众号 | KEEP | REVIEW | DROP |", "|---|---|---|---|"]
    for acc in sorted({r["account"] for r in rows}):
        lines.append(
            f"| {acc} | {acc_cnt[(acc, 'KEEP')]} | {acc_cnt[(acc, 'REVIEW')]} | {acc_cnt[(acc, 'DROP')]} |"
        )

    lines += ["", "## KEEP 清单", ""]
    for r in [x for x in rows if x["label"] == "KEEP"]:
        lines.append(f"- {r['title']}  \n  `{r['account']}` · {r['reason']}")

    keep_rows = [x for x in rows if x["label"] == "KEEP"]
    groups: dict[str, list[dict]] = {}
    for r in keep_rows:
        groups.setdefault(dedupe_key(r["title"]), []).append(r)
    dup = {k: v for k, v in groups.items() if len(v) > 1}
    lines += [
        "",
        "## KEEP 内的重复组（同一模型发过两次，抽因子读一篇即可）",
        "",
        f"独立选题 {len(groups)} 个，涉及 {len(keep_rows)} 篇，重复 {len(keep_rows) - len(groups)} 篇。",
        "",
    ]
    for k, v in dup.items():
        lines.append(f"- **{k}**")
        for r in v:
            lines.append(f"  - {r['title']}")

    lines += ["", "## REVIEW 清单", ""]
    for r in [x for x in rows if x["label"] == "REVIEW"]:
        lines.append(f"- {r['title']}  \n  `{r['account']}` · {r['category']} · {r['reason']}")

    lines += ["", "## DROP 类别抽样", ""]
    seen = Counter()
    for r in rows:
        if r["label"] != "DROP":
            continue
        if seen[r["category"]] < 3:
            lines.append(f"- [{r['category']}] {r['title']}")
            seen[r["category"]] += 1

    (out / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"文章数: {len(rows)}")
    print(f"KEEP={cnt['KEEP']}  REVIEW={cnt['REVIEW']}  DROP={cnt['DROP']}")
    print(f"输出: {out.resolve()}")
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(main())
