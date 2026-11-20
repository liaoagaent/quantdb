#!/usr/bin/env python3
"""腾讯分笔 → 每日成交笔数（免费替代 Wind L2，只拿「笔数」这一个量）。

腾讯分笔接口只给**最近一个交易日**，所以必须每天收盘后跑一次，攒够 20 个交易日
就能覆盖理想反转的滚动窗口。实测：8 并发全市场约 92 分钟，16 并发约 46 分钟。

已知缺失：分笔不含集合竞价（实测茅台累计 40,350 手 vs 快照 41,780 手，少 3.4%），
笔数口径略低于 Wind 的「成交笔数」。对「单笔成交额排序」影响很小。

落盘：data/quantdb/5_technical_derived/trade_count_daily.parquet
      与 backend/scripts/l2_ticks_daily.py 同 schema，可混用/互相覆盖。

用法:
    python backend/scripts/tencent_ticks_daily.py --all --workers 12
    python backend/scripts/tencent_ticks_daily.py --symbols 600519.SH,000001.SZ
    python backend/scripts/tencent_ticks_daily.py --all --date 2026-11-13   # 补当日（默认自动识别）
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("tencent_ticks_daily")

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0 Safari/537.36",
      "Referer": "https://gu.qq.com/"}
URL = "https://stock.gtimg.cn/data/index.php?appn=detail&action=data&c={code}&p={p}"


def data_dir() -> Path:
    env = os.getenv("QM_QUANTDB_DATA_DIR", "").strip()
    return Path(env) if env else PROJECT_ROOT / "data" / "quantdb"


def to_tencent(symbol: str) -> str:
    code, _, suffix = symbol.partition(".")
    return f"{suffix.lower()}{code}"


def universe(limit: int | None = None) -> list[str]:
    parts = sorted(glob.glob(str(data_dir() / "6_ml_datasets/factor_dictionary/dt=*")))
    if not parts:
        raise SystemExit("找不到 factor_dictionary 分区，改用 --symbols")
    f = glob.glob(parts[-1] + "/*.parquet")[0]
    syms = pd.read_parquet(f, columns=["symbol"])["symbol"].drop_duplicates().tolist()
    syms = sorted(s for s in syms if not s.endswith(".BJ"))
    return syms[:limit] if limit else syms


def fetch_ticks(symbol: str, session: requests.Session, sleep: float = 0.05,
                max_page: int = 300) -> dict | None:
    """逐页取当日分笔，返回笔数/金额/手数。"""
    code = to_tencent(symbol)
    n, amt, vol = 0, 0.0, 0.0
    for p in range(1, max_page + 1):
        r = session.get(URL.format(code=code, p=p), timeout=20)
        m = re.search(r"=\[(.*)\];?", r.text, re.S)
        if not m:
            break
        parts = re.findall(r'"([^"]*)"', m.group(1))
        if not parts:
            break
        recs = [x for x in parts[0].split("|") if x]
        if not recs:
            break
        n += len(recs)
        for x in recs:
            f = x.split("/")
            if len(f) >= 6:
                try:
                    # 腾讯分笔第 5 字段是「手」，×100 转成股，与 daily/min1/min5 口径一致
                    vol += float(f[4]) * 100.0
                    amt += float(f[5])
                except ValueError:
                    pass
        if len(recs) < 60:
            break
        time.sleep(sleep)
    if n == 0:
        return None
    return {"symbol": symbol, "num_trades": n, "volume": vol, "amount": amt,
            "avg_trade_amt": amt / n if n else float("nan")}


def last_trade_date(symbol: str) -> str:
    """从行情快照取「最近交易日」，避免周末/节假日把数据记到错误日期下。"""
    code = to_tencent(symbol)
    r = requests.get(f"https://qt.gtimg.cn/q={code}", headers=UA, timeout=10)
    r.encoding = "gbk"
    f = r.text.split("~")
    if len(f) > 31 and re.fullmatch(r"\d{14}", f[30].strip()):
        return datetime.strptime(f[30].strip()[:8], "%Y%m%d").date().isoformat()
    return date.today().isoformat()


def main() -> None:
    ap = argparse.ArgumentParser(description="腾讯分笔 → 每日成交笔数")
    ap.add_argument("--all", action="store_true", help="全市场")
    ap.add_argument("--symbols", default=None)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--sleep", type=float, default=0.05, help="翻页间隔（秒）")
    ap.add_argument("--date", default=None, help="落库日期，缺省用今天")
    args = ap.parse_args()

    syms = ([s.strip() for s in args.symbols.split(",") if s.strip()]
            if args.symbols else universe(args.limit))
    if not syms:
        raise SystemExit("标的为空，用 --all 或 --symbols")

    dst = data_dir() / "5_technical_derived" / "trade_count_daily.parquet"
    d = args.date or last_trade_date(syms[0])
    log.info("落库交易日 = %s（快照探测）", d)
    have = set()
    if dst.exists():
        old = pd.read_parquet(dst)
        have = set(old.loc[old["date"] == d, "symbol"])
        log.info("已有 %d 条 %s 记录，跳过", len(have), d)
    else:
        old = pd.DataFrame()

    todo = [s for s in syms if s not in have]
    log.info("待抓 %d 只（%d 并发，翻页间隔 %.2fs）", len(todo), args.workers, args.sleep)
    t0, rows, failed = time.time(), [], 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {}
        for s in todo:
            sess = requests.Session()
            sess.headers.update(UA)
            futs[ex.submit(fetch_ticks, s, sess, args.sleep)] = s
        for i, f in enumerate(as_completed(futs), 1):
            try:
                rec = f.result()
            except Exception as e:  # noqa: BLE001
                failed += 1
                log.debug("失败 %s: %s", futs[f], e)
                rec = None
            if rec:
                rec["date"] = d
                rows.append(rec)
            if i % 300 == 0:
                log.info("进度 %d/%d，成功 %d，失败 %d，已用 %.0fs",
                         i, len(todo), len(rows), failed, time.time() - t0)

    if not rows:
        log.warning("没有取到数据（非交易日？或接口变更）")
        return
    out = pd.DataFrame(rows)[["symbol", "date", "num_trades", "avg_trade_amt",
                              "volume", "amount"]]
    if len(old):
        out = (pd.concat([old, out], ignore_index=True)
               .drop_duplicates(subset=["symbol", "date"], keep="last"))
    dst.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(dst, index=False)
    log.info("完成：新增 %d 只，累计 %d 行 → %s（耗时 %.0fs）",
             len(rows), len(out), dst, time.time() - t0)
    log.info("落库时间戳 %s", datetime.now().isoformat(timespec="seconds"))


if __name__ == "__main__":
    main()
