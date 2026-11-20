#!/usr/bin/env python3
"""从 Wind L2 tick 快照抽取「每日成交笔数」— 供理想反转（单笔成交额）使用。

前置： Wind L2 7z 已由 backend/scripts/wind_l2_import.py 导入，
       落盘 data/quantdb/1_kline_data/tick_data/{symbol}_{YYYYMMDD}.parquet
       其中 transactionNum（= 万得「成交笔数」）为当日**累计**笔数。

产出： data/quantdb/5_technical_derived/trade_count_daily.parquet
       symbol, date, num_trades, avg_trade_amt, volume, amount
       num_trades   = 当日最后一笔快照的 transactionNum（当日累计笔数）
       avg_trade_amt= amount*1e4 / num_trades（元/笔，理想反转的排序键）

用法:
    python backend/scripts/l2_ticks_daily.py --start 2026-01-01
    python backend/scripts/l2_ticks_daily.py --symbols 600519.SH --start 2026-10-01
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import re
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("l2_ticks_daily")

NAME_RE = re.compile(r"^(?P<sym>[0-9]{6}_(?:SH|SZ|BJ))_(?P<date>[0-9]{8})$")


def data_dir() -> Path:
    env = os.getenv("QM_QUANTDB_DATA_DIR", "").strip()
    return Path(env) if env else PROJECT_ROOT / "data" / "quantdb"


def to_suffix(sym: str) -> str:
    """000001_SZ → 000001.SZ"""
    code, _, mk = sym.partition("_")
    return f"{code}.{mk}"


def scan(start: str, end: str, symbols: set[str] | None) -> list[Path]:
    files = glob.glob(str(data_dir() / "1_kline_data/tick_data/*.parquet"))
    keep = []
    for f in files:
        m = NAME_RE.match(Path(f).stem)
        if not m:
            continue
        d = f"{m['date'][:4]}-{m['date'][4:6]}-{m['date'][6:]}"
        if not (start <= d <= end):
            continue
        if symbols and to_suffix(m["sym"]) not in symbols:
            continue
        keep.append(Path(f))
    return keep


def one_day(path: Path) -> dict | None:
    """单只单日：取最后一笔快照的累计笔数与累计成交额。"""
    df = pd.read_parquet(path, columns=["time", "transactionNum", "volume", "amount"])
    if df.empty:
        return None
    df = df.sort_values("time")
    last = df.iloc[-1]
    n = float(last.get("transactionNum") or 0)
    if n <= 0:
        return None
    amt = float(last.get("amount") or 0)  # 元
    return {
        "num_trades": int(n),
        "volume": float(last.get("volume") or 0),
        "amount": amt,
        "avg_trade_amt": amt / n if n else float("nan"),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Wind L2 tick → 每日成交笔数")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", default="2099-12-31")
    ap.add_argument("--symbols", default=None, help="逗号分隔后缀式，缺省全市场")
    args = ap.parse_args()

    syms = ({s.strip() for s in args.symbols.split(",") if s.strip()}
            if args.symbols else None)
    files = scan(args.start, args.end, syms)
    if not files:
        log.warning("区间内没有 tick_data 文件；先用 wind_l2_import.py 导入 7z 包")
        return

    rows = []
    for i, f in enumerate(files, 1):
        m = NAME_RE.match(f.stem)
        rec = one_day(f)
        if rec:
            rows.append({"symbol": to_suffix(m["sym"]),
                         "date": f"{m['date'][:4]}-{m['date'][4:6]}-{m['date'][6:]}",
                         **rec})
        if i % 500 == 0:
            log.info("已处理 %d/%d", i, len(files))

    out = pd.DataFrame(rows)
    if out.empty:
        log.warning("没有可用记录")
        return
    dst = data_dir() / "5_technical_derived" / "trade_count_daily.parquet"
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        out = (pd.concat([pd.read_parquet(dst), out], ignore_index=True)
               .drop_duplicates(subset=["symbol", "date"], keep="last"))
    out.to_parquet(dst, index=False)
    log.info("完成：%d 行 → %s", len(out), dst)


if __name__ == "__main__":
    main()
