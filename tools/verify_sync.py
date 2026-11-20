# -*- coding: utf-8 -*-
"""同步验收脚本（还原版，2026-11-20）：打印 QuantDB 分钟 + exday 当日覆盖。

用法:
    python verify_sync.py [YYYYMMDD]     # 缺省=当天
"""
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

QUANTDB_DIR = Path(r"D:\gitProject\QuantMind\data\quantdb")
EXDAY_FILE = QUANTDB_DIR / "5_technical_derived" / "tdx_exday_daily.parquet"


def main():
    today = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y%m%d")
    print(f"=== 同步验收 {today} ===")
    for kind in ("min1", "min5"):
        d = QUANTDB_DIR / "1_kline_data" / f"{kind}_kline" / f"dt={today}"
        f = d / "data.parquet"
        if not f.exists():
            print(f"[{kind}] dt={today}: [MISS] 无分区")
            continue
        df = pd.read_parquet(f)
        marked = "✓" if (d / ".tdxlocal").exists() else "✗ 无标记"
        print(f"[{kind}] dt={today}: {len(df)} 行 / {df['symbol'].nunique()} 只 "
              f"[{marked}]  time {df['time'].min()} → {df['time'].max()}")
        if kind == "min1":
            s = df[df["symbol"] == "000001.SZ"]
            if len(s):
                print(f"    000001.SZ: {len(s)} 根 {s['time'].min():%H:%M}→{s['time'].max():%H:%M} "
                      f"vol_sum={s['volume'].sum():,}")
    d = QUANTDB_DIR / "1_kline_data" / "daily_unadjusted" / f"dt={today}"
    f = d / "data.parquet"
    if not f.exists():
        print(f"[daily] dt={today}: [MISS] 无分区")
    else:
        df = pd.read_parquet(f)
        marked = "✓" if (d / ".tdxlocal").exists() else "✗ 无标记"
        print(f"[daily] dt={today}: {len(df)} 行 / {df['symbol'].nunique()} 只 [{marked}]")
        s = df[df["symbol"] == "000001.SZ"]
        if len(s):
            r = s.iloc[0]
            print(f"    000001.SZ: O={r['open']} H={r['high']} L={r['low']} C={r['close']} "
                  f"vol={r['volume']:,.0f} amt={r['amount']:,.0f}万")
    if EXDAY_FILE.exists():
        df = pd.read_parquet(EXDAY_FILE)
        date_col = "date" if "date" in df.columns else "trade_date"
        day = df[df[date_col].astype(str) == today]
        if day.empty:
            print(f"[exday] dt={today}: [MISS] 无数据")
        else:
            trades_col = "num_trades" if "num_trades" in day.columns else "CJBS"
            cjbs_pos = (day[trades_col].fillna(0) > 0).sum()
            print(f"[exday] dt={today}: {len(day)} 只, {trades_col}>0 {cjbs_pos} 只, "
                  f"src={day['src'].iloc[0]}")
    else:
        print(f"[exday] [MISS] {EXDAY_FILE} 不存在")


if __name__ == "__main__":
    main()
