# -*- coding: utf-8 -*-
"""通达信 get_exday_data → L2 日统计 parquet 同步工具（还原版，2026-11-20）。

用法:
    python tdx_exday_daily.py                # 全市场当日（默认）
    python tdx_exday_daily.py --date 20261120

行为约定（与既有湖表 schema 对齐，26 列）:
- 数据源: tqcenter HTTP JSON-RPC (127.0.0.1:17709) get_exday_data，免 Key
- 输出:   <QUANTDB>/5_technical_derived/tdx_exday_daily.parquet（单文件累积，追加+去重）
- 主键 (symbol, date) 去重 keep=last，src=tdx_exday
- 列: symbol, date, num_trades, volume, amount, avg_trade_amt,
      vol_xlarge/large/mid/small, amt_xlarge/large/mid/small,
      big_vol_ratio, big_amt_ratio, buy_trades, sell_trades,
      border_vol, bcancel_vol, sorder_vol, scancel_vol,
      border_num, sorder_num, cancel_rate, src
"""
import argparse
import json
import sys
import time
import urllib.request
from pathlib import Path

import pandas as pd

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TQ_URL = "http://127.0.0.1:17709/"
QUANTDB_DIR = Path(r"D:\gitProject\QuantMind\data\quantdb")
OUT_FILE = QUANTDB_DIR / "5_technical_derived" / "tdx_exday_daily.parquet"


def tq_call(method: str, params: dict, timeout: float = 10.0, retries: int = 2):
    body = json.dumps({"id": 1, "method": method, "params": params}).encode("utf-8")
    last = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(TQ_URL, data=body,
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                result = json.loads(resp.read().decode("utf-8"))
            if "error" in result:
                raise RuntimeError(f"{method}: {result['error']}")
            inner = result.get("result", {})
            if str(inner.get("ErrorId", "0")) not in ("0", "-1"):
                raise RuntimeError(f"{method} ErrorId={inner.get('ErrorId')}: {inner.get('Msg')}")
            return inner
        except Exception as e:  # noqa: BLE001
            last = e
            if attempt < retries:
                time.sleep(0.5 * (2 ** attempt))
    raise RuntimeError(f"{method} 调用失败: {last}")


def all_stock_codes() -> list:
    """全市场股票代码（后缀式）。兼容 Value 为字符串列表或 dict 列表两种返回。"""
    r = tq_call("get_stock_list", {"market": None, "list_type": 0}, timeout=30.0)
    codes = []
    for item in r.get("Value", []):
        if isinstance(item, str):
            code = item
        elif isinstance(item, dict):
            code = item.get("Code", "")
        else:
            continue
        if "." in code:
            codes.append(code)
    return sorted(set(codes))


def fetch_one(code: str, count: int = 1) -> list:
    r = tq_call("get_exday_data", {"stock_code": code, "count": count})
    return r.get("Value", []) if isinstance(r.get("Value"), list) else []


def to_rows(code: str, entries: list) -> list:
    """get_exday_data 原始条目 → 湖表 26 列行。

    原始字段: Date, CJBS(成交笔数), Amo/Vol/VolNum(四档 [[小单,大单合计]] 或 4 档),
    BOrder/SOrder(委买卖量), BCancel/SCancel(撤单量), BuyAvp/SellAvp,
    TotalBOrder/TotalSOrder(委买卖笔数)。
    映射: num_trades=CJBS; buy_trades=CJBS×BuyAvp 占比简化为买卖笔数
    （无直接字段时用 CJBS 平分）; border/sorder/cancel 为量;
    border_num/sorder_num 用 TotalBOrder/TotalSOrder。
    """
    rows = []
    for e in entries:
        date = str(e.get("Date", ""))
        if len(date) != 8 or not date.isdigit():
            continue
        num = lambda k: (lambda v: float(v) if v not in (None, "") else None)(e.get(k))
        cjbs = num("CJBS") or 0.0
        vol4 = [float(x) for x in (e.get("Vol") or [[]])[0][:4]] if e.get("Vol") else []
        amo4 = [float(x) for x in (e.get("Amo") or [[]])[0][:4]] if e.get("Amo") else []
        volnum4 = [float(x) for x in (e.get("VolNum") or [[]])[0][:4]] if e.get("VolNum") else []
        # 四档补齐到 4 位（不足时后位补 0）
        pad = lambda lst: (lst + [0.0] * 4)[:4]  # noqa: E731
        vol4, amo4, volnum4 = pad(vol4), pad(amo4), pad(volnum4)
        vol_total = sum(vol4) or None
        amo_total = sum(amo4) or None
        buy_avp = num("BuyAvp") or 0.0
        sell_avp = num("SellAvp") or 0.0
        # 买卖笔数：按买卖均价权重近似拆分（无精确字段）
        if buy_avp + sell_avp > 0:
            buy_trades = cjbs * buy_avp / (buy_avp + sell_avp)
            sell_trades = cjbs - buy_trades
        else:
            buy_trades = sell_trades = cjbs / 2 if cjbs else 0.0
        border_vol = num("BOrder") or 0.0
        sorder_vol = num("SOrder") or 0.0
        bcancel_vol = num("BCancel") or 0.0
        scancel_vol = num("SCancel") or 0.0
        order_vol = border_vol + sorder_vol
        cancel_vol = bcancel_vol + scancel_vol
        rows.append({
            "symbol": code,
            "date": date,
            "num_trades": cjbs,
            "volume": vol_total,
            "amount": amo_total,
            "avg_trade_amt": (amo_total / cjbs) if (amo_total and cjbs) else None,
            "vol_xlarge": vol4[0], "vol_large": vol4[1],
            "vol_mid": vol4[2], "vol_small": vol4[3],
            "amt_xlarge": amo4[0], "amt_large": amo4[1],
            "amt_mid": amo4[2], "amt_small": amo4[3],
            "big_vol_ratio": ((vol4[0] + vol4[1]) / vol_total) if vol_total else None,
            "big_amt_ratio": ((amo4[0] + amo4[1]) / amo_total) if amo_total else None,
            "buy_trades": buy_trades,
            "sell_trades": sell_trades,
            "border_vol": border_vol,
            "bcancel_vol": bcancel_vol,
            "sorder_vol": sorder_vol,
            "scancel_vol": scancel_vol,
            "border_num": num("TotalBOrder"),
            "sorder_num": num("TotalSOrder"),
            "cancel_rate": (cancel_vol / order_vol) if order_vol else None,
            "src": "tdx_exday",
        })
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--date", default=None, help="只保留指定交易日 YYYYMMDD")
    ap.add_argument("--count", type=int, default=1, help="每只股票回溯条数")
    args = ap.parse_args()

    codes = all_stock_codes()
    print(f"全市场股票数: {len(codes)}")
    all_rows = []
    for i, code in enumerate(codes, 1):
        try:
            rows = to_rows(code, fetch_one(code, args.count))
        except Exception as e:  # noqa: BLE001
            print(f"  ! {code}: {e}")
            continue
        if args.date:
            rows = [r for r in rows if r["trade_date"] == args.date]
        all_rows.extend(rows)
        if i % 500 == 0:
            print(f"  ... {i}/{len(codes)}")
    if not all_rows:
        print("无新数据")
        return
    df = pd.DataFrame(all_rows)
    # 去重：主键 (symbol, date)，keep=last
    df = df.drop_duplicates(subset=["symbol", "date"], keep="last")
    # 合并历史（列对齐到并集）
    if OUT_FILE.exists():
        old = pd.read_parquet(OUT_FILE)
        df = pd.concat([old, df], ignore_index=True)
        df = df.drop_duplicates(subset=["symbol", "date"], keep="last")
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_FILE, index=False)
    dates = sorted(df["date"].unique())
    print(f"→ {OUT_FILE}  total_rows={len(df)}  latest_dates={dates[-3:]}  src=tdx_exday")


if __name__ == "__main__":
    main()
