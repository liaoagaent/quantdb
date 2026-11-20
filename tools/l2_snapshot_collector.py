# -*- coding: utf-8 -*-
"""盘中 L2 快照采集器（tqcenter HTTP 桥版）。

落盘约定（对齐 QuantDB 湖 l2_snapshot jsonl 风格）：
  data/quantdb/1_kline_data/l2_snapshot/dt=YYYYMMDD/round=001/batch=NNN.jsonl
jsonl 每行一个 JSON 对象：
  首行 meta : {"type":"meta","batch_id":"...","trade_date":"20261120",
               "captured_at":"...ISO...","count":N,"source":"tdx_bridge"}
  数据行    : {"type":"quote","symbol":"000001.SZ","captured_at":"...",
               "Now":...,"Open":...,"High":...,"Low":...,"PreClose":...,
               "Volume":手,"Amount":万元,"NowVol":...,"Average":...,
               "Inside":...,"Outside":...,
               "Buyp":[买1..买5价],"Buyv":[买1..买5量],
               "Sellp":[卖1..卖5价],"Sellv":[卖1..卖5量]}

粒度说明：tqcenter 无逐笔/十档接口；本采集器取「实时快照 + 买卖五档」，
是现有通道（127.0.0.1:17709）能拿到的最细盘中粒度。

用法：
  python tools\\l2_snapshot_collector.py --once --stocks 600519.SH,000001.SZ
  python tools\\l2_snapshot_collector.py                # 交易时段内每 30s 一轮全市场
  python tools\\l2_snapshot_collector.py --interval 60 --stocks自选池
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
import urllib.request
from pathlib import Path

BRIDGE = "http://127.0.0.1:17709/"
OUT_ROOT = Path(r"D:\gitProject\QuantMind\data\quantdb\1_kline_data\l2_snapshot")

_session = {"id": 0}


def tq_call(method: str, params: dict, timeout: float = 10.0, retries: int = 2):
    """POST JSON-RPC 到本机 tqcenter 桥；失败重试。"""
    last = None
    for i in range(retries + 1):
        try:
            _session["id"] += 1
            body = json.dumps({"id": _session["id"], "method": method,
                               "params": params}, ensure_ascii=False).encode("utf-8")
            req = urllib.request.Request(BRIDGE, data=body, headers={
                "Content-Type": "application/json; charset=utf-8"}, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                r = json.loads(resp.read().decode("utf-8"))
            inner = r.get("result", {})
            if str(inner.get("ErrorId", "0")) not in ("0", "-1"):
                raise RuntimeError(f"{method} ErrorId={inner.get('ErrorId')}: {inner.get('Msg')}")
            return inner
        except Exception as e:  # noqa: BLE001
            last = e
            if i < retries:
                time.sleep(0.5 * (i + 1))
    raise RuntimeError(f"{method} 调用失败: {last}")


def all_stock_codes() -> list[str]:
    """全市场 A 股代码（get_stock_list 返回字符串列表）。"""
    r = tq_call("get_stock_list", {"market": None, "list_type": 0})
    val = r.get("Value", [])
    if isinstance(val, dict):
        val = val.get("Value", [])
    out = []
    for s in val or []:
        code = str(s).strip().upper()
        if len(code) >= 9 and "." in code:
            out.append(code)
    return out


def is_trading_time(now: dt.datetime | None = None) -> bool:
    now = now or dt.datetime.now()
    if now.weekday() >= 5:
        return False
    t = now.time()
    morning = dt.time(9, 15) <= t <= dt.time(11, 30)
    afternoon = dt.time(13, 0) <= t <= dt.time(15, 0)
    return morning or afternoon


def _f(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def snapshot_row(code: str, captured_at: str) -> dict | None:
    """单只股票：实时快照 + 买卖五档 → quote 行。"""
    snap = tq_call("get_market_snapshot", {"stock_code": code})
    if _f(snap.get("Now")) <= 0:
        return None
    return {
        "type": "quote",
        "symbol": code,
        "captured_at": captured_at,
        "Now": _f(snap.get("Now")),
        "Open": _f(snap.get("Open")),
        "High": _f(snap.get("Max")),
        "Low": _f(snap.get("Min")),
        "PreClose": _f(snap.get("LastClose")),
        "Volume": _f(snap.get("Volume")),        # 手
        "Amount": _f(snap.get("Amount")),        # 万元
        "NowVol": _f(snap.get("NowVol")),
        "Average": _f(snap.get("Average")),
        "Inside": _f(snap.get("Inside")),        # 内盘
        "Outside": _f(snap.get("Outside")),      # 外盘
        "Buyp": [str(x) for x in snap.get("Buyp", [])],   # 买1..买5 价
        "Buyv": [str(x) for x in snap.get("Buyv", [])],   # 买1..买5 量
        "Sellp": [str(x) for x in snap.get("Sellp", [])], # 卖1..卖5 价
        "Sellv": [str(x) for x in snap.get("Sellv", [])], # 卖1..卖5 量
    }


def next_batch_path(trade_date: str) -> Path:
    """当日 round=001 下取下一个 batch 序号（从 001 起，幂等递增）。"""
    day_dir = OUT_ROOT / f"dt={trade_date}" / "round=001"
    day_dir.mkdir(parents=True, exist_ok=True)
    seq = 1
    for p in day_dir.glob("batch=*.jsonl"):
        try:
            seq = max(seq, int(p.stem.split("=")[1]) + 1)
        except (IndexError, ValueError):
            continue
    return day_dir / f"batch={seq:03d}.jsonl"


def collect_once(stocks: list[str], per_call_sleep: float = 0.05,
                 batch_size: int = 200) -> list[Path]:
    """采一轮：股票池按 batch_size(默认200) 切批，每批落一个 batch jsonl。

    对齐旧版「200 只/批」逻辑：5,584 只 → 每轮约 28 个 batch 文件，
    batch 序号当日全局递增，重跑不覆盖。
    """
    trade_date = dt.datetime.now().strftime("%Y%m%d")
    nbatches = (len(stocks) + batch_size - 1) // batch_size
    paths: list[Path] = []
    total_ok = total_fail = 0
    for bi, start in enumerate(range(0, len(stocks), batch_size), 1):
        chunk = stocks[start:start + batch_size]
        out = next_batch_path(trade_date)
        captured = dt.datetime.now().isoformat(timespec="seconds")
        ok, fail = 0, 0
        with out.open("w", encoding="utf-8") as f:
            meta = {"type": "meta", "batch_id": out.stem, "trade_date": trade_date,
                    "captured_at": captured, "count": len(chunk),
                    "seq_in_round": bi, "total_batches": nbatches,
                    "source": "tdx_bridge", "granularity": "snapshot+5level"}
            f.write(json.dumps(meta, ensure_ascii=False) + "\n")
            for code in chunk:
                ts = dt.datetime.now().isoformat(timespec="milliseconds")
                try:
                    row = snapshot_row(code, ts)
                    if row:
                        f.write(json.dumps(row, ensure_ascii=False) + "\n")
                        ok += 1
                    else:
                        fail += 1
                except Exception:  # noqa: BLE001
                    fail += 1
                time.sleep(per_call_sleep)
        total_ok += ok
        total_fail += fail
        paths.append(out)
        print(f"  批次 {bi}/{nbatches} → {out.name}  ok={ok} fail={fail}", flush=True)
    print(f"→ 本轮 {nbatches} 批 / {len(stocks)} 只  ok={total_ok} fail={total_fail}", flush=True)
    return paths


def wait_for_open() -> None:
    """非交易时段休眠至开盘（每 30s 醒一次，便于 Ctrl+C）。"""
    while not is_trading_time():
        now = dt.datetime.now()
        nxt = now.replace(hour=9, minute=15, second=0, microsecond=0)
        if now.time() > dt.time(15, 0) or now.weekday() >= 5:
            nxt += dt.timedelta(days=1)
            while nxt.weekday() >= 5:
                nxt += dt.timedelta(days=1)
        if now.time() > dt.time(11, 30) and now.time() < dt.time(13, 0):
            nxt = now.replace(hour=13, minute=0, second=0, microsecond=0)
        wait = max(1, int((nxt - now).total_seconds()))
        print(f"\r非交易时段，距开盘约 {wait}s ...", end="", flush=True)
        time.sleep(min(wait, 30))
    print()


def main() -> None:
    ap = argparse.ArgumentParser(description="盘中 L2 快照采集器（快照+五档）")
    ap.add_argument("--interval", type=int, default=30, help="轮间隔秒数（默认30）")
    ap.add_argument("--rounds", type=int, default=0, help="轮数上限，0=无限")
    ap.add_argument("--stocks", type=str, default="", help="逗号分隔股票池；缺省全市场")
    ap.add_argument("--once", action="store_true", help="只采一轮（测试）")
    args = ap.parse_args()

    stocks = [s.strip().upper() for s in args.stocks.split(",") if s.strip()] \
        if args.stocks else all_stock_codes()
    if not stocks:
        print("股票列表为空，退出")
        sys.exit(1)
    print(f"股票池: {len(stocks)} 只 | interval={args.interval}s | once={args.once}")

    rnd = 0
    while True:
        if not is_trading_time() and not args.once:
            wait_for_open()
        rnd += 1
        print(f"\n===== 第 {rnd} 轮 {dt.datetime.now().strftime('%H:%M:%S')} =====")
        try:
            collect_once(stocks)
        except Exception as e:  # noqa: BLE001
            print(f"本轮失败: {type(e).__name__}: {e}", flush=True)
        if args.once or (args.rounds and rnd >= args.rounds):
            break
        time.sleep(max(1, args.interval))


if __name__ == "__main__":
    main()
