#!/usr/bin/env python3
"""5 分钟线同步 — 为「聪明钱 / APM」类因子补齐分钟级数据。

两个源，分工明确：
  - em（东财 push2his）：单请求返回 **1488 根 ≈ 31 交易日**，3 并发 0.31s/只，
    全市场约 27 分钟 → 用于**每日增量**（覆盖 20 日因子窗口）。
  - baostock：2020 年至今全历史，但单只 5s、5000 只约 6 小时 → 只用于**一次性回补**。

落盘：data/quantdb/1_kline_data/minute_5m/{symbol}.parquet
      列 date, time(YYYYMMDDHHMMSS000), open, high, low, close, volume(股), amount(元), symbol

派生（每次同步后自动执行，或 --derive 单独跑）：
      data/quantdb/5_technical_derived/minute_derived.parquet
      pm_open     13:00 价（13:05 bar 的 open）
      vwap_all    全日成交量加权均价
      vwap_top20  成交量累计占比前 20% 的 bar 的 VWAP  ← 聪明钱主项

用法:
    python backend/scripts/minute_5m_sync.py --mode daily            # 每日增量（东财）
    python backend/scripts/minute_5m_sync.py --mode backfill --source baostock --start 2020-01-01
    python backend/scripts/minute_5m_sync.py --symbols 600519.SH,000001.SZ --start 2026-10-01
    python backend/scripts/minute_5m_sync.py --derive                # 只做派生
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("minute_5m_sync")

FIELDS = "date,time,open,high,low,close,volume,amount"
EM_FIELDS = "f51,f52,f53,f54,f55,f56,f57"  # 时间,开,收,高,低,量,额
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0 Safari/537.36",
      "Referer": "https://quote.eastmoney.com/"}
_LOCK = threading.Lock()
_LAST = [0.0]
MIN_INTERVAL = 0.18  # 东财节流：单进程串行 ≥0.18s，多线程共享


def data_dir() -> Path:
    env = os.getenv("QM_QUANTDB_DATA_DIR", "").strip()
    return Path(env) if env else PROJECT_ROOT / "data" / "quantdb"


def to_baostock(symbol: str) -> str:
    code, _, suffix = symbol.partition(".")
    return f"{suffix.lower()}.{code}"


def to_secid(symbol: str) -> str:
    code, _, suffix = symbol.partition(".")
    return f"{'1' if suffix.upper() == 'SH' else '0'}.{code}"


def to_tencent(symbol: str) -> str:
    code, _, suffix = symbol.partition(".")
    return f"{suffix.lower()}{code}"


def universe(limit: int | None = None) -> list[str]:
    """因子字典最新分区的全市场标的（后缀式），剔除 baostock/东财口径不兼容的北交所。"""
    parts = sorted(glob.glob(str(data_dir() / "6_ml_datasets/factor_dictionary/dt=*")))
    if not parts:
        raise SystemExit("找不到 factor_dictionary 分区，改用 --symbols 显式指定")
    latest = glob.glob(parts[-1] + "/*.parquet")
    syms = pd.read_parquet(latest[0], columns=["symbol"])["symbol"].drop_duplicates().tolist()
    syms = sorted(s for s in syms if not s.endswith(".BJ"))
    return syms[:limit] if limit else syms


def _throttle() -> None:
    with _LOCK:
        wait = MIN_INTERVAL - (time.time() - _LAST[0])
        if wait > 0:
            time.sleep(wait)
        _LAST[0] = time.time()


def _save(symbol: str, new: pd.DataFrame, out_dir: str) -> int:
    if new.empty:
        return 0
    path = Path(out_dir) / f"{symbol}.parquet"
    if path.exists():
        old = pd.read_parquet(path)
        merged = (pd.concat([old, new], ignore_index=True)
                  .drop_duplicates(subset=["time"], keep="last")
                  .sort_values("time"))
        added = len(merged) - len(old)
    else:
        merged, added = new.sort_values("time"), len(new)
    merged.to_parquet(path, index=False)
    return added


def _volume_multiplier(vol: pd.Series, amt: pd.Series, close: pd.Series) -> float:
    """判定「手」换「股」的乘数（主板 100、科创板 200、已是股则 1）。"""
    best, best_err = 100.0, float("inf")
    for m in (100.0, 200.0, 1.0):
        implied = amt / (vol * m).replace(0, pd.NA)
        err = float((implied - close).abs().median())
        if err < best_err:
            best, best_err = m, err
    return best


def fetch_em(symbol: str, lmt: int = 2000) -> pd.DataFrame:
    """东财 5 分钟线：单请求 1488 根 ≈ 31 交易日。"""
    for attempt in range(3):
        _throttle()
        try:
            r = requests.get(
                "https://push2his.eastmoney.com/api/qt/stock/kline/get",
                params={"secid": to_secid(symbol), "klt": 5, "fqt": 1, "beg": 0,
                        "end": 20500101, "lmt": lmt, "fields1": "f1,f2,f3,f4,f5",
                        "fields2": EM_FIELDS},
                headers=UA, timeout=20)
            kl = (r.json().get("data") or {}).get("klines") or []
            if not kl:
                continue
            rows = [k.split(",") for k in kl]
            df = pd.DataFrame(rows, columns=["dt", "open", "close", "high", "low",
                                             "volume", "amount"])
            for c in ("open", "close", "high", "low", "volume", "amount"):
                df[c] = pd.to_numeric(df[c], errors="coerce")
            mult = _volume_multiplier(df["volume"], df["amount"], df["close"])
            df["volume"] = (df["volume"] * mult).astype("int64")
            dt = pd.to_datetime(df["dt"], format="%Y-%m-%d %H:%M")
            df["date"] = dt.dt.strftime("%Y-%m-%d")
            df["time"] = dt.dt.strftime("%Y%m%d%H%M%S") + "000"
            df["symbol"] = symbol
            return df[FIELDS.split(",") + ["symbol"]]
        except Exception:  # noqa: BLE001 - 重试后放弃该只
            time.sleep(0.8 * (attempt + 1))
    return pd.DataFrame()


def fetch_tencent(symbol: str, freq: str = "15") -> pd.DataFrame:
    """腾讯分钟线：单请求固定 320 根（m15=15 交易日，m5=6.7 日），不封 IP。

    m15 是东财/baostock 都不好用时的兜底：粒度粗，但覆盖 10 日聪明钱窗口。
    腾讯不给成交额（第 8 字段是换手率基点），用 volume×close 近似。
    """
    for attempt in range(3):
        try:
            r = requests.get(
                "https://ifzq.gtimg.cn/appstock/app/kline/mkline"
                f"?param={to_tencent(symbol)},m{freq},,320",
                headers=UA, timeout=15)
            d = r.json()["data"][to_tencent(symbol)][f"m{freq}"]
            if not d:
                continue
            df = pd.DataFrame([x[:6] for x in d],
                              columns=["t", "open", "close", "high", "low", "volume"])
            for c in ("open", "close", "high", "low", "volume"):
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df["volume"] = (df["volume"] * 100).astype("int64")  # 手→股，乘数不影响比值
            df["amount"] = df["volume"] * df["close"]            # VWAP 近似
            dt = pd.to_datetime(df["t"], format="%Y%m%d%H%M")
            df["date"] = dt.dt.strftime("%Y-%m-%d")
            df["time"] = dt.dt.strftime("%Y%m%d%H%M%S") + "000"
            df["symbol"] = symbol
            return df[FIELDS.split(",") + ["symbol"]]
        except Exception:  # noqa: BLE001
            time.sleep(0.3 * (attempt + 1))
    return pd.DataFrame()


def sync_tencent_one(symbol: str, out_dir: str, freq: str = "15") -> tuple:
    try:
        return symbol, _save(symbol, fetch_tencent(symbol, freq), out_dir), None
    except Exception as e:  # noqa: BLE001
        return symbol, 0, f"{type(e).__name__}: {e}"


def sync_em_one(symbol: str, out_dir: str) -> tuple:
    try:
        return symbol, _save(symbol, fetch_em(symbol), out_dir), None
    except Exception as e:  # noqa: BLE001
        return symbol, 0, f"{type(e).__name__}: {e}"


def sync_bs_one(symbol: str, start: str, end: str, out_dir: str) -> tuple:
    import baostock as bs

    try:
        if bs.login().error_code != "0":
            return symbol, 0, "baostock login failed"
        rs = bs.query_history_k_data_plus(to_baostock(symbol), FIELDS, start_date=start,
                                          end_date=end, frequency="5", adjustflag="3")
        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())
        bs.logout()
        if not rows:
            return symbol, 0, "empty"
        new = pd.DataFrame(rows, columns=FIELDS.split(","))
        new["symbol"] = symbol
        for c in ("open", "high", "low", "close", "volume", "amount"):
            new[c] = pd.to_numeric(new[c], errors="coerce")
        return symbol, _save(symbol, new, out_dir), None
    except Exception as e:  # noqa: BLE001
        return symbol, 0, f"{type(e).__name__}: {e}"


def run_sync(symbols: list[str], start: str, end: str, workers: int, source: str,
             freq: str = "5") -> None:
    # 落盘目录带频率后缀：em / baostock 恒为 5m，tencent 跟随 --freq，避免混频率污染
    bar = "5" if source in ("em", "baostock") else freq
    out_dir = str(data_dir() / "1_kline_data" / f"minute_{bar}m")
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    log.info("标的 %d 只 → %s（源=%s，%s ~ %s，%d 并发）",
             len(symbols), out_dir, source, start, end, workers)
    t0, done, failed, total_rows = time.time(), 0, [], 0

    if source in ("em", "tencent"):
        submit = ((lambda s: ex.submit(sync_em_one, s, out_dir)) if source == "em"
                  else (lambda s: ex.submit(sync_tencent_one, s, out_dir, freq)))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [submit(s) for s in symbols]
            for f in as_completed(futs):
                sym, added, err = f.result()
                done += 1
                total_rows += added
                if err:
                    failed.append((sym, err))
                if done % 200 == 0 or done == len(symbols):
                    el = time.time() - t0
                    log.info("进度 %d/%d  新增 %d 行  失败 %d  已用 %.0fs  预计剩余 %.0fs",
                             done, len(symbols), total_rows, len(failed), el,
                             el / done * (len(symbols) - done))
    else:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(sync_bs_one, s, start, end, out_dir) for s in symbols]
            for f in as_completed(futs):
                sym, added, err = f.result()
                done += 1
                total_rows += added
                if err:
                    failed.append((sym, err))
                if done % 50 == 0 or done == len(symbols):
                    el = time.time() - t0
                    log.info("进度 %d/%d  新增 %d 行  失败 %d  已用 %.0fs  预计剩余 %.0fs",
                             done, len(symbols), total_rows, len(failed), el,
                             el / done * (len(symbols) - done))

    log.info("完成：成功 %d 只，新增 %d 行，失败 %d 只，耗时 %.0fs",
             len(symbols) - len(failed), total_rows, len(failed), time.time() - t0)
    for s, e in failed[:10]:
        log.warning("失败 %s: %s", s, e)


def _pm_open_ticks(freq: str) -> list[str]:
    """13:00 之后第一根 bar 的时刻（pm_open 的口径）。"""
    return {"5": ["1305", "1300"], "15": ["1315", "1330"], "30": ["1330", "1315"]}.get(
        freq, ["1305", "1300"])


def derive() -> None:
    """派生 pm_open / vwap_all / vwap_top20（聪明钱 + APM 的输入）。

    按 minute_{freq}m 目录逐个派生，输出 minute_derived_{freq}m.parquet；
    5 分钟那一路同时写旧名 minute_derived.parquet，兼容既有读取方。
    """
    root = data_dir() / "1_kline_data"
    dirs = [d for d in sorted(root.glob("minute_*m")) if d.is_dir()]
    if not dirs:
        raise SystemExit("没有分钟线数据，先跑同步")

    wrote = 0
    for d in dirs:
        freq = d.name[len("minute_"):-1]  # "5" / "15" / "30"
        src = sorted(glob.glob(str(d / "*.parquet")))
        if not src:
            continue
        out = []
        for f in src:
            df = pd.read_parquet(f)
            if df.empty:
                continue
            df["t"] = df["time"].astype(str).str[8:12]
            g = df.groupby("date", sort=True)
            rec = pd.DataFrame({"amount": g["amount"].sum(), "volume": g["volume"].sum(),
                                "close": g["close"].last()})
            rec["vwap_all"] = rec["amount"] / rec["volume"].replace(0, pd.NA)
            pm = df[df["t"].isin(_pm_open_ticks(freq))].groupby("date")["open"].first()
            rec["pm_open"] = pm

            def _top20(x: pd.DataFrame) -> float:
                x = x.sort_values("t")
                cum = x["volume"].cumsum() / max(float(x["volume"].sum()), 1e-9)
                sel = x[cum <= 0.2]
                if sel.empty:
                    sel = x.iloc[:1]
                v = float(sel["volume"].sum())
                return float(sel["amount"].sum()) / v if v > 0 else float("nan")

            rec["vwap_top20"] = g.apply(_top20, include_groups=False)
            rec["symbol"] = df["symbol"].iloc[0]
            rec["freq"] = f"{freq}m"
            out.append(rec.reset_index())
        if not out:
            continue
        panel = pd.concat(out, ignore_index=True)
        dst = data_dir() / "5_technical_derived" / f"minute_derived_{freq}m.parquet"
        dst.parent.mkdir(parents=True, exist_ok=True)
        panel.to_parquet(dst, index=False)
        if freq == "5":
            panel.to_parquet(dst.parent / "minute_derived.parquet", index=False)
        log.info("派生完成（%sm）：%d 行 → %s", freq, len(panel), dst)
        wrote += 1
    if not wrote:
        raise SystemExit("分钟线目录里没有可用数据")


def main() -> None:
    ap = argparse.ArgumentParser(description="5 分钟线同步（东财增量 / baostock 回补）")
    ap.add_argument("--mode", choices=["backfill", "daily"], default="daily")
    ap.add_argument("--source", choices=["em", "baostock", "tencent"], default="em")
    ap.add_argument("--freq", default="15", choices=["5", "15", "30"], help="tencent 源粒度")
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--symbols", default=None, help="逗号分隔，后缀式如 600519.SH")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--workers", type=int, default=3, help="东财建议 2~4，baostock 建议 8")
    ap.add_argument("--derive-only", action="store_true", help="只做派生，不抓数据")
    ap.add_argument("--derive", action="store_true", help="兼容旧用法：等同 --derive-only")
    args = ap.parse_args()

    if args.derive_only or args.derive:
        derive()
        return

    today = date.today()
    if args.mode == "backfill":
        start = args.start or "2020-01-01"
        end = args.end or today.isoformat()
    else:
        start = args.start or (today - timedelta(days=45)).isoformat()
        end = args.end or today.isoformat()

    syms = ([s.strip() for s in args.symbols.split(",") if s.strip()]
            if args.symbols else universe(args.limit))
    run_sync(syms, start, end, args.workers, args.source, args.freq)
    derive()


if __name__ == "__main__":
    main()
