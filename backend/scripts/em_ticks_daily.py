#!/usr/bin/env python3
"""东财逐笔 → 每日成交笔数（比腾讯分笔更全：含集合竞价 + 盘后固定价格）。

腾讯分笔缺 09:25 集合竞价（实测茅台少 3.4%），东财 `details` 从 09:15:02 开始，
一直给到 15:27~15:30，且一次请求返回当日全部明细（不用翻 62 页）。

实测（2026-11-13）：
    600519.SH  4,483 条   300750.SZ  4,841 条
    000001.SZ  4,744 条   002520.SZ  4,417 条

已知口径问题（待核）：Σ(手数)×100 比日线 volume 多约 11.2%
    （茅台 4,647,800 股 vs 日线 4,177,984 股），疑似 details 含竞价阶段委托
    或盘后固定价格交易。落库时同时写 `tick_volume` / `tick_amount` 两个原始合计，
    便于日后与日线对账；`num_trades`（条数）不受影响。

接口：https://push2.eastmoney.com/api/qt/stock/details/get
    secid = 1.600519(沪) / 0.000001(深) ；fields2 = f51..f55 → 时间,价,手数,?,?
    注意：push2 有频率限制，连续快打会 RemoteDisconnected，需冷却 + 限速。

落盘：data/quantdb/5_technical_derived/trade_count_daily.parquet
      与 tencent_ticks_daily.py / l2_ticks_daily.py 同 schema，可混用。

用法:
    python backend/scripts/em_ticks_daily.py --all --workers 6
    python backend/scripts/em_ticks_daily.py --symbols 600519.SH,000001.SZ
    python backend/scripts/em_ticks_daily.py --all --source auto   # 东财失败自动退腾讯
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("em_ticks_daily")

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/",
    "Accept": "*/*",
}
URL = ("https://push2.eastmoney.com/api/qt/stock/details/get"
       "?secid={secid}&fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55&pos=-5000")

MIN_INTERVAL = 0.35          # 全局请求间隔（秒）≈2.8 req/s。东财 push2 很敏感：
                             # 连打十几个请求（间隔 <0.1s）就会被 RemoteDisconnected 断开，
                             # 且冷却期长达数分钟。宁可慢，不要封。
BLOCK_COOLDOWN = (60, 120, 300)   # 触发风控后的全局冷却阶梯（秒）
MAX_CONSEC_FAIL = 12              # 连续失败这么多次就整体熔断退出
_gate = threading.Lock()
_last = [0.0]
_fail_streak = [0]
_cooldown_until = [0.0]


def _throttle() -> None:
    with _gate:
        now = time.monotonic()
        if _cooldown_until[0] > now:
            time.sleep(_cooldown_until[0] - now)
            now = time.monotonic()
        wait = MIN_INTERVAL - (now - _last[0])
        if wait > 0:
            time.sleep(wait)
        _last[0] = time.monotonic()


def _on_success() -> None:
    with _gate:
        _fail_streak[0] = 0


def _on_block(err: Exception) -> None:
    with _gate:
        _fail_streak[0] += 1
        n = _fail_streak[0]
        if n >= MAX_CONSEC_FAIL:
            raise RuntimeError(f"连续失败 {n} 次，东财已风控，整体中止（稍后重跑可续传）") from err
        idx = min(n // 4, len(BLOCK_COOLDOWN) - 1)
        cool = BLOCK_COOLDOWN[idx]
        _cooldown_until[0] = max(_cooldown_until[0], time.monotonic() + cool)
        log.warning("东财风控/断连（连续 %d 次），全局冷却 %ds", n, cool)


def data_dir() -> Path:
    env = os.getenv("QM_QUANTDB_DATA_DIR", "").strip()
    return Path(env) if env else PROJECT_ROOT / "data" / "quantdb"


def to_secid(symbol: str) -> str:
    code, _, suffix = symbol.partition(".")
    market = "1" if suffix.upper() == "SH" else "0"
    return f"{market}.{code}"


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


def _get_json(url: str, tries: int = 4) -> dict:
    last_err: Exception | None = None
    for i in range(tries):
        _throttle()
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=25) as r:
                data = json.loads(r.read().decode("utf-8", "replace"))
            _on_success()
            return data
        except Exception as e:  # noqa: BLE001
            last_err = e
            if isinstance(e, (urllib.error.URLError, ConnectionError, TimeoutError)) or "Disconnected" in type(e).__name__:
                _on_block(e)          # 断连/超时 → 全局降速
            else:
                time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"请求失败 {tries} 次: {type(last_err).__name__}") from last_err


def fetch_em(symbol: str) -> dict | None:
    j = _get_json(URL.format(secid=to_secid(symbol)))
    data = j.get("data") or {}
    details = data.get("details") or []
    if not details:
        return None
    n = len(details)
    vol_lot = 0.0      # 手
    amt = 0.0          # 元
    for x in details:
        f = x.split(",")
        if len(f) < 3:
            continue
        try:
            price = float(f[1])
            lots = float(f[2])
        except ValueError:
            continue
        vol_lot += lots
        amt += price * lots * 100.0
    return {
        "symbol": symbol,
        "num_trades": n,
        "volume": vol_lot * 100.0,
        "amount": amt,
        "avg_trade_amt": amt / n if n else float("nan"),
    }


def fetch_tencent(symbol: str, max_page: int = 300) -> dict | None:
    """兜底源：腾讯分笔（缺集合竞价）。"""
    import re

    code = to_tencent(symbol)
    n, vol, amt = 0, 0.0, 0.0
    for p in range(1, max_page + 1):
        _throttle()
        req = urllib.request.Request(
            f"https://stock.gtimg.cn/data/index.php?appn=detail&action=data&c={code}&p={p}",
            headers={"User-Agent": UA["User-Agent"], "Referer": "https://gu.qq.com/"})
        with urllib.request.urlopen(req, timeout=20) as r:
            text = r.read().decode("gbk", "replace")
        m = re.search(r"=\[(.*)\];?", text, re.S)
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
                    vol += float(f[4]) * 100.0
                    amt += float(f[5])
                except ValueError:
                    pass
        if len(recs) < 60:
            break
    if n == 0:
        return None
    return {"symbol": symbol, "num_trades": n, "volume": vol, "amount": amt,
            "avg_trade_amt": amt / n if n else float("nan")}


def last_trade_date(symbol: str) -> str:
    code = to_tencent(symbol)
    req = urllib.request.Request(f"https://qt.gtimg.cn/q={code}",
                                 headers={"User-Agent": UA["User-Agent"]})
    with urllib.request.urlopen(req, timeout=15) as r:
        text = r.read().decode("gbk", "replace")
    f = text.split("~")
    if len(f) > 30:
        stamp = f[30].strip()
        if len(stamp) >= 8 and stamp[:8].isdigit():
            return f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}"
    return date.today().isoformat()


def main() -> None:
    ap = argparse.ArgumentParser(description="东财逐笔 → 每日成交笔数")
    ap.add_argument("--all", action="store_true", help="全市场")
    ap.add_argument("--symbols", default=None)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--date", default=None, help="落库日期，缺省从行情快照探测")
    ap.add_argument("--source", choices=["em", "tencent", "auto"], default="em")
    ap.add_argument("--force", action="store_true", help="已存在的标的也重抓")
    args = ap.parse_args()

    syms = ([s.strip() for s in args.symbols.split(",") if s.strip()]
            if args.symbols else universe(args.limit))
    if not syms:
        raise SystemExit("标的为空，用 --all 或 --symbols")

    dst = data_dir() / "5_technical_derived" / "trade_count_daily.parquet"
    d = args.date or last_trade_date(syms[0])
    log.info("落库交易日 = %s（快照探测）", d)

    old = pd.DataFrame()
    have: set[str] = set()
    if dst.exists():
        old = pd.read_parquet(dst)
        if not args.force:
            have = set(old.loc[old["date"].astype(str) == d, "symbol"])
            log.info("已有 %d 条 %s 记录，跳过", len(have), d)

    todo = [s for s in syms if s not in have]
    log.info("待抓 %d 只（%d 并发，间隔 %.2fs，源=%s）",
             len(todo), args.workers, MIN_INTERVAL, args.source)

    t0 = time.time()
    rows: list[dict] = []
    failed = 0

    def work(sym: str) -> dict | None:
        if args.source in ("em", "auto"):
            try:
                rec = fetch_em(sym)
                if rec:
                    rec["src"] = "em"
                    return rec
            except Exception as e:  # noqa: BLE001
                if args.source == "em":
                    raise
                log.debug("%s 东财失败退腾讯: %s", sym, e)
        rec = fetch_tencent(sym)
        if rec:
            rec["src"] = "tencent"
        return rec

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(work, s): s for s in todo}
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
                log.info("进度 %d/%d 成功 %d 失败 %d 用时 %.0fs",
                         i, len(todo), len(rows), failed, time.time() - t0)

    if not rows:
        log.warning("没有取到数据（非交易日？接口变更？）")
        return

    out = pd.DataFrame(rows)
    cols = ["symbol", "date", "num_trades", "avg_trade_amt", "volume", "amount"]
    if "src" in out.columns:
        cols.append("src")
    out = out[cols]
    if len(old):
        out = (pd.concat([old, out], ignore_index=True)
               .drop_duplicates(subset=["symbol", "date"], keep="last"))
    dst.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(dst, index=False)
    log.info("完成：新增 %d 只，累计 %d 行 → %s（耗时 %.0fs，失败 %d）",
             len(rows), len(out), dst, time.time() - t0, failed)
    log.info("落库时间戳 %s", datetime.now().isoformat(timespec="seconds"))


if __name__ == "__main__":
    main()
