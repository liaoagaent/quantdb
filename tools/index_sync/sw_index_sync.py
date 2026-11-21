#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""申万行业指数日线同步（Tushare sw_daily → QuantDB 湖 index_industry 独立目录）。

行业指数**不与宽基指数混存**：宽基在 `1_kline_data/index_daily/`，
申万行业在本目录 `1_kline_data/index_industry/`，避免影响按 index_daily
全量扫描的消费端（quantdb_hub / qlib 构建 / 基准识别）。

数据源：Tushare Pro（token 在 .env 的 TUSHARE_TOKEN，一年权限）。
  - index_classify(src=SW2021, level=L1/L2) → 行业清单（801xxx.SI）
  - sw_daily(ts_code, start_date, end_date) → 日线

单位（2026-11-21 实测校准，勿照抄 index_daily 的手/千元约定）：
sw_daily 的 vol 单位=万股、amount 单位=万元。证据：R=amount/vol 对 31 个申万一级行业
全部落在成交量加权平均价合理带（银行 9.0、家电 12.5、电力设备 15.9、医药 14.5、食品饮料 20.5 元/股）；
且 31 行业 amount 求和=1.65 万亿元/日，与 CSI300 成交额（4400 亿）量级自洽。
换算：volume×1e4→股、amount 保持万元。写库前做量级 sanity 检查，异常即拒绝写入。

用法（项目根目录）：
    python tools/index_sync/sw_index_sync.py                  # 增量：本地最新分区+1 → 今天
    python tools/index_sync/sw_index_sync.py --full-history   # 从 2000 年全量回补
    python tools/index_sync/sw_index_sync.py --verify-only    # 只看覆盖，不写库
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_DATA_DIR = Path(os.getenv("QM_QUANTDB_DATA_DIR", str(PROJECT_ROOT / "data" / "quantdb")))
REL_DIR = Path("1_kline_data") / "index_industry"
TUSHARE_URL = "http://api.tushare.pro"

COLUMNS = ["symbol", "time", "open", "high", "low", "close", "volume", "amount", "Category"]
VOL_SCALE = 1e4   # 实测：sw_daily vol=万股 -> 股（注意与 index_daily 的手不同）
AMT_SCALE = 1.0   # 实测：sw_daily amount=万元，直接入湖（与湖内 amount=万元口径一致）
FULL_HISTORY_START = "20000101"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("sw_index_sync")


def load_token() -> str:
    try:
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        env_file = PROJECT_ROOT / ".env"
        if env_file.exists():
            for line in env_file.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if line.startswith("TUSHARE_TOKEN="):
                    os.environ.setdefault("TUSHARE_TOKEN", line.split("=", 1)[1].strip())
    token = os.getenv("TUSHARE_TOKEN", "")
    if not token:
        log.error("TUSHARE_TOKEN 未设置（.env）")
        sys.exit(1)
    return token


def tushare_call(token: str, api: str, params: dict, fields: str = "", max_retry: int = 4) -> list:
    """Tushare Pro 调用；限频/网络错误退避重试，业务错误直接抛出。"""
    last_err: Exception | None = None
    for attempt in range(max_retry):
        try:
            r = requests.post(TUSHARE_URL, json={
                "api_name": api, "token": token, "params": params, "fields": fields,
            }, timeout=30)
            r.raise_for_status()
            j = r.json()
            if j.get("code") != 0:
                msg = str(j.get("msg", ""))
                if ("每分钟" in msg or "频" in msg or "权限" in msg) and attempt < max_retry - 1:
                    log.warning("%s 限频/权限提示（%s），%ds 后重试", api, msg[:80], 15 * (attempt + 1))
                    time.sleep(15 * (attempt + 1))
                    last_err = RuntimeError(msg)
                    continue
                raise RuntimeError(f"{api} 业务错误: {msg}")
            return (j.get("data") or {}).get("items") or []
        except Exception as e:  # 本机代理不稳（ProxyError）等网络错误，退避重试
            last_err = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"{api} 重试 {max_retry} 次仍失败: {last_err}")


def get_universe(token: str, levels: list[str]) -> dict[str, str]:
    """申万行业清单：{801xxx.SI: 行业名}。"""
    uni: dict[str, str] = {}
    for lv in levels:
        items = tushare_call(token, "index_classify", {"src": "SW2021", "level": lv})
        for it in items:
            # index_classify 返回: index_code, industry_name, ...
            uni[it[0]] = f"申万{lv}"
        log.info("申万%s 行业 %d 个", lv, len(items))
    return uni


def fetch_history(token: str, ts_code: str, start: str, end: str, sleep_sec: float) -> pd.DataFrame:
    """sw_daily 分段拉取（单次调用行数有限，按 8 年窗口切）。返回原始行 DataFrame。"""
    frames = []
    cur = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")
    while cur <= end_dt:
        seg_end = min(datetime(cur.year + 7, 12, 31), end_dt)
        items = tushare_call(token, "sw_daily", {
            "ts_code": ts_code,
            "start_date": cur.strftime("%Y%m%d"),
            "end_date": seg_end.strftime("%Y%m%d"),
        }, fields="trade_date,open,high,low,close,vol,amount")
        for it in items:
            frames.append({
                "date": str(it[0]).replace("-", ""),
                "open": float(it[1]), "high": float(it[2]), "low": float(it[3]),
                "close": float(it[4]), "volume": float(it[5]), "amount": float(it[6]),
            })
        cur = datetime(seg_end.year + 1, 1, 1)
        time.sleep(sleep_sec)
    return pd.DataFrame(frames, columns=["date", "open", "high", "low", "close", "volume", "amount"])


def sanity_check(df: pd.DataFrame, label: str) -> None:
    """换算后的量级检查：行业日成交额应在 1e2~1e9 万元；防单位搞错入湖。"""
    amt = df["amount"].replace(0, pd.NA).dropna()
    if amt.empty:
        return
    med = float(amt.median())
    if not (1e2 <= med <= 1e9):
        raise RuntimeError(
            f"{label} 换算后 amount 中位 {med:.3g} 万元，量级异常（预期 1e2~1e9），"
            f"请核对 sw_daily 单位假设（vol=万股、amount=万元）是否成立")


def write_partitions(data_dir: Path, by_date: dict[str, list[dict]], overwrite: bool) -> tuple[int, int]:
    written = skipped = 0
    for ds in sorted(by_date):
        part = data_dir / REL_DIR / f"dt={ds}"
        final = part / "data.parquet"
        if final.exists() and not overwrite:
            skipped += 1
            continue
        part.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(by_date[ds], columns=COLUMNS)
        df["time"] = df["time"].astype("datetime64[ns]")
        tmp = part / "data.parquet.part"
        df.sort_values("symbol").reset_index(drop=True).to_parquet(tmp, index=False)
        os.replace(tmp, final)
        written += 1
    return written, skipped


def verify(data_dir: Path, since: str, until: str) -> None:
    base = data_dir / REL_DIR
    cur = datetime.strptime(since, "%Y%m%d")
    end = datetime.strptime(until, "%Y%m%d")
    have, miss = [], []
    while cur <= end:
        ds = cur.strftime("%Y%m%d")
        f = base / f"dt={ds}" / "data.parquet"
        (have.append((ds, len(pd.read_parquet(f)))) if f.exists() else miss.append(ds))
        cur += timedelta(days=1)
    log.info("index_industry 覆盖 %s~%s：%d 天有数据，缺 %s", since, until, len(have), miss or "无")
    for ds, n in have[-5:]:
        log.info("  dt=%s rows=%d", ds, n)


def main() -> None:
    ap = argparse.ArgumentParser(description="申万行业指数日线同步（Tushare）")
    ap.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    ap.add_argument("--since", default=None, help="起始日 YYYYMMDD（默认本地最新分区+1；无分区则全历史）")
    ap.add_argument("--until", default=None, help="结束日 YYYYMMDD（默认今天）")
    ap.add_argument("--full-history", action="store_true", help="强制从 2000 年全量回补")
    ap.add_argument("--levels", default="L1", help="申万层级，逗号分隔：L1 或 L1,L2")
    ap.add_argument("--sleep", type=float, default=0.25, help="Tushare 请求间隔秒")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--verify-only", action="store_true")
    args = ap.parse_args()

    token = load_token()
    data_dir = Path(args.data_dir)
    universe = get_universe(token, [x.strip().upper() for x in args.levels.split(",") if x.strip()])
    if not universe:
        log.error("未取到申万行业清单")
        sys.exit(1)

    base = data_dir / REL_DIR
    parts = sorted((p for p in base.glob("dt=*") if p.is_dir()), key=lambda p: p.name)
    until = args.until or datetime.now().strftime("%Y%m%d")
    if args.verify_only:
        since = args.since or (parts[-1].name.split("=", 1)[1] if parts else until)
        verify(data_dir, since, until)
        return

    if args.since:
        since = args.since
    elif parts:
        since = (datetime.strptime(parts[-1].name.split("=", 1)[1], "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
    else:
        since = FULL_HISTORY_START
    if args.full_history:
        since = FULL_HISTORY_START

    log.info("拉取区间 %s ~ %s，行业 %d 个", since, until, len(universe))
    by_date: dict[str, list[dict]] = {}
    empty = []
    for i, (ts_code, cat) in enumerate(sorted(universe.items()), 1):
        df = fetch_history(token, ts_code, since, until, args.sleep)
        sym = ts_code.replace(".SI", ".SW")  # 后缀式规范，SI=Tushare 申万原后缀
        if df.empty:
            empty.append(sym)
            continue
        seg = df[(df["date"] >= since) & (df["date"] <= until)]
        sanity_check(seg, sym)
        for _, r in seg.iterrows():
            by_date.setdefault(r["date"], []).append({
                "symbol": sym, "time": pd.to_datetime(r["date"]),
                "open": r["open"], "high": r["high"], "low": r["low"], "close": r["close"],
                "volume": r["volume"] * VOL_SCALE, "amount": r["amount"] * AMT_SCALE,
                "Category": cat,
            })
        log.info("[%02d/%02d] %s(%s)：%d 根", i, len(universe), sym, cat, len(seg))
        time.sleep(args.sleep)
    if empty:
        log.warning("无数据行业：%s", ", ".join(empty))

    if not by_date:
        log.info("区间 %s~%s 无待写数据（本地已是最新）", since, until)
        return
    written, skipped = write_partitions(data_dir, by_date, args.overwrite)
    log.info("写入完成：新增 %d 天，跳过已存在 %d 天", written, skipped)
    verify(data_dir, since, until)


if __name__ == "__main__":
    main()
