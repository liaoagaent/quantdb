#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""指数日线盘后同步（东财 push2his 主力源 → QuantDB 湖 index_daily 分区）。

源优先级（2026-11-21 与操作员确认）：
  P0 东财 push2his —— 单请求返回单指数全历史日线，T 日盘后即可取；限频用 sleep 控制
  P1 Tushare       —— 权威对账源（token 为一年权限，另可用于补申万行业指数）
  P2 通达信 vipdoc —— 待接入（shlday/szlday，与个股盘后 SOP 同链路）

用法（项目根目录）：
    python tools/index_sync/index_sync.py                  # 补 lake 最新分区之后到最近交易日的缺口
    python tools/index_sync/index_sync.py --since 20261107 --until 20261120
    python tools/index_sync/index_sync.py --verify-only    # 只做覆盖/schema 检查，不写库
    python tools/index_sync/index_sync.py --tushare-check  # 写库后用 Tushare 收盘价对账

输出 schema 与存量分区一致：
    symbol(后缀式 str) / time(datetime64[ns]) / open / high / low / close
    volume(股) / amount(万元) / Category(宽基|风格|中证行业|主题)
单位换算以重叠交易日实测校准：EM volume(手)×100→股；EM amount(元)/1e4→万元。
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
REL_DIR = Path("1_kline_data") / "index_daily"

EM_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
# f51..f57 = 日期,开盘,收盘,最高,最低,成交量(手),成交额(元) —— 注意开/收顺序与常规不同
EM_FIELDS1 = "f1,f2,f3,f4,f5,f6"
EM_FIELDS2 = "f51,f52,f53,f54,f55,f56,f57"
COLUMNS = ["symbol", "time", "open", "high", "low", "close", "volume", "amount", "Category"]

# 湖内尚无、需要新纳入的指数（Category 沿用存量四分类）
EXTRA_INDICES = {"000985.SH": "宽基"}  # 中证全指

VOL_SCALE = 100.0  # EM 手 -> 股
AMT_SCALE = 1e-4   # EM 元 -> 万元

TUSHARE_URL = "http://api.tushare.pro"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("index_sync")


# ---------------------------------------------------------------- 湖侧

def latest_partition(data_dir: Path) -> tuple[Path, str] | None:
    """返回 (最新分区目录, YYYYMMDD)；无分区返回 None。"""
    base = data_dir / REL_DIR
    parts = sorted((p for p in base.glob("dt=*") if p.is_dir()), key=lambda p: p.name)
    if not parts:
        return None
    p = parts[-1]
    return p, p.name.split("=", 1)[1]


def partition_before(data_dir: Path, date_str: str) -> tuple[Path, str] | None:
    """返回日期早于 date_str 的最新分区，作为指数清单与单位校准的基线。

    不能用"最新分区"当基线：回补后最新分区可能本身缺指数（如首次运行只写进 26 只），
    以它为清单会把缺口固化。基线必须取缺口开始之前的最后一个完整分区。
    """
    base = data_dir / REL_DIR
    parts = sorted(
        (p for p in base.glob("dt=*") if p.is_dir() and p.name.split("=", 1)[1] < date_str),
        key=lambda p: p.name,
    )
    if not parts:
        return None
    p = parts[-1]
    return p, p.name.split("=", 1)[1]


def load_universe(latest_file: Path) -> dict[str, str]:
    """指数清单 = 湖内最新分区已有 symbol(+Category) + EXTRA_INDICES。"""
    df = pd.read_parquet(latest_file, columns=["symbol", "Category"])
    uni = dict(zip(df["symbol"].astype(str), df["Category"].astype(str)))
    for code, cat in EXTRA_INDICES.items():
        uni.setdefault(code, cat)
    return uni


def write_partition(data_dir: Path, date_str: str, df: pd.DataFrame, overwrite: bool) -> bool:
    part = data_dir / REL_DIR / f"dt={date_str}"
    part.mkdir(parents=True, exist_ok=True)
    final = part / "data.parquet"
    if final.exists() and not overwrite:
        return False
    tmp = part / "data.parquet.part"
    df.sort_values("symbol").reset_index(drop=True).to_parquet(tmp, index=False)
    os.replace(tmp, final)
    return True


# ---------------------------------------------------------------- 东财源

def em_secid(symbol: str) -> str:
    """000xxx.SH -> 1.000xxx；399xxx.SZ / 899xxx.BJ -> 0.xxxxx。"""
    code = symbol.split(".", 1)[0]
    market = "0" if code.startswith(("399", "899")) else "1"
    return f"{market}.{code}"


def em_fetch_daily(session: requests.Session, symbol: str, sleep_sec: float) -> pd.DataFrame:
    """单指数全历史日线。返回列：date(str) open high low close volume amount。"""
    params = {
        "secid": em_secid(symbol),
        "klt": "101",       # 日线
        "fqt": "0",         # 指数无除权，不复权
        "beg": "0",
        "end": "20500101",
        "fields1": EM_FIELDS1,
        "fields2": EM_FIELDS2,
    }
    last_err: Exception | None = None
    for attempt in range(4):
        try:
            r = session.get(EM_KLINE_URL, params=params, timeout=15)
            r.raise_for_status()
            data = (r.json() or {}).get("data") or {}
            klines = data.get("klines") or []
            if not klines:
                # EM 限频/风控时返回 HTTP 200 + data:null，与"真没数据"无法区分；
                # 指数全历史请求正常必有数据，故空结果一律按可重试失败处理
                raise RuntimeError(f"empty klines (secid={params['secid']}, attempt={attempt + 1})")
            rows = []
            for line in klines:
                f = line.split(",")
                if len(f) < 7:
                    continue
                rows.append({
                    "date": f[0].replace("-", ""),
                    "open": float(f[1]), "close": float(f[2]),
                    "high": float(f[3]), "low": float(f[4]),
                    "volume": float(f[5]), "amount": float(f[6]),
                })
            return pd.DataFrame(rows)
        except Exception as e:  # 网络抖动重试
            last_err = e
            time.sleep(sleep_sec * (attempt + 1) * 2)
    log.warning("EM 拉取失败 %s: %s", symbol, last_err)
    return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
    # NOTE: 返回空 DataFrame 时调用方按"该指数本段无数据"处理并计数告警


def calibrate_scales(lake_file: Path, em_frames: dict[str, pd.DataFrame]) -> None:
    """用湖内最新分区与 EM 同日数据校准 volume/amount 换算，失败即中断（防脏数据入湖）。"""
    lake = pd.read_parquet(lake_file)
    lake["symbol"] = lake["symbol"].astype(str)
    vol_ratios, amt_ratios = [], []
    for sym, edf in em_frames.items():
        if edf.empty:
            continue
        lrow = lake[lake["symbol"] == sym]
        if lrow.empty:
            continue
        ldate = pd.to_datetime(lrow["time"].iloc[0]).strftime("%Y%m%d")
        erow = edf[edf["date"] == ldate]
        if erow.empty or erow["volume"].iloc[0] <= 0 or erow["amount"].iloc[0] <= 0:
            continue
        vol_ratios.append(float(lrow["volume"].iloc[0]) / float(erow["volume"].iloc[0]))
        amt_ratios.append(float(lrow["amount"].iloc[0]) / float(erow["amount"].iloc[0]))
    if len(vol_ratios) < 5:
        raise RuntimeError(f"校准样本不足（{len(vol_ratios)} 个），拒绝写库")
    med_v, med_a = sorted(vol_ratios)[len(vol_ratios) // 2], sorted(amt_ratios)[len(amt_ratios) // 2]
    log.info("校准：volume 中位比=%.4f（期望 100），amount 中位比=%.8f（期望 1e-4），样本=%d",
             med_v, med_a, len(vol_ratios))
    if not (50 <= med_v <= 200 and 5e-5 <= med_a <= 5e-4):
        raise RuntimeError(f"单位换算校准失败：vol={med_v}, amt={med_a}，请人工核对后重跑")


# ---------------------------------------------------------------- Tushare 对账

def tushare_check(symbols: list[str], since: str, until: str, data_dir: Path) -> None:
    token = os.getenv("TUSHARE_TOKEN", "")
    if not token:
        log.warning("TUSHARE_TOKEN 未设置，跳过对账")
        return
    for sym in symbols:
        try:
            r = requests.post(TUSHARE_URL, json={
                "api_name": "index_daily", "token": token,
                "params": {"ts_code": sym, "start_date": since, "end_date": until},
                "fields": "trade_date,close",
            }, timeout=15)
            items = ((r.json() or {}).get("data") or {}).get("items") or []
            ts_close = {it[0].replace("-", ""): float(it[1]) for it in items}
        except Exception as e:
            log.warning("Tushare 拉取失败 %s: %s", sym, e)
            continue
        diffs = []
        for d, tsv in sorted(ts_close.items()):
            part = data_dir / REL_DIR / f"dt={d}" / "data.parquet"
            if not part.exists():
                continue
            lk = pd.read_parquet(part, columns=["symbol", "close"])
            row = lk[lk["symbol"].astype(str) == sym]
            if row.empty:
                continue
            diffs.append(abs(float(row["close"].iloc[0]) - tsv))
        if diffs:
            log.info("Tushare 对账 %s：n=%d，最大收盘价偏差=%.4f", sym, len(diffs), max(diffs))
        else:
            log.warning("Tushare 对账 %s：无可比日期", sym)
        time.sleep(0.3)


# ---------------------------------------------------------------- 校验

def verify(data_dir: Path, since: str, until: str, baseline_date: str) -> None:
    base = data_dir / REL_DIR
    d0 = datetime.strptime(since, "%Y%m%d")
    d1 = datetime.strptime(until, "%Y%m%d")
    baseline_schema = None
    bp = base / f"dt={baseline_date}" / "data.parquet"
    if bp.exists():
        baseline_schema = pd.read_parquet(bp).dtypes
    cur = d0
    missing, counts = [], []
    while cur <= d1:
        ds = cur.strftime("%Y%m%d")
        f = base / f"dt={ds}" / "data.parquet"
        if f.exists():
            df = pd.read_parquet(f)
            counts.append((ds, len(df), sorted(df["symbol"].astype(str).unique())[:3]))
            if baseline_schema is not None and list(df.dtypes.astype(str)) != list(baseline_schema.astype(str)):
                log.warning("schema 与基线不一致 dt=%s：new=%s base=%s", ds,
                            list(df.dtypes.astype(str)), list(baseline_schema.astype(str)))
        else:
            missing.append(ds)
        cur += timedelta(days=1)
    log.info("覆盖检查 %s~%s：有数据 %d 天，缺 %s", since, until, len(counts), missing or "无")
    for ds, n, head in counts:
        log.info("  dt=%s rows=%d symbols_head=%s", ds, n, head)


# ---------------------------------------------------------------- 主流程

def main() -> None:
    # 加载项目根 .env（TUSHARE_TOKEN 等），不覆盖已有环境变量
    try:
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        env_file = PROJECT_ROOT / ".env"
        if env_file.exists():
            for line in env_file.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())

    ap = argparse.ArgumentParser(description="指数日线盘后同步（东财主力源）")
    ap.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR), help="QuantDB 数据根目录")
    ap.add_argument("--since", default=None, help="起始日 YYYYMMDD（默认 lake 最新分区+1）")
    ap.add_argument("--until", default=None, help="结束日 YYYYMMDD（默认今天）")
    ap.add_argument("--sleep", type=float, default=0.4, help="东财请求间隔秒")
    ap.add_argument("--overwrite", action="store_true", help="重写已存在分区")
    ap.add_argument("--verify-only", action="store_true", help="只检查覆盖与 schema")
    ap.add_argument("--tushare-check", action="store_true", help="写库后用 Tushare 对账收盘价")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    lp = latest_partition(data_dir)
    if not lp:
        log.error("未找到任何 index_daily 分区：%s", data_dir / REL_DIR)
        sys.exit(1)
    lake_file, lake_date = lp

    since = args.since or (datetime.strptime(lake_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
    until = args.until or datetime.now().strftime("%Y%m%d")

    # 指数清单/单位校准基线 = 缺口开始之前的最后一个分区（而非最新分区，防缺口固化）
    bl = partition_before(data_dir, since) or lp
    baseline_file, baseline_date = bl
    universe = load_universe(baseline_file)
    log.info("指数清单：%d 只（基线分区 dt=%s，新增 %d）", len(universe),
             baseline_date, len(EXTRA_INDICES))

    if args.verify_only:
        verify(data_dir, since, until, baseline_date)
        return

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

    # 1) 全量拉取（每指数一次请求），再做区间过滤
    em_frames: dict[str, pd.DataFrame] = {}
    pending = sorted(universe)
    for i, sym in enumerate(pending, 1):
        em_frames[sym] = em_fetch_daily(session, sym, args.sleep)
        log.info("[%02d/%02d] %s：历史 %d 根", i, len(pending), sym, len(em_frames[sym]))
        time.sleep(args.sleep)

    # 本机代理对连续 HTTPS 请求不稳定（ProxyError / RemoteDisconnected），
    # 对失败指数做多轮补拉，直到全部成功或连续 3 轮无进展
    for pass_no in range(1, 4):
        failed = [s for s in pending if em_frames[s].empty]
        if not failed:
            break
        log.warning("第 %d 轮补拉 %d 个失败指数：%s", pass_no, len(failed), ", ".join(failed))
        time.sleep(5)
        for sym in failed:
            em_frames[sym] = em_fetch_daily(session, sym, args.sleep)
            time.sleep(args.sleep)

    empty_syms = [s for s in pending if em_frames[s].empty]
    if empty_syms:
        log.warning("以下指数东财无数据（将缺席新分区）：%s", ", ".join(empty_syms))

    # 2) 单位校准（重叠日实测），失败直接中断
    calibrate_scales(baseline_file, em_frames)

    # 3) 组装逐日分区
    by_date: dict[str, list[dict]] = {}
    for sym, edf in em_frames.items():
        if edf.empty:
            continue
        seg = edf[(edf["date"] >= since) & (edf["date"] <= until)]
        for _, r in seg.iterrows():
            by_date.setdefault(r["date"], []).append({
                "symbol": sym,
                "time": pd.to_datetime(r["date"]),
                "open": r["open"], "high": r["high"], "low": r["low"], "close": r["close"],
                "volume": r["volume"] * VOL_SCALE,
                "amount": r["amount"] * AMT_SCALE,
                "Category": universe[sym],
            })

    if not by_date:
        log.info("区间 %s~%s 无待写数据（湖已是最新）", since, until)
        return

    written, skipped = [], []
    for ds in sorted(by_date):
        df = pd.DataFrame(by_date[ds], columns=COLUMNS)
        # 与存量分区保持 datetime64[ns] 精度（pandas 3 默认 us，会导致 schema 漂移）
        df["time"] = df["time"].astype("datetime64[ns]")
        if write_partition(data_dir, ds, df, args.overwrite):
            written.append(ds)
        else:
            skipped.append(ds)
    log.info("写入完成：新增 %d 天（%s ~ %s），跳过已存在 %d 天",
             len(written), written[0] if written else "-", written[-1] if written else "-", len(skipped))
    if skipped:
        log.info("跳过：%s", ", ".join(skipped))

    # 4) 验收 + 可选对账
    verify(data_dir, since, until, baseline_date)
    if args.tushare_check:
        check_syms = [s for s in ("000300.SH", "000905.SH", "399001.SZ") if s in universe]
        tushare_check(check_syms, since, until, data_dir)


if __name__ == "__main__":
    main()
