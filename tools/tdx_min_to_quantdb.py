# -*- coding: utf-8 -*-
"""通达信 vipdoc 分钟线 → QuantDB parquet 同步工具（流式版，2026-11-20）。

用法:
    python tdx_min_to_quantdb.py --kind min      # lc1→min1_kline + lc5→min5_kline（增量，推荐）
    python tdx_min_to_quantdb.py --kind lc1      # 只同步 1 分钟
    python tdx_min_to_quantdb.py --kind lc5      # 只同步 5 分钟
    python tdx_min_to_quantdb.py --kind min --date 20261120   # 只处理指定交易日

vipdoc 目录结构（通达信实际布局）:
    vipdoc/<mk>/lday/<mk><code>.day      日线
    vipdoc/<mk>/minline/<mk><code>.lc1   1 分钟线
    vipdoc/<mk>/fzline/<mk><code>.lc5    5 分钟线

行为约定（与 2026-11-18 验收版一致）:
- 输出目录: <QUANTDB>/1_kline_data/min{1,5}_kline/dt=YYYYMMDD/data.parquet
- 每个交易日一个分区；覆盖已存在的同日分区（幂等重跑安全）
- 只处理 FLOOR=20261118 之后的交易日（增量接管；更早历史由 QuantDB 自身管线负责）
- volume 单位=股、amount 单位=万元（与 .tmp_qdb 幸存样本对齐）
- min5 剔除 13:00 时点；停牌股补 vol=0 行需 temp/trading_minutes_*.csv，缺省时跳过补齐
- 写完分区后落 .tdxlocal 标记文件

内存模型（流式）: 逐交易日处理，逐文件 numpy 向量化解析后只保留当日切片，
写完即弃。峰值内存 ≈ 单文件 + 当日全市场切片（数百 MB 级），与历史天数无关。
"""
import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tdx_reader import read_day_last_close, read_min_frame  # noqa: E402

TDX_VIPDOC = Path(r"D:\softwore\tdx\vipdoc")
QUANTDB_DIR = Path(r"D:\gitProject\QuantMind\data\quantdb")
FLOOR = 20261118  # 只接管此日期（含）之后的交易日

MIN5_DROP_MINUTE = 780  # 13:00 → 13*60，min5 剔除该时点
EXT_SUBDIR = {"lc1": "minline", "lc5": "fzline", "day": "lday"}

# A 股股票代码前缀白名单（对齐湖内历史分区口径，见 dt=20261117 前3位全集）。
# 注意 000 段同时命中 sh000xxx 大盘指数（历史分区即含 000001.SH/000300.SH）。
PREFIX_WHITELIST = {
    "000", "001", "002", "003",          # 深主板
    "300", "301", "302",                 # 创业板
    "600", "601", "603", "605",          # 沪主板
    "688", "689",                        # 科创板
    "920",                               # 北交所
}


def iter_tdx_codes(market: str, ext: str):
    """枚举 vipdoc/<market>/<EXT_SUBDIR[ext]>/ 下股票代码文件，返回 [(code6, path)]。

    过滤：文件名 <mk><code6>.<ext> 且 code6 前 3 位在 PREFIX_WHITELIST（剔除
    可转债/ETF/基金/债券/非 000 段指数等非股票品种，对齐历史分区口径）。
    """
    d = TDX_VIPDOC / market / EXT_SUBDIR[ext]
    if not d.is_dir():
        return
    for p in sorted(d.glob(f"*.{ext}")):
        stem = p.stem
        if len(stem) != 8 or stem[:2].lower() != market:
            continue
        code = stem[2:]
        if code.isdigit() and code[:3] in PREFIX_WHITELIST:
            yield code, p


def suffix_for(market: str, code: str) -> str:
    """市场+代码 → 后缀式 symbol（600036.SH / 000001.SZ / 430047.BJ）。"""
    if market == "sh":
        return f"{code}.SH"
    if market == "sz":
        return f"{code}.SZ"
    if market == "bj":
        return f"{code}.BJ"
    raise ValueError(market)


def scan_dates(ext: str, floor: int) -> set:
    """扫描文件内出现过的全部交易日（≥floor）。

    注意：不能只看文件尾日期——中间缺失的交易日（如某天未下载）永远不会
    出现在文件尾，必须全量读日期列取 unique。numpy 解析很快（分钟级→秒级/文件）。
    """
    dates = set()
    for market in ("sh", "sz", "bj"):
        for _code, path in iter_tdx_codes(market, ext):
            df = read_min_frame(path)
            if df.empty:
                continue
            d = df["date"].values
            d = d[d >= floor]
            if len(d):
                dates.update(d.tolist())
    return dates


def build_partition_stream(kind: str, target: int, ext: str) -> pd.DataFrame:
    """流式构建 target 日全市场分钟长表：逐文件解析即取当日切片，不保留全量。"""
    cols = ["symbol", "date", "minute", "open", "high", "low", "close", "volume", "amount"]
    frames = []
    for market in ("sh", "sz", "bj"):
        for code, path in iter_tdx_codes(market, ext):
            df = read_min_frame(path, after_date=target)
            if df.empty:
                continue
            day = df[df["date"] == target]
            if day.empty:
                continue
            if ext == "lc5":
                day = day[day["minute"] != MIN5_DROP_MINUTE]
                if day.empty:
                    continue
            day = day.copy()
            day["symbol"] = suffix_for(market, code)
            frames.append(day[cols])
    if not frames:
        return pd.DataFrame(columns=cols)
    big = pd.concat(frames, ignore_index=True)
    # 停牌股补 vol=0 平价行（需静态分钟时点表；缺表则跳过补齐，宁可少不可错）
    big = fill_suspended(big, kind)
    # 统一 schema 与单位
    big["volume"] = big["volume"].astype("int64")
    big["amount"] = big["amount"] / 10000.0  # 元 → 万元
    big["time"] = (pd.to_datetime(big["date"].astype(str), format="%Y%m%d")
                   + pd.to_timedelta(big["minute"], unit="m"))
    out = big[["symbol", "time", "open", "high", "low", "close", "volume", "amount"]]
    return out.sort_values(["symbol", "time"]).reset_index(drop=True)


def fill_suspended(big: pd.DataFrame, kind: str) -> pd.DataFrame:
    """对当日无分钟数据、但 .day 末收盘有效的停牌股，补 vol=0 平价行（需静态时点表）。"""
    have = set(big["symbol"].unique())
    expect = []
    for market in ("sh", "sz", "bj"):
        for code, path in iter_tdx_codes(market, "day"):
            sym = suffix_for(market, code)
            if sym in have:
                continue
            last = read_day_last_close(path)
            if last is None or last <= 0:
                continue
            expect.append((sym, last))
    if not expect:
        return big
    n_bars = 48 if kind == "min5" else 241
    csv = _trading_minutes_csv(kind)
    if not csv.exists():
        return big  # 无静态时点表：跳过补齐，保留原数据
    times = pd.read_csv(csv, header=None)[0].tolist()
    base_date = int(big["date"].iloc[0]) if len(big) else 0
    rows = []
    for sym, px in expect:
        for i in range(min(n_bars, len(times))):
            hh, mm = str(times[i]).split(":")
            rows.append({
                "symbol": sym,
                "date": base_date,
                "minute": int(hh) * 60 + int(mm),
                "open": px, "high": px, "low": px, "close": px,
                "volume": 0, "amount": 0.0,
            })
    suspend = pd.DataFrame(rows)
    keep_cols = ["symbol", "date", "minute", "open", "high", "low", "close", "volume", "amount"]
    return pd.concat([big[keep_cols], suspend[keep_cols]], ignore_index=True)


def _trading_minutes_csv(kind: str) -> Path:
    return Path(__file__).resolve().parent / "temp" / f"trading_minutes_{kind}.csv"


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--kind", choices=["min", "lc1", "lc5"], default="min",
                    help="min=两种都同步; lc1/lc5=只同步一种")
    ap.add_argument("--date", default=None, help="只处理指定交易日 YYYYMMDD（默认全部 ≥FLOOR）")
    args = ap.parse_args()

    kinds = ["min1", "min5"] if args.kind == "min" else (
        ["min1"] if args.kind == "lc1" else ["min5"])

    for kind in kinds:
        ext = "lc1" if kind == "min1" else "lc5"
        if args.date:
            dates = {int(args.date)}
        else:
            dates = scan_dates(ext, FLOOR)
        print(f"[{kind}] 待同步交易日: {sorted(dates)}", flush=True)
        for target in sorted(dates):
            df = build_partition_stream(kind, target, ext)
            if df.empty:
                print(f"[{kind}] dt={target} 无数据，跳过", flush=True)
                continue
            out_dir = QUANTDB_DIR / "1_kline_data" / f"{kind}_kline" / f"dt={target}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / "data.parquet"
            df.to_parquet(out_file, index=False)
            (out_dir / ".tdxlocal").touch()
            print(f"[{kind}] dt={target} -> {out_file}  rows={len(df)}  "
                  f"symbols={df['symbol'].nunique()}  "
                  f"({datetime.now():%H:%M:%S})", flush=True)


if __name__ == "__main__":
    main()
