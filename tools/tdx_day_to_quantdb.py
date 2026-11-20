# -*- coding: utf-8 -*-
"""通达信 vipdoc 日线 → QuantDB daily_unadjusted parquet 同步工具（流式，2026-11-20）。

用法:
    python tdx_day_to_quantdb.py                # 增量：同步所有 ≥FLOOR 的交易日
    python tdx_day_to_quantdb.py --date 20261120  # 只处理指定交易日

输入: D:\softwore\tdx\vipdoc\{sh,sz,bj}\lday\<mk><code>.day（32B/条，未复权）
输出: <QUANTDB>\1_kline_data\daily_unadjusted\dt=YYYYMMDD\data.parquet + .tdxlocal

口径（与现有湖分区对齐，见 temp/schema_dump.txt）:
- 列: symbol, time(datetime64 00:00), open, high, low, close, volume, amount
- symbol 后缀式（000001.SZ）；volume=股(float64)；amount=万元（.day 元 ÷1e4）
- 日线分区不补停牌股（与现有 dt=20261117 分区 5580 行口径一致）
- 覆盖式写同日分区，幂等可重跑；FLOOR=20261118 起接管

内存模型: 逐交易日处理，逐文件 numpy 解析只留当日切片，峰值数百 MB。
"""
import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tdx_reader import read_day_frame  # noqa: E402

TDX_VIPDOC = Path(r"D:\softwore\tdx\vipdoc")
QUANTDB_DIR = Path(r"D:\gitProject\QuantMind\data\quantdb")
FLOOR = 20261118  # 只接管此日期（含）之后的交易日

# A 股股票代码前缀白名单（对齐湖内历史分区口径；000 段含 sh000xxx 大盘指数）
PREFIX_WHITELIST = {
    "000", "001", "002", "003",          # 深主板
    "300", "301", "302",                 # 创业板
    "600", "601", "603", "605",          # 沪主板
    "688", "689",                        # 科创板
    "920",                               # 北交所
}


def iter_tdx_codes(market: str):
    """枚举 vipdoc/<market>/lday/ 下股票代码 .day 文件，返回 [(code6, path)]。

    过滤：文件名 <mk><code6>.day 且 code6 前 3 位在 PREFIX_WHITELIST。
    """
    d = TDX_VIPDOC / market / "lday"
    if not d.is_dir():
        return
    for p in sorted(d.glob("*.day")):
        stem = p.stem
        if len(stem) != 8 or stem[:2].lower() != market:
            continue
        code = stem[2:]
        if code.isdigit() and code[:3] in PREFIX_WHITELIST:
            yield code, p


def suffix_for(market: str, code: str) -> str:
    if market == "sh":
        return f"{code}.SH"
    if market == "sz":
        return f"{code}.SZ"
    if market == "bj":
        return f"{code}.BJ"
    raise ValueError(market)


def scan_day_dates(floor: int) -> set:
    """扫描文件内出现过的全部交易日（≥floor）。

    不能只看文件尾日期——中间缺失的交易日永远不会出现在文件尾，
    必须全量读日期列取 unique（日线文件小，numpy 解析很快）。
    """
    dates = set()
    for market in ("sh", "sz", "bj"):
        for _code, path in iter_tdx_codes(market):
            df = read_day_frame(path)
            if df.empty:
                continue
            d = df["date"].values
            d = d[d >= floor]
            if len(d):
                dates.update(d.tolist())
    return dates


def build_day_partition(target: int) -> pd.DataFrame:
    """流式构建 target 日全市场日线切片：逐文件解析只留当日，不保留全量。"""
    cols = ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
    frames = []
    for market in ("sh", "sz", "bj"):
        for code, path in iter_tdx_codes(market):
            df = read_day_frame(path, after_date=target)
            if df.empty:
                continue
            day = df[df["date"] == target]
            if day.empty:
                continue
            day = day.copy()
            day["symbol"] = suffix_for(market, code)
            frames.append(day[cols])
    if not frames:
        return pd.DataFrame(columns=cols)
    big = pd.concat(frames, ignore_index=True)
    big["volume"] = big["volume"].astype("float64")   # 对齐湖 schema：日线 volume=float64
    big["amount"] = big["amount"] / 10000.0           # 元 → 万元
    big["time"] = pd.to_datetime(big["date"].astype(str), format="%Y%m%d")
    out = big[["symbol", "time", "open", "high", "low", "close", "volume", "amount"]]
    return out.sort_values("symbol").reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--date", default=None, help="只处理指定交易日 YYYYMMDD（默认全部 ≥FLOOR）")
    args = ap.parse_args()

    if args.date:
        dates = {int(args.date)}
    else:
        dates = scan_day_dates(FLOOR)
    print(f"[daily] 待同步交易日: {sorted(dates)}", flush=True)
    for target in sorted(dates):
        df = build_day_partition(target)
        if df.empty:
            print(f"[daily] dt={target} 无数据，跳过", flush=True)
            continue
        out_dir = QUANTDB_DIR / "1_kline_data" / "daily_unadjusted" / f"dt={target}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "data.parquet"
        df.to_parquet(out_file, index=False)
        (out_dir / ".tdxlocal").touch()
        print(f"[daily] dt={target} -> {out_file}  rows={len(df)}  "
              f"symbols={df['symbol'].nunique()}  ({datetime.now():%H:%M:%S})", flush=True)


if __name__ == "__main__":
    main()
