#!/usr/bin/env python3
"""从 Wind L2 7z 日包里只抽「成交笔数」，不落 tick、不解逐笔。

Wind L2 包（如 `20261113.7z`）结构：
    20261113/000001.SZ/行情.csv      十档快照（含「成交笔数」「当日成交额」）
    20261113/000001.SZ/逐笔委托.csv  占包体 90%+，本脚本完全不碰
    20261113/000001.SZ/逐笔成交.csv  同上

`行情.csv` 是 GBK 编码，价格 ×10000。理想反转只需要「当日成交笔数」与成交额，
所以本脚本只解出 `*/行情.csv`（每只几十 KB），跑完即可删包。

落盘：data/quantdb/5_technical_derived/trade_count_daily.parquet
      与 l2_ticks_daily.py / tencent_ticks_daily.py 同 schema，可互相覆盖。

用法:
    # 先看包里有什么、行情.csv 占多少体积（不解压）
    python backend/scripts/wind_l2_trade_counts.py --archive D:/l2/20261113.7z --dry-run

    # 抽笔数（需要 7-Zip 的 7z.exe；没有则自动退回 py7zr，速度慢）
    python backend/scripts/wind_l2_trade_counts.py --archive D:/l2/20261113.7z

    # 只抽部分
    python backend/scripts/wind_l2_trade_counts.py --archive ... --limit 50
    python backend/scripts/wind_l2_trade_counts.py --archive ... --symbols 000001.SZ,600519.SH
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("wind_l2_trade_counts")

QUOTE = "行情.csv"
PRICE_SCALE = 10000.0
CODE_RE = re.compile(r"^(?P<code>\d{6}\.[A-Z]{2})$")


def data_dir() -> Path:
    env = os.getenv("QM_QUANTDB_DATA_DIR", "").strip()
    return Path(env) if env else PROJECT_ROOT / "data" / "quantdb"


def find_7z(explicit: str | None) -> str | None:
    for cand in ([explicit] if explicit else []) + ["7z", "7za",
                                                   r"C:\Program Files\7-Zip\7z.exe",
                                                   r"C:\Program Files (x86)\7-Zip\7z.exe",
                                                   "/opt/p7zip-legacy/bin/7z", "/usr/bin/7z"]:
        if not cand:
            continue
        p = Path(cand)
        if p.is_file():
            return str(p)
        found = shutil.which(cand)
        if found:
            return found
    return None


def list_members(archive: Path, seven_zip: str | None) -> list[tuple[str, int]]:
    """返回 [(成员路径, 字节数)]。"""
    if seven_zip:
        proc = subprocess.run([seven_zip, "l", "-slt", str(archive)],
                              capture_output=True, text=True, errors="replace")
        if proc.returncode != 0:
            raise RuntimeError(f"7z l 失败: {proc.stderr[:300]}")
        out, cur = [], {}
        for line in proc.stdout.splitlines():
            if line.startswith("Path = "):
                cur = {"path": line[7:].strip()}
            elif line.startswith("Size = "):
                cur["size"] = line[7:].strip()
            elif not line.strip() and cur.get("path"):
                try:
                    out.append((cur["path"], int(cur.get("size") or 0)))
                except ValueError:
                    pass
                cur = {}
        return out
    import py7zr

    with py7zr.SevenZipFile(archive, mode="r") as z:
        return [(i.filename, i.uncompressed or 0) for i in z.list()]


def is_quote(member: str) -> bool:
    return member.replace("\\", "/").endswith("/" + QUOTE)


def member_code(member: str) -> str | None:
    parts = member.replace("\\", "/").split("/")
    for p in reversed(parts[:-1]):
        if CODE_RE.match(p):
            return p
    return None


def extract(archive: Path, members: list[str], tmp: Path, seven_zip: str | None) -> None:
    if seven_zip:
        for i in range(0, len(members), 200):
            batch = members[i:i + 200]
            proc = subprocess.run([seven_zip, "x", f"-o{tmp}", str(archive), *batch, "-y"],
                                  capture_output=True, text=True, errors="replace")
            if proc.returncode not in (0, 1):
                raise RuntimeError(f"7z x 失败: {proc.stderr[:300]}")
        return
    import py7zr

    with py7zr.SevenZipFile(archive, mode="r") as z:
        z.extract(path=tmp, targets=members)


def parse_quote(path: Path, symbol: str) -> dict | None:
    try:
        df = pd.read_csv(path, encoding="gbk",
                         usecols=["成交笔数", "当日成交额", "当日累计成交量"])
    except Exception as e:  # noqa: BLE001
        log.debug("解析失败 %s: %s", path, e)
        return None
    if df.empty:
        return None
    last = df.iloc[-1]
    n = pd.to_numeric(last["成交笔数"], errors="coerce")
    amt = pd.to_numeric(last["当日成交额"], errors="coerce") / PRICE_SCALE
    vol = pd.to_numeric(last["当日累计成交量"], errors="coerce")
    if pd.isna(n) or n <= 0:
        return None
    return {"symbol": symbol, "num_trades": int(n), "amount": float(amt),
            "volume": float(vol) if pd.notna(vol) else float("nan"),
            "avg_trade_amt": float(amt) / int(n)}


def main() -> int:
    ap = argparse.ArgumentParser(description="Wind L2 7z → 每日成交笔数（只解 行情.csv）")
    ap.add_argument("--archive", required=True, help="7z 日包路径（文件名须为 YYYYMMDD.7z）")
    ap.add_argument("--symbols", default=None, help="逗号分隔，如 000001.SZ,600519.SH")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true", help="只汇总包内结构与体积")
    ap.add_argument("--7z", dest="seven_zip", default=None, help="7z 可执行文件路径")
    ap.add_argument("--keep-temp", action="store_true")
    args = ap.parse_args()

    archive = Path(args.archive)
    if not archive.is_file():
        raise SystemExit(f"找不到压缩包: {archive}")
    date_str = archive.stem
    if not re.fullmatch(r"\d{8}", date_str):
        raise SystemExit(f"文件名必须是 YYYYMMDD.7z，实际: {archive.name}")

    seven_zip = find_7z(args.seven_zip)
    log.info("压缩包 %s（%.2f GB），解压工具 %s", archive.name,
             archive.stat().st_size / 1e9, seven_zip or "py7zr")
    members = list_members(archive, seven_zip)
    quotes = [(m, s) for m, s in members if is_quote(m)]
    total_bytes = sum(s for _, s in members)
    quote_bytes = sum(s for _, s in quotes)
    log.info("成员 %d 个，合计 %.2f GB；其中 %s %d 个，%.2f GB（占 %.1f%%）",
             len(members), total_bytes / 1e9, QUOTE, len(quotes), quote_bytes / 1e9,
             100 * quote_bytes / total_bytes if total_bytes else 0)
    if args.dry_run:
        kinds: dict[str, tuple[int, int]] = {}
        for m, s in members:
            name = m.replace("\\", "/").rsplit("/", 1)[-1]
            kinds[name] = (kinds.get(name, (0, 0))[0] + 1, kinds.get(name, (0, 0))[1] + s)
        for name, (c, sz) in sorted(kinds.items(), key=lambda x: -x[1][1])[:10]:
            log.info("  %-16s %5d 个  %7.2f GB", name, c, sz / 1e9)
        return 0

    if args.symbols:
        want = {s.strip().upper() for s in args.symbols.split(",") if s.strip()}
        quotes = [(m, s) for m, s in quotes if member_code(m) in want]
    if args.limit:
        quotes = quotes[: args.limit]
    if not quotes:
        raise SystemExit("没有可抽的 行情.csv")
    log.info("准备抽取 %d 只（%.1f MB）", len(quotes), quote_bytes / 1e6)

    tmp = Path(tempfile.mkdtemp(prefix="l2q_"))
    t0 = time.time()
    rows, failed = [], 0
    try:
        extract(archive, [m for m, _ in quotes], tmp, seven_zip)
        for m, _ in quotes:
            sym = member_code(m)
            f = tmp / m.replace("\\", "/")
            if not sym or not f.is_file():
                failed += 1
                continue
            rec = parse_quote(f, sym)
            if rec:
                rec["date"] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                rows.append(rec)
            else:
                failed += 1
    finally:
        if args.keep_temp:
            log.info("临时目录保留: %s", tmp)
        else:
            shutil.rmtree(tmp, ignore_errors=True)

    if not rows:
        log.warning("没有解析到数据（%d 只失败）", failed)
        return 1
    out_new = pd.DataFrame(rows)[["symbol", "date", "num_trades", "avg_trade_amt",
                                  "volume", "amount"]]
    dst = data_dir() / "5_technical_derived" / "trade_count_daily.parquet"
    if dst.exists():
        old = pd.read_parquet(dst)
        out_new = (pd.concat([old, out_new], ignore_index=True)
                   .drop_duplicates(subset=["symbol", "date"], keep="last"))
    dst.parent.mkdir(parents=True, exist_ok=True)
    out_new.to_parquet(dst, index=False)
    log.info("完成：%d 只，失败 %d，累计 %d 行 → %s（%.0fs）",
             len(rows), failed, len(out_new), dst, time.time() - t0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
