"""聚宽（JoinQuant / JQData）数据能力探测。

用途：拿到 JQData 账号后先跑这个，用真实返回值确认「有哪些频率、各频率历史到哪一年」，
而不是照抄文档。结论直接打印成表格，可粘进 docs/research/2026-11-14_开源交易行为因子接入评估.md。

用法：
    set JQ_USER=你的手机号
    set JQ_PASSWORD=你的密码
    .venv\\Scripts\\python.exe backend\\scripts\\jq_capability_probe.py
    # 或
    .venv\\Scripts\\python.exe backend\\scripts\\jq_capability_probe.py --symbol 600519.XSHG

注意：
    - 每次试探都会消耗 JQData 的流量配额（get_query_count() 可查剩余），别在生产账号上反复跑。
    - 探测用极小 count（3 根 bar / 3 条 tick），成本可忽略。
"""

from __future__ import annotations

import argparse
import os
import sys
import time

# Windows 控制台默认 GBK，中文表格会乱码
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    except Exception:
        pass

PROBE_DATES = ["2005-01-04", "2010-01-04", "2015-01-05", "2020-01-02", "2024-01-02"]
FREQUENCIES = ["1m", "5m", "15m", "30m", "60m", "120m", "1d"]


def _import_sdk():
    try:
        import jqdatasdk as jq  # noqa: PLC0415
    except ImportError:
        print("缺少 jqdatasdk，先执行：.venv\\Scripts\\python.exe -m pip install jqdatasdk")
        raise SystemExit(1)
    return jq


def probe_price(jq, symbol: str) -> list[tuple[str, str, str]]:
    """逐个频率 × 起始日试探，记录能取到数据的最早起点。"""
    rows: list[tuple[str, str, str]] = []
    for freq in FREQUENCIES:
        earliest = ""
        note = ""
        for start in PROBE_DATES:
            try:
                df = jq.get_price(
                    symbol,
                    start_date=start,
                    count=3,
                    frequency=freq,
                    fields=["open", "close", "volume", "money"],
                    skip_paused=True,
                    panel=False,
                )
                got = 0 if df is None else len(df)
                if got > 0:
                    earliest = start
                    note = f"{got} 行, money 字段可用"
                    break
                note = note or "返回空"
            except Exception as exc:  # 权限不足 / 频率不支持 都落这里
                note = f"异常: {type(exc).__name__}: {str(exc)[:80]}"
                break
            time.sleep(0.05)
        rows.append((freq, earliest or "-", note))
    return rows


def probe_ticks(jq, symbol: str) -> list[tuple[str, str, str]]:
    """tick 是五档快照，不是逐笔成交；这里确认历史深度与字段。"""
    rows: list[tuple[str, str, str]] = []
    for start in PROBE_DATES:
        try:
            df = jq.get_ticks(symbol, start_dt=start, count=3)
            if df is not None and len(df) > 0:
                cols = ",".join(list(df.columns)[:8])
                rows.append((start, f"{len(df)} 条", f"列: {cols}"))
                break
            rows.append((start, "空", "该起点无 tick"))
        except Exception as exc:
            rows.append((start, "异常", f"{type(exc).__name__}: {str(exc)[:80]}"))
            break
        finally:
            time.sleep(0.05)
    return rows


def probe_call_auction(jq, symbol: str) -> str:
    """集合竞价 tick：正好补腾讯分笔缺失的那部分成交量。"""
    try:
        df = jq.get_call_auction(symbol, start_date="2026-11-10", end_date="2026-11-13")
        if df is None or len(df) == 0:
            return "无数据（可能权限不足或当日无竞价记录）"
        return f"{len(df)} 条，列: {','.join(df.columns)}"
    except Exception as exc:
        return f"异常: {type(exc).__name__}: {str(exc)[:100]}"


def main() -> int:
    ap = argparse.ArgumentParser(description="JQData 数据能力探测")
    ap.add_argument("--symbol", default="600519.XSHG", help="探测标的，默认贵州茅台")
    ap.add_argument("--user", default=os.getenv("JQ_USER", ""))
    ap.add_argument("--password", default=os.getenv("JQ_PASSWORD", ""))
    args = ap.parse_args()

    if not args.user or not args.password:
        print("未提供账号：设置环境变量 JQ_USER / JQ_PASSWORD，或传 --user / --password")
        return 2

    jq = _import_sdk()
    jq.auth(args.user, args.password)
    print(f"登录成功，标的 {args.symbol}\n")

    try:
        priv = jq.get_privilege()
        print("当前权限:")
        for key, val in (priv.items() if isinstance(priv, dict) else [("raw", priv)]):
            print(f"  {key}: {val}")
    except Exception as exc:
        print(f"权限查询失败: {exc}")
    print()

    print("一、K 线频率 × 历史起点（get_price）")
    print("| 频率 | 最早可取起点 | 备注 |")
    print("|---|---|---|")
    for freq, earliest, note in probe_price(jq, args.symbol):
        print(f"| {freq} | {earliest} | {note} |")

    print("\n二、tick 历史深度（get_ticks，五档快照而非逐笔）")
    print("| 起点 | 结果 | 字段 |")
    print("|---|---|---|")
    for start, got, note in probe_ticks(jq, args.symbol):
        print(f"| {start} | {got} | {note} |")

    print("\n三、集合竞价 tick（get_call_auction）")
    print(probe_call_auction(jq, args.symbol))

    try:
        print(f"\n剩余流量: {jq.get_query_count()}")
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
