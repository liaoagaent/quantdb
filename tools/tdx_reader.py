# -*- coding: utf-8 -*-
"""通达信 vipdoc 二进制文件读取器（2026-11-20 适配新版分钟线布局 + numpy 流式读取）。

文件格式（每条 32 字节，小端）：

.day  日线:
    <IIIIIfII>  date(u32 YYYYMMDD), open/high/low/close(u32, 价格×100 整数分),
                amount(f32, 元), volume(u32, 股), 保留(u32)

.lc1/.lc5  分钟线（每条 32 字节，存在新旧两种布局，自动识别）:
    旧版 <fffffIHH4x>  open/high/low/close(f32, 元), amount(f32, 元),
                       volume(u32, 股), date(u16), time(u16), 保留(4B)
    新版 <IfffffI4x>   header(u32 = time*65536 + date_code),  ← 2026-11 实测新格式
                       open/high/low/close(f32, 元), amount(f32, 元),
                       volume(u32, 股), 保留(4B)
    date 编码: (yy-2004)*2048 + mm*100 + dd   （rem 需 %100 切月日）
    time 编码: h*60 + m
"""
import struct
from pathlib import Path

import numpy as np
import pandas as pd

DAY_STRUCT = struct.Struct("<IIIIIfII")        # 32B
MIN_STRUCT = struct.Struct("<fffffIHH4x")      # 32B 旧版
MIN_STRUCT_NEW = struct.Struct("<IfffffI4x")   # 32B 新版（2026-11 通达信升级后实测）

assert DAY_STRUCT.size == 32
assert MIN_STRUCT.size == 32
assert MIN_STRUCT_NEW.size == 32

# numpy 结构化布局（与上述 struct 一一对应，用于向量化解析）
MIN_DTYPE_OLD = np.dtype([
    ("o", "<f4"), ("h", "<f4"), ("l", "<f4"), ("c", "<f4"),
    ("amt", "<f4"), ("vol", "<u4"), ("du", "<u2"), ("tu", "<u2"), ("res", "V4"),
])
MIN_DTYPE_NEW = np.dtype([
    ("hdr", "<u4"), ("o", "<f4"), ("h", "<f4"), ("l", "<f4"), ("c", "<f4"),
    ("amt", "<f4"), ("vol", "<u4"), ("res", "V4"),
])
assert MIN_DTYPE_OLD.itemsize == 32
assert MIN_DTYPE_NEW.itemsize == 32

_MIN_EMPTY = pd.DataFrame({
    "date": np.empty(0, dtype="i8"),
    "minute": np.empty(0, dtype="u2"),
    "open": np.empty(0, dtype="f4"),
    "high": np.empty(0, dtype="f4"),
    "low": np.empty(0, dtype="f4"),
    "close": np.empty(0, dtype="f4"),
    "amount": np.empty(0, dtype="f4"),
    "volume": np.empty(0, dtype="u4"),
})


def _date_from_code(dc):
    """u16 date 编码 → (yy, mm, dd)。"""
    yy = 2004 + (dc >> 11)
    rem = dc & 0x7FF
    return yy, rem // 100, rem % 100


def _date_ok(dc):
    yy, mm, dd = _date_from_code(dc)
    return 2004 <= yy <= 2040 and 1 <= mm <= 12 and 1 <= dd <= 31


def _min_layout(data):
    """探测分钟线文件布局：返回 'new' 或 'old'。头尾两条同时合法才判定。"""
    if len(data) >= 64:
        offs = (0, len(data) - 32)
        ok_new = True
        for o in offs:
            u = struct.unpack_from("<I", data, o)[0]
            if not (_date_ok(u & 0xFFFF) and (u >> 16) <= 1439):
                ok_new = False
                break
        if ok_new:
            return "new"
        if all(_date_ok(struct.unpack_from("<H", data, o + 24)[0]) for o in offs):
            return "old"
    return "new"


def read_day(path):
    """读 .day 日线文件 → list[dict(date, open, high, low, close, amount, volume)]。

    date 为 int YYYYMMDD；价格为元（已 ÷100）；amount 为元；volume 为股。
    """
    data = Path(path).read_bytes()
    n = len(data) // 32
    out = []
    unpack = DAY_STRUCT.unpack_from
    for i in range(n):
        d, o, h, l, c, amt, vol, _r = unpack(data, i * 32)
        out.append({
            "date": d,
            "open": o / 100.0,
            "high": h / 100.0,
            "low": l / 100.0,
            "close": c / 100.0,
            "amount": amt,
            "volume": vol,
        })
    return out


def read_day_last_close(path):
    """只读 .day 最后一根的 close（停牌股补价用，避免整文件解析）。缺文件/空文件返回 None。"""
    p = Path(path)
    try:
        size = p.stat().st_size
        if size < 32:
            return None
        with open(p, "rb") as f:
            f.seek(size - 32)
            buf = f.read(32)
        _d, _o, _h, _l, c, _amt, _vol, _r = DAY_STRUCT.unpack(buf)
        return c / 100.0
    except OSError:
        return None


def read_min_tail_date(path):
    """只读分钟线文件最后一条的 date（int YYYYMMDD）。空/坏文件返回 None。

    只读头尾各 32 字节判别布局，用于快速扫描"哪些交易日有数据"。
    """
    p = Path(path)
    try:
        size = p.stat().st_size
        if size < 32:
            return None
        with open(p, "rb") as f:
            f.seek(0)
            head = f.read(32)
            f.seek(size - 32)
            tail = f.read(32)
        layout = _min_layout(head + tail)
        if layout == "new":
            u = struct.unpack_from("<I", tail, 0)[0]
            yy, mm, dd = _date_from_code(u & 0xFFFF)
        else:
            du = struct.unpack_from("<H", tail, 24)[0]
            yy, mm, dd = _date_from_code(du)
        if not _date_ok((yy - 2004) * 2048 + mm * 100 + dd):
            return None
        return yy * 10000 + mm * 100 + dd
    except OSError:
        return None


def read_min_frame(path, after_date=None):
    """读 .lc1/.lc5 → DataFrame(date, minute, open, high, low, close, amount, volume)。

    numpy 向量化解析（比逐条 dict 快 1-2 个数量级）。
    after_date: 只保留 date >= after_date 的行（int YYYYMMDD），用于流式同步省内存。
    date 为 int YYYYMMDD；minute = h*60+m；价格为元；amount 为元；volume 为股。
    """
    p = Path(path)
    try:
        data = p.read_bytes()
    except OSError:
        return _MIN_EMPTY.copy()
    n = len(data) // 32
    if n == 0:
        return _MIN_EMPTY.copy()
    if _min_layout(data) == "new":
        arr = np.frombuffer(data, dtype=MIN_DTYPE_NEW, count=n)
        dc = (arr["hdr"] & 0xFFFF).astype("u2")
        tu = (arr["hdr"] >> 16).astype("u2")
    else:
        arr = np.frombuffer(data, dtype=MIN_DTYPE_OLD, count=n)
        dc = arr["du"]
        tu = arr["tu"]
    yy = 2004 + (dc >> 11)
    rem = dc & 0x7FF
    mm = rem // 100
    dd = rem % 100
    date_int = (yy.astype("i4") * 10000 + mm * 100 + dd).astype("i4")
    # 过滤非法日期行（脏数据保险）
    ok = (mm >= 1) & (mm <= 12) & (dd >= 1) & (dd <= 31) & (yy >= 2004) & (yy <= 2040) & (tu <= 1439)
    if after_date is not None:
        ok &= date_int >= np.int32(after_date)
    if not ok.all():
        idx = np.nonzero(ok)[0]
        arr = arr[idx]
        date_int = date_int[idx]
        tu = tu[idx]
    df = pd.DataFrame({
        "date": date_int.astype("i8"),
        "minute": tu,
        "open": arr["o"].astype("f8"),
        "high": arr["h"].astype("f8"),
        "low": arr["l"].astype("f8"),
        "close": arr["c"].astype("f8"),
        "amount": arr["amt"].astype("f8"),
        "volume": arr["vol"].astype("u4"),
    })
    return df


def read_min(path, after_date=None):
    """读 .lc1/.lc5 分钟线文件 → list[dict(date, time, open, high, low, close, amount, volume)]。

    date 为 int YYYYMMDD；time 为 'HH:MM' 字符串；价格为元；amount 为元；volume 为股。
    after_date: 只保留 date >= after_date 的行。兼容旧接口；大批量请用 read_min_frame。
    """
    df = read_min_frame(path, after_date=after_date)
    if df.empty:
        return []
    out = []
    for d, tu, o, h, l, c, amt, vol in zip(
            df["date"], df["minute"], df["open"], df["high"], df["low"],
            df["close"], df["amount"], df["volume"]):
        out.append({
            "date": int(d),
            "time": "%02d:%02d" % (tu // 60, tu % 60),
            "open": float(o),
            "high": float(h),
            "low": float(l),
            "close": float(c),
            "amount": float(amt),
            "volume": int(vol),
        })
    return out


def min_date_str(du):
    """u16 date 编码 → 'YYYYMMDD' 字符串。"""
    yy, mm, dd = _date_from_code(du)
    return "%04d%02d%02d" % (yy, mm, dd)


# ── 日线 numpy 向量化读取 ─────────────────────────────────────────────

DAY_DTYPE = np.dtype([
    ("date", "<u4"), ("o", "<u4"), ("h", "<u4"), ("l", "<u4"), ("c", "<u4"),
    ("amt", "<f4"), ("vol", "<u4"), ("res", "<u4"),
])
assert DAY_DTYPE.itemsize == 32

_DAY_EMPTY = pd.DataFrame({
    "date": np.empty(0, dtype="i8"),
    "open": np.empty(0, dtype="f8"),
    "high": np.empty(0, dtype="f8"),
    "low": np.empty(0, dtype="f8"),
    "close": np.empty(0, dtype="f8"),
    "amount": np.empty(0, dtype="f8"),
    "volume": np.empty(0, dtype="u4"),
})


def read_day_frame(path, after_date=None):
    """读 .day 日线 → DataFrame(date, open, high, low, close, amount, volume)。

    date 为 int YYYYMMDD；价格为元（÷100）；amount 为元；volume 为股。
    after_date: 只保留 date >= after_date 的行（int YYYYMMDD）。
    """
    p = Path(path)
    try:
        data = p.read_bytes()
    except OSError:
        return _DAY_EMPTY.copy()
    n = len(data) // 32
    if n == 0:
        return _DAY_EMPTY.copy()
    arr = np.frombuffer(data, dtype=DAY_DTYPE, count=n)
    d = arr["date"].astype("i4")
    ok = (d >= 19900101) & (d <= 20401231)
    if after_date is not None:
        ok &= d >= np.int32(after_date)
    if not ok.all():
        idx = np.nonzero(ok)[0]
        arr = arr[idx]
        d = d[idx]
    return pd.DataFrame({
        "date": d.astype("i8"),
        "open": arr["o"].astype("f8") / 100.0,
        "high": arr["h"].astype("f8") / 100.0,
        "low": arr["l"].astype("f8") / 100.0,
        "close": arr["c"].astype("f8") / 100.0,
        "amount": arr["amt"].astype("f8"),
        "volume": arr["vol"],
    })


def read_day_tail_date(path):
    """只读 .day 文件最后一条的 date（int YYYYMMDD）。空/坏文件返回 None。"""
    p = Path(path)
    try:
        size = p.stat().st_size
        if size < 32:
            return None
        with open(p, "rb") as f:
            f.seek(size - 32)
            buf = f.read(32)
        d = struct.unpack_from("<I", buf, 0)[0]
        if 19900101 <= d <= 20401231:
            return int(d)
        return None
    except OSError:
        return None
