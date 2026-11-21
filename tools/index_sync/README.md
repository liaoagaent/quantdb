# index_sync — 指数日线盘后同步

> 建于 2026-11-21。解决 A 股指数日线断供问题（QuantDB 云端 `index_daily` 停更于 20261106）。

## 目录约定（重要）

| 数据 | 落盘目录 | 说明 |
|---|---|---|
| 宽基/风格/主题指数（000xxx/399xxx/899xxx/000985） | `data/quantdb/1_kline_data/index_daily/` | 原有目录，schema/清单与 QuantDB 云端一致 |
| **申万行业指数（801xxx.SW）** | `data/quantdb/1_kline_data/index_industry/` | **独立新目录，不与 index_daily 混存**，避免影响按 index_daily 全量扫描的消费端（quantdb_hub / qlib 构建 / 基准识别） |

两者 schema 相同：`symbol(后缀式) time(datetime64[ns]) open high low close volume(股) amount(万元) Category`。

## 申万行业指数：sw_index_sync.py

```bash
# 全量回补（2000 年起，首次已执行，6514 个交易日分区）
.venv/Scripts/python.exe tools/index_sync/sw_index_sync.py --full-history --overwrite

# 每日盘后增量（默认从本地最新分区+1 补到今天，幂等）
.venv/Scripts/python.exe tools/index_sync/sw_index_sync.py

# 只看覆盖
.venv/Scripts/python.exe tools/index_sync/sw_index_sync.py --verify-only
```

- 行业清单：Tushare `index_classify(src=SW2021, level=L1)` 动态获取（31 个一级），`--levels L1,L2` 可加二级
- symbol：`801780.SI`（Tushare 原后缀）→ `801780.SW`（湖内后缀式规范）
- **单位（实测校准，与 index_daily 的手/千元约定不同！）**：sw_daily `vol=万股、amount=万元`——
  证据：R=amount/vol 对银行 9.0 / 家电 12.5 / 电力设备 15.9 / 医药 14.5 / 食品饮料 20.5 元/股，
  全部落在成交量加权平均价合理带；31 行业日成交额合计 ≈1.65 万亿元，与 CSI300（4400 亿）量级自洽。
  换算 `volume×1e4→股`、`amount 原样入湖（万元）`；脚本内置量级 sanity，异常拒绝写库。

## 宽基指数：index_sync.py

## 源优先级（操作员确认）

| 优先级 | 数据源 | 用途 | 备注 |
|---|---|---|---|
| P0 | 东财 push2his | 日常盘后同步 + 历史回补 | 单请求返回单指数全历史日线；有限频，用 `--sleep` 节流 |
| P1 | Tushare（一年权限） | 收盘价对账；申万行业指数补数（`sw_daily`） | token 在 `.env` 的 `TUSHARE_TOKEN`，不进仓库 |
| P2 | 通达信 vipdoc | 待接入：`shlday/`+`szlday/` 目录与个股同为 32B lday，可挂进 `tools/tdx_day_to_quantdb.py` 同一 SOP | T 日日终即可入湖，及时性最好 |
| 弃 | baostock（限频严、无北交所）/ akshare（爬虫封装，同撞东财限频） | | |

## 用法（项目根目录）

```bash
# 默认：补齐 lake 最新分区之后到今天的缺口（重复执行幂等，已存在分区自动跳过）
.venv/Scripts/python.exe tools/index_sync/index_sync.py

# 指定区间回补
.venv/Scripts/python.exe tools/index_sync/index_sync.py --since 20261107 --until 20261120

# 同步后用 Tushare 对账收盘价（000300/000905/399001）
.venv/Scripts/python.exe tools/index_sync/index_sync.py --tushare-check

# 只检查覆盖与 schema，不写库
.venv/Scripts/python.exe tools/index_sync/index_sync.py --verify-only
```

每日盘后（交易日 15:30 后）手动执行一次，或自行挂 cron / Windows 计划任务。
按 AGENTS.md 约定，本项目**不内置默认调度**，是否纳入前端「同步调度」由用户决定。

## 输出与规范

- 落盘：`data/quantdb/1_kline_data/index_daily/dt=YYYYMMDD/data.parquet`（与存量 Hive 布局一致）
- schema：`symbol(后缀式) time(datetime64) open high low close volume(股) amount(万元) Category`
- 单位换算：东财 volume(手)×100→股、amount(元)/1e4→万元；**每次运行都用湖内最新分区做重叠日实测校准，校准不过直接拒写**
- 指数清单：湖内最新分区已有 symbol + `EXTRA_INDICES`（当前新增 `000985.SH` 中证全指）；改清单直接编辑 `index_sync.py` 顶部 `EXTRA_INDICES`
- 东财 kline 字段顺序是 `日期,开,收,高,低,量,额`（收在生前），解析已按此处理

## 已知边界

- 东财对北证指数（899050 北证50）的覆盖以实测为准，取不到会在日志中告警缺席
- 申万一级 31 个行业指数已全历史入湖（`index_industry/`）；申万二级（`--levels L1,L2`）与中信一级（无免费稳定源，可用申万替代）按需扩展
- 盘中实时（秒级快照）走另一条链路：tqcenter 桥 → Redis db3 `market:snapshot:{code}` → stream WebSocket，见 `tools/l2_snapshot_collector.py` 与 stream 服务 `remote_redis_source.py`（注意：`tools/市场数据到redis.py` 依赖已删除的 tqcenter.py，属死代码，勿直接运行）
