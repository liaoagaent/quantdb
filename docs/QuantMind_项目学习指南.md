# QuantMind 项目学习指南（完整版）

> 面向新加入成员。目标：理解项目全貌、数据从哪来、因子/模型体系、部署运维，并能独立完成日常数据同步与验收。
> 更新：2026-11-20。基于仓库源码与实战过程整理，关键结论均标注出处文件。
> 配套文档：`docs/` 目录下《模型训练完全指南》《因子与特征参考》《默认策略手册》《QuantDB_数据包解压指南》《通达信数据同步_目录约定与踩坑》《部署指南》等。

---

## 1. 项目是什么

QuantMind 是**开源量化交易平台**，覆盖 A 股、港股、美股、加密货币、期货五个市场。核心能力：

- 多市场数据同步与本地数据湖（QuantDB）
- Qlib 回测、AI 策略生成、因子挖掘（AlphaAgent / RD-Agent）、模型训练与推理
- 模拟交易 + 通达信实盘桥
- 实时行情 WebSocket 推送
- QuantBot 技能中心（投研报告生成，md+PDF 归档）

技术栈：**后端 Python/FastAPI + Celery，前端 Electron/React/TypeScript，PostgreSQL + Redis，回测 Qlib，Docker 单镜像部署**。

## 2. 后端架构

统一入口 `backend/main_oss.py`（OSS 版单容器跑四个服务）：

| 服务 | 端口 | 职责 |
|------|------|------|
| api | 8000 | 认证、策略管理、社区、新闻代理 |
| engine | 8001 | Qlib 回测、AI 策略、模型推理、数据平台、Alpha Agent |
| trade | 8002 | 订单、持仓、风控（外部报单前"本地优先"落库） |
| stream | 8003 | 实时行情、WebSocket 推送 |

- 共享模块 `backend/shared/`：DB/Redis 管理器、配置、日志、`utc_datetime`（瞬时列一律 TIMESTAMPTZ+aware UTC）、`stock_utils`、`strategy_storage`（策略增删改查唯一入口）、`version.py`。
- **Celery worker 必须唯一**：`SERVICE_MODE=all` 下 `main_oss.py:211` 的 `EMBEDDED_CELERY_WORKER` 默认 false，只有 `celery-worker` 容器消费 `qlib_backtest_srv` 队列。重复 worker 会瓜分消息 → 定时任务"随机不执行"。排查：`redis-cli client list | grep cmd=brpop` 应只有 1 个。
- **Redis 库分配**：0=通用，1=认证，2=交易，3=行情，4=回测，5=缓存。Redis 必须配 `noeviction`（LRU 会静默丢队列消息）。
- 测试：`python backend/run_tests.py unit|integration|all`。

## 3. 代码与单位规范（高频踩坑区）

**股票代码分层，禁止跨层混用：**

| 层 | 格式 | 示例 |
|---|---|---|
| QuantDB parquet / Qlib 数据层 | 后缀式 | `600036.SH` |
| Qlib 桥接边界 | 全小写 | `sh600036` |
| PG 字段 / Redis 键 / 前端 | 前缀式 | `SH600036` |

- 转换唯一入口：后端 `backend/shared/stock_utils.py` → `StockCodeUtil.to_suffix()/to_prefix()/to_qlib()`；前端 `electron/src/utils/portfolioUtils.ts` → `normalizeSymbol()`。禁止手写切片。
- 市场识别：SH=60/68/90，SZ=00/30/20，BJ=43/83/87/88/92。脱离 backend 环境的独立脚本（如 tools/ 下通达信工具）按号段镜像实现并注明"以 stock_utils 为准"。
- **单位约定**：日线 volume=股、amount=元；分钟线 volume=股、**amount=万元**；快照 Volume=手、Amount=万元。跨数据集对账时先统一单位（如日 K 与分钟 K 互验：000001.SZ 20261120 日线 vol=213,498,896 股 = 分钟线 vol_sum=213,498,900 股）。

## 4. QuantDB 本地数据湖

位置：`data/quantdb/`（容器内 `/data/quantdb`，由 `QM_QUANTDB_DATA_DIR` 控制；**data/ 有 10 万+ 文件，禁止递归遍历，一律精确路径**）。

```
data/quantdb/
├── 1_kline_data/            # K线主数据
│   ├── daily_unadjusted/    # 日K 不复权（撮合/涨跌停判定）
│   ├── daily_backward/      # 后复权（依赖除权因子，可能滞后）
│   ├── daily_forward/       # 前复权（特征计算主用，除权日全量重写）
│   ├── index_daily/         # 指数日线
│   ├── min1_kline/          # 1分钟K（~135.5万行/日，~5,646只）
│   ├── min5_kline/          # 5分钟K（~27.1万行/日）
│   ├── minute_5m/           # 5分钟线按标的存放（聪明钱因子用，东财增量+baostock回补）
│   └── l2_snapshot/         # 盘中快照 jsonl：dt=.../round=001/batch=NNN.jsonl（200只/批）
├── 2_base_sector/           # instrument_detail(152列)、sector_concept、index_weights、
│                            # trading_calendar、margin_trading(融资融券)…
├── 3_financial_data/        # 财务八件套：balance/income/cashflow/capital/pershare_index/
│                            # per_share/dividend_factors/holder_num（按标的分文件）
├── 4_bond_etf/              # etf_pcf、convertible_bond（初始包带入，无每日任务）
├── 5_technical_derived/     # valuation、technical_indicators、market_sentiment、
│                            # tdx_exday_daily.parquet(CJBS)、trade_count_daily…
├── 6_ml_datasets/           # features_daily、l1_factors、l1_l2_factors、factor_dictionary(571列)、
│                            # alpha_library(429列)…
├── _cache/                  # Qlib 二进制缓存(.qlib_cache/cn_data)、adj_factor 等派生产物
└── .sync_state/             # 增量同步状态库（SQLite，objects/releases 表做指纹对账）
```

**三种存储 layout**（`backend/shared/quantdb_datasets.py:44-79`）：
1. 按交易日分区：`1_kline_data/daily_forward/dt=YYYYMMDD/data.parquet`（K 线/因子类）
2. 按标的分文件：`3_financial_data/balance/{symbol}.parquet`（财务类）
3. 单文件：`2_base_sector/instrument_detail/instrument_detail.parquet`（基础信息类）

- 读取统一入口：`backend/services/engine/data_platform/quantdb_hub.py`（DuckDB 视图 + 精确分区路径读取 + UNIVERSE_MAP 股票池）。
- `.tdxlocal` 标记文件 = 该分区来自通达信本地同步（区别于远程数据包）。

## 5. 数据从哪来（全景）

> 一句话：A 股主干 = **QuantDB 云端 SDK 每日增量**（付费 CDN，sha256/ETag 指纹对账）；首次安装 = **ModelScope 数据集全量初始化**；2026-11-18 起不复权日线与分钟线由**本机通达信 vipdoc 接管**；GitHub investment_data 与 baostock/akshare/eltdx 为 PG 日线层多级兜底；美股/港股走 Yahoo Finance + akshare，加密走 Binance，期货走 akshare。所有同步均显式触发，项目不内置默认调度。

### 5.1 A 股主链路：`backend/scripts/quantdb_daily_sync.py`（1,511 行）

**数据源**：quantdb-sdk（付费 CDN，API Key 取运行时密钥 `QUANTDB_API_KEY`；parquet 经 CDN 两跳 302 分发）。

**同步机制**（:294-520）：
- **V2 分区数据集 12 个**：daily_unadjusted/forward/backward、index_daily、margin_trading、valuation、technical_indicators、market_sentiment、features_daily、l1_factors、factor_dictionary、l1_l2_factors。先查 `/api/v1/data/releases` 增量 + `query_manifest` 补漏，按服务端 **sha256 指纹对账**（不一致即重下），8 线程下载、每 100 分区落一次状态。
- **V1 全量数据集 13 个**：sector_concept、instrument_detail、index_weights、trading_calendar、balance、income、cashflow、capital、pershare_index、per_share、dividend_factors、holder_num。manifest + **ETag(md5)** 对账，未变即跳过。

**四阶段**：
1. SDK 拉取 → 写 `data/quantdb` parquet（`.part` 原子落盘，sqlite 状态库记录）
2. backfill_l1_ohlcv：用 daily_backward 回填 l1_factors 历史 OHLCV
3. 回填 PG `stock_daily_latest`（DuckDB 合并 daily_forward + features_daily，`_FEATURE_COLS` 直取 + 价格派生列前复权重算）
4. `ensure_qlib_cache()`：从 parquet 生成 Qlib 二进制缓存 `data/quantdb/.qlib_cache/cn_data`

**状态库**：`.sync_state/quantdb_sync_<数据根>.sqlite`（WAL），`objects` 表记录每个云端对象 key/etag/sha256/size/path/layout/dataset，`releases` 表记录各数据集已同步 release_id；`--reseed-state` 可用本地文件重建状态库（换目录后免全量重拉）。env `QUANTDB_STATE_DIR` 必须固定到 bind mount，容器重建不丢状态。

### 5.2 首次全量初始化

- `backend/services/engine/data_platform/modelscope_dataset_sync.py`：拉取魔搭公开数据集 **`qusong0627/LightGBM_Alpha300`**（旧百度网盘 quant_data.7z 已下线）。入口 = 管理后台【数据管理】→【初始化数据】（`backend/services/api/routers/admin/quantdb_console.py`）。分页枚举远端 parquet 清单（含 sha256/size）→ 并发流式下载 → 逐文件校验、`os.replace` 原子覆盖 → 按远端 sha256 重建同步状态库（后续增量 fast-path 直接命中）。
- 历史包规模：6 大目录约 **56GB/7.4 万文件**（1_kline_data ~7.4GB、2_base_sector ~0.4GB、3_financial_data ~0.85GB、5_technical_derived ~9.3GB、6_ml_datasets ~39GB）；解压高峰需 ~92GB，完成后删压缩包回收 ~36GB。详见 `docs/QuantDB_数据包解压指南.md`。
- 遗留兜底：`backend/scripts/sync_investment_data.py` 从 GitHub `chenditc/investment_data` Releases 下 `qlib_bin.tar.gz` → `data/qlib/cn_data`（calendars/instruments/features）→ Qlib API 批量 UPSERT 进 PG `stock_daily_latest`。

### 5.3 多源兜底日线：`backend/scripts/daily_data_sync.py`（`--incremental/--full`）

优先级（文件头明示）：**QuantDB parquet**（最权威，T-1）→ **baostock**（稳定，前复权）→ **akshare**（东财 `stock_zh_a_hist`，失败再新浪）→ **eltdx**（mootdx 通达信，取当日 T，仅工作日）→ 逐级 fallback，UPSERT PG `stock_daily_latest` → ensure_qlib_cache → 校准 MA/收益率等指标。
另有 `minute_5m_sync.py`：5 分钟线两源分工——东财 push2his（单请求 1488 根≈31 交易日，每日增量）+ baostock（2020 至今一次性回补）→ `1_kline_data/minute_5m/{symbol}.parquet`，派生 `5_technical_derived/minute_derived.parquet`（vwap 聪明钱主项）。

### 5.4 市场 → 数据源 → 脚本 → 落盘 → 频率对照表

| 市场 | 数据源 | 摄取脚本 | 落盘 | 频率 |
|---|---|---|---|---|
| A股 | QuantDB 云端 SDK | quantdb_daily_sync.py | data/quantdb 五大目录 | 每日（调度） |
| A股 | ModelScope 初始化包 | modelscope_dataset_sync.py | data/quantdb 全量 | 一次性 |
| A股 | 通达信 vipdoc / tqcenter | tools\tdx_day_to_quantdb / tdx_min_to_quantdb / tdx_exday_daily | daily_unadjusted、min1/min5_kline、tdx_exday_daily | 每交易日日终（FLOOR=20261118 起接管） |
| A股 | baostock→akshare→eltdx | daily_data_sync.py | PG stock_daily_latest | 每日兜底 |
| A股 | 东财 push2his + baostock | minute_5m_sync.py | 1_kline_data/minute_5m | 每日增量 + 一次性回补 |
| 美股 | Yahoo Finance（yfinance） | quantus_daily_sync → global_market_sync.py | data/quantus（K线/财务/估值/分析师/期权链） | 每日；标的池 SP500_FULL+NASDAQ_EXTRA ≈540 只 |
| 美股 | akshare | akshare_index_sync、quantus_extra_datasets、sync_macro_usa | index_daily、us_universe、macro_usa | 每日 |
| 港股 | akshare（K线唯一源） | quanthk_daily_sync → quanthk_akshare_kline | data/quanthk/1_kline_data/daily_forward | 每日 |
| 港股 | Yahoo Finance | global_market_sync(market=HK) | 财务/估值/分析师/期权（HSI+HSTECH+热门 ≈82 只） | 每日 |
| 港股 | akshare/爬虫/中证 | akshare_hk_fundamental、quanthk_south_sync、quanthk_extra_datasets、quanthk_adjust_factor | hsgt_south、AH 溢价、adjust_factors、daily_backward | 每日/按勾选 |
| 加密 | Binance 公开 API（data-api.binance.vision） | quantbc_daily_sync → blockchain_sync | data/quantbc（daily_forward + valuation，默认 365 天） | 每日；top35 交易对；生产 `ENABLE_CRYPTO=false` 隐藏 |
| 期货 | akshare | quantfutures_daily_sync → akshare_futures_sync/_extra | data/quantfutures（外盘 30 + 国内 82 主力连续 + 上金所 + 仓单/持仓/CFTC/汇率） | 每日 |
| 自定义 | 用户自传 | — | data/quantcustom | 字段自动发现 |

**调度入口**：Celery Beat 任务 `engine.tasks.daily_data_sync`（A 股走 run_daily_sync，Redis 锁 `quantmind:daily_sync:lock` 防并发）；通用调度器 `market_sync_scheduler.run_market_sync` 按市场分发。**是否同步、何时触发一律以前端「同步调度」写入的 Redis 配置 `quantmind:sync_schedule:{market}` 为准，未配置 5 个市场全部不跑**（AGENTS.md 硬性规则）。

### 5.5 数据平台内部结构（backend/services/engine/data_platform/）

- `quantdb_hub.py`（1,536 行）：DuckDB 视图 + 精确分区路径读取 + UNIVERSE_MAP 股票池 + l1 因子类别目录。
- `adapters/`：19 个适配器——quantdb_local（A 股主源，本地 parquet）、quantdb（远程 SDK 实时查询兜底：日线/Tick/财务/315 维 AI 因子，不落盘）、akshare/baostock/efinance/eltdx/yahoo 等。`config/data_sources/field_routing.yaml`：A 股全字段 primary=quantdb_local；HK/US daily=yahoo + efinance/akshare fallbacks。
- `qlib_data_builder.py`（934 行）：parquet → Qlib 二进制缓存。CN 复权因子 = dividend_factors 事件表累乘（**乘法因子，防加法漂移**）；市场前缀 `sh600036 / us_AAPL / hk_00700 / bc_BTCUSDT / fut_CL.FUT`；CN 额外补沪深300/中证500/1000 指数。

## 6. 通达信本地链路（tdxlocal）

本机 A 股分钟级数据的来源：**通达信客户端 →（vipdoc 二进制 / tqcenter 桥）→ tools 同步工具 → parquet 湖**。

### 6.1 vipdoc 二进制格式（tools/tdx_reader.py）

- `.day`（lday/）：32 字节/条 `<IIIIIfII>`，价格×100（整数分）。
- `.lc1/.lc5`（minline//fzline/）：**新版 32 字节/条 `<IfffffI4x>`**——头部 u32 = `time*65536 + date_code`；o/h/l/c/amount 为 f32（元）；vol u32（股）；`date_code=(yy-2004)*2048+mm*100+dd`（日=code%100），time=h*60+m。旧版 `<fffffIHH4x>`（date/time 在 offset 24）仍需兼容——按头尾两条解码合法性自动判别布局。
- ⚠️ 通达信运行时**独占锁定 vipdoc**，`Get-ChildItem -ErrorAction SilentlyContinue` 会返回假空；判断文件用 `Test-Path`/`[IO.File]::Exists` 逐个确认。

### 6.2 tqcenter HTTP 桥（127.0.0.1:17709，客户端运行时开放，JSON-RPC）

- `get_market_snapshot(code)`：实时快照+买卖五档（现有通道最细盘中粒度，无逐笔/十档）。
- `get_exday_data(code, count)`：**日级 L2 资金统计（CJBS）**，需专业版权限。
- `get_stock_list` / `get_market_data` / `get_financial_data` 等；封装见 `tools/bridge-windows/src/tdx/client.py`，自愈保活见 `tools/bridge-windows/`。

### 6.3 同步工具四件套（tools/，约定：同步脚本一律放这里）

| 工具 | 作用 | 要点 |
|---|---|---|
| tdx_reader.py | 二进制解析库 | numpy 结构化 dtype 向量化；`read_*_frame` 全量、`read_*_tail_date` 只读头尾 |
| tdx_min_to_quantdb.py | 分钟线入湖 | `--kind min`；`FLOOR=20261118` 增量；**流式**（先扫日期集合，逐日逐文件只留当日切片）；min5 剔 13:00；白名单号段（000/001/002/003/300/301/302/600/601/603/605/688/689/920）剔除转债/ETF |
| tdx_day_to_quantdb.py | 日线入湖 | `--date YYYYMMDD`；同构流式；不补停牌 |
| tdx_exday_daily.py | CJBS 入湖 | 逐只过桥拉取（~5,600 只，前台 1-2 分钟）；(symbol,date) 去重 keep=last，幂等 |
| verify_sync.py | 验收 | 打印 min1/min5/daily/exday 各交易日覆盖 + 000001.SZ 抽样 |

### 6.4 每日盘后 SOP（A 股）

```bash
# 前提：通达信 TimeDown=1（盘后自动下载K线；NotifyDownLoad=0 静音）
python tools\tdx_min_to_quantdb.py --kind min        # 1) 分钟线
python tools\tdx_day_to_quantdb.py --date 20261120   # 2) 日线
python tools\tdx_exday_daily.py                      # 3) CJBS（需客户端在线）
python tools\verify_sync.py                          # 4) 验收
```

### 6.5 CJBS 与 FirstGPType（重要运维知识）

- CJBS=成交单数；`get_exday_data` 字段：Vol(4×4 特大/大/中/小 × 买/卖/主买/主卖)、Amo(4×4 万元)、VolNum(2×2 笔数)、BOrder/BCancel/SOrder/SCancel、BuyAvp/SellAvp、TotalBOrder/TotalSOrder → 湖内 26 列 schema 见 `tools/tdx_exday_daily.py`。
- **客户端只有启动后默认进入某类行情页（user.ini `FirstGPType=6`）才会自动批量拉取全市场 L2 统计**；=15 时只有手动点开过的股票有数据。改 user.ini 必须先退出通达信（运行时周期回写），且 user.ini 是 **GBK 编码**。

### 6.6 盘中采集

- `tools/l2_snapshot_collector.py`：快照+五档采集器（HTTP 桥版）。30s/轮、**200 只/批**、每批一个 `batch=NNN.jsonl`（首行 meta，每股一行 quote，毫秒 captured_at），batch 当日递增、重跑不覆盖；非交易时段自动休眠。
- `tools/市场数据到redis.py`：快照轮询 → Redis DB3 `market:snapshot:{code}` + publish `qdb:rt:quote:{symbol}` → stream 推 WebSocket（依赖的 tqcenter.py 已删，需走 HTTP 桥）。
- 逐笔/十档 L2：tqcenter 无接口；Wind CSV 走 `backend/scripts/wind_l2_import.py` 离线导入。

## 7. 因子体系（含"新因子"的数据依赖）

### 7.1 因子分几层（先分清口径，别被旧数字误导）

| 层 | 数量 | 位置 | 生成者 |
|---|---|---|---|
| 日频特征因子（动态截面） | **200 个**（FEATURE_COLS 201 项） | `6_ml_datasets/features_daily/` | `backend/scripts/update_feature_parquet.py` |
| AI 因子 L1 | 实测 119 列 | `6_ml_datasets/l1_factors/` | factor_factory 表达式组合 + IC 筛选 |
| AI 因子 L2（微观结构） | 15 子类 | `6_ml_datasets/l1_l2_factors/` | 同上 |
| 经典因子库 | **429 列**（Alpha158+Alpha101+GTJA170） | `6_ml_datasets/alpha_library/` | `alpha_library_factors.py` |
| RD-Agent 挖掘因子 | IC>0.05/ICIR>0.5 的产出 | PG `rd_agent_factors` 表 | RD-Agent FactorRDLoop |
| 旧口径（仅历史文档） | 315（L1/L2 旧称）、48（market_data_daily 外部特征）、151（旧特征数）、571（V5 因子字典） | — | 已被上表取代 |

> 常见疑问"115 个因子"：仓库中无 115 维清单；最接近的现行口径是 l1_factors 实测 119 列。

### 7.2 update_feature_parquet.py：200 个因子与数据依赖

25 组因子（动量 10+新增 9、波动 8+新增 12、流动性 11+新增 9、资金流 6+新增 4、风格 6+新增 4、行业 18、辅助 2、DB 基本面 8、指数成分/概念 15、DB 技术指标 31、Alpha158K 线 9+价格相对 4、价格位置 5、波动调整动量 4、量价配合 4、趋势质量 3、时序滞后 2、第六梯队 13、基本面补充 2、行业编码 2）。

**数据依赖（要跑通新因子，这些表必须先就位）**：

| 依赖 | 内容 | 来源 |
|---|---|---|
| daily_forward（前复权） | OHLCV | 1_kline_data/daily_forward |
| valuation | pe、pb、mv、net_profit、equity、cap | 3_financial_data |
| technical_indicators | MACD/KDJ/RSI 等 31 项 | 5_technical_derived |
| instrument_detail | is_st、rs_hycode_sim（行业）、BelongHS300、RZRQ | 2_base_sector |
| sector_concept | 10 个概念 0/1（CONCEPT_MAP） | 2_base_sector |
| l1_l2_factors | 高频波动、跳空等 | 6_ml_datasets |
| index_daily | 指数行情（相对强弱分母） | 1_kline_data/index_daily |

### 7.3 因子工厂与评估

- `backend/scripts/factor_factory.py`：l1_factors 58 字段 × 算子（tsrank/tsstd/delta/roc/zscore/decay/slope，窗口 5/10/20/60）× 截面（csrank/cszscore）× 14 锚点二元组合 → IC 筛选（coverage>0.5、|corr|>0.85 去重、top200）→ 落 `6_ml_datasets/l1_factors`。
- `backend/scripts/evaluate_factors.py`：IC/ICIR/衰减/分层/去冗余。L1 门槛 |IC|>0.02 且 |ICIR|>0.3；L2 |ICIR|>0.15，net_ic = gross − turnover×6bp，top40。
- `backend/scripts/alpha_library_factors.py`：Alpha158+Alpha101+GTJA170=429 因子；输入 daily_forward+index_daily+valuation+instrument_detail（128 行业中性化）；注意 amount 万元→元、复权一致性 f=close_fwd/close_unadj。

## 8. 因子挖掘（AlphaAgent / RD-Agent）

### 8.1 启动器与 API

- 核心类 `AlphaAgentLauncher`（`backend/services/engine/alpha_agent/launcher.py`，1,359 行）双模式：**legacy alphaagent**（仅 A 股，`scripts/alpha_agent/run.py`）与 **RD-Agent 多市场**（market ∈ a_share/crypto/hong_kong/us_stock/futures，走 `rdagent` + market_adapters）。
- 流程：`start_evolution()` 建 EvolutionTask（task_id=uuid16）→ `Popen(start_new_session=True)` 拉起 `scripts/alpha_agent/run_rd_agent.py --task-id --user-id --market --universe --loop-n --direction --log-dir` → 每 3 秒扫描 RD-Agent 工作区（Loop_N/r/d 目录）推进度（hypothesis→experiment→coding→runner→feedback）。
- **LLM 凭证只在 launcher 统一注入**（`_build_subprocess_env` → `rd_agent/llm_env.py: build_llm_env()`）：优先级 DEEPSEEK_API_KEY > LITELLM_OPENAI > AI_IDE_LLM > OPENAI；用户"AI 服务配置"优先于容器 env。⚠️ adapter 只读 os.getenv 会把用户配置覆盖回容器占位符导致 401——这是源码注释明确记录的坑。
- API（`backend/services/engine/routers/alpha_agent.py`，前缀 `/api/v1/alpha-agent`）：`GET /markets`、`POST /evolve`（market/loop_n/direction/data_source=parquet|pg|qlib_bin/seed_factors≤3，含幂等键+配额检查）、`GET /tasks/{id}`（状态/日志/进度/token 用量）、`DELETE /tasks/{id}`（SIGTERM→30s→SIGKILL）、`GET /factors`、`POST /factors/{id}/backtest`（单因子回测）。
- 并发控制：`resource_budget.begin_execution` 占额度（满员排队重试）；`hw_lock.py` 硬件锁防多任务挤爆内存。

### 8.2 市场适配器（backend/services/engine/rd_agent/market_adapters/）

| | A股 | Crypto | 港股 | 美股 | 期货 |
|---|---|---|---|---|---|
| 数据源 | QuantDB parquet | Binance 公开 API | yfinance | yfinance | akshare |
| Qlib provider | `<quantdb>/.qlib_cache/cn_data` | `.qlib_cache/bc_data` | `.qlib_cache/hk_data` | `.qlib_cache/us_data` | `.qlib_cache/futures_data` |
| 年化天数 | 252 | 365 | 252 | 252 | 252 |
| 涨跌停限制 | 0.1 | 无 | 无 | 无 | 无 |
| 最低佣金 | 5 元 | 0 | 50 港币 | 0 | 0 |
| 股票池 | csi300 | 35 个合约 | 恒生+恒科+H股红筹 ~130 | SP500+NASDAQ ~530 | 国内商品+贵金属+国际 |
| 因子集 | Alpha158 兜底+L2 近似+QuantDB 列 | CRYPTO_ALPHA 90+ | HK_ALPHA | US_ALPHA | FUTURES_ALPHA 40+ |

- 注册表模式：`@register_adapter` 按 market_id 注册；`ENABLE_CRYPTO` 环境变量控制 crypto 是否注册（生产可关）。
- Qlib 路径统一解析：`backend/shared/qlib_paths.py: resolve_qlib_provider_uri(market)`（env > 市场数据目录 .qlib_cache > 项目目录回退）。
- A 股特有 `generate_base_factors_json()`：为 LLM 生成 base_factors.json 参考（Alpha158 表达式、L2 日频近似式、可直引的 QuantDB 列 `$列名`）。

### 8.3 RDLoop 封装（rd_loop_wrapper.py，1,005 行）

1. `_ensure_data_file()`：把行情准备成 RD-Agent 期望的 `daily_pv.h5`（MultiIndex [datetime, instrument]，$open/$high/$low/$close/$volume/$factor/$amount + 富化列）。A 股从 QuantDB parquet 生成，带共享缓存 `.h5_cache/daily_pv_all.h5`（mtime 判新鲜度）；富化列白名单来自 features_daily 与 l1_factors（RSI/MACD/ATR/beta/PE/PB/资金流/筹码/风格/行业强度）；代码转 Qlib 小写（600036.SH→sh600036）。同时把 Qlib bin symlink 到 `~/.qlib/qlib_data/<name>`。
2. `_patch_prompts_for_chinese()`：monkey-patch prompt 模板，追加中文输出要求与用户研究方向（direction）。
3. `run(loop_n)`：实例化 FactorRDLoop → `loop.run(step_n=loop_n*len(steps))` → `_extract_factors()` 从工作区 pickle 提取因子元信息+代码+反馈，**只保留有代码的因子**（防 loop_n 截断的半成品入库）。

### 8.4 run_rd_agent.py：IC 回填与入库

参数：`--market`（默认 a_share）、`--universe`（csi300/csi500/csi1000/sse50/gem/star/csi800/all_a）、`--loop-n`（默认 5，每轮=propose→exp_gen→coding→running→feedback）、`--direction`（研究方向注入 prompt）。

执行：跑 RDLoop → 对每个因子 `compute_factor_ic()`（临时脚本执行因子代码出 result.h5，与次日收益对齐做日度 Spearman，得 IC/RANK_IC/ICIR/RANK_ICIR；A 股窗口=QuantDB 最新交易日前推一年，120s 超时）→ `factor_id=md5(task_id:name)` upsert 进 PG **`rd_agent_factors`**（factor_code、status、ic/rank_ic、metadata）→ 写 result.json 供 launcher 收集。

### 8.5 因子生命周期落点（每一笔数据在哪）

| 阶段 | 落点 |
|---|---|
| ① 提交任务 | PG `alpha_mining_tasks` / `alpha_mining_task_specs` / `alpha_mining_request_keys` |
| ② RD-Agent 演化 | `<log_dir>/Loop_N/...` 工作区 pickle + subprocess_stdout.log |
| ③ 因子入库 | PG **`rd_agent_factors`**（+ `rd_agent_factor_revisions` 修订史、`rd_agent_factor_lifecycle_events` 事件） |
| ④ 因子池 | PG **`rd_agent_factor_pool`**（pool_score）+ `rd_agent_factor_pool_edges`（相关性边）+ factor_pool_panels/ 面板 parquet；池内因子可作下一轮 seed_factors |
| ⑤ 进入训练 | 直读 `<quantdb>/6_ml_datasets/*`（DuckDB）或 model_features_*.parquet 快照 |
| ⑥ 训练执行 | `data/training_jobs/{run_id}/` 产物 + PG `admin_training_jobs` |
| ⑦ 注册模型 | PG **`qm_user_models`**（candidate→syncing→ready/active）+ 产物拷贝到 models/users/… |
| ⑧ 回测 | 信号源=模型目录 `pred.pkl`/`pred.parquet`（SimpleSignal 适配） |
| ⑨ 推理 | 新分数合并回 pred.parquet（pred_merge.py），缺口 gap_backfill.py 逐日补 |
| ⑩ 单因子回测 | `POST /factors/{id}/backtest`：自动识别因子代码形态；H5-only 列走 H5 路径，否则走 Qlib bin；IC/夏普/回撤回写 rd_agent_factors |

> 关键理解：因子"户口"在 `rd_agent_factors`（含完整 Python 代码与 IC），"池"在 `rd_agent_factor_pool`；回测消费的不是因子代码，而是训练产出的 **pred 信号**——因子→模型→信号→回测是单向流水线。物理底座是 QuantDB parquet，向上派生 Qlib bin（`.qlib_cache/`，供回测）与 H5 面板（供因子实现/单因子回测）两种形态。

## 9. 模型训练

任务定义：**横截面未来 N 日收益排序预测**。标签=未来 N 日收益截面排名；评价看 Rank IC / ICIR（>0.2 可用，>0.5 优秀）。详见 `docs/QuantMind_模型训练完全指南.md`（与源码逐项互证）。

### 9.1 触发与编排

- 用户入口 `POST /api/v1/models/run-training`（`backend/services/api/routers/model_training.py`）→ 轮询 `GET /training-runs/{run_id}`；核心实现在 `backend/services/api/routers/admin/admin_training_utils.py: submit_training_job`。
- 提交流程：推断市场（context.market 或 benchmark 如 SH000300→CN）→ 按因子目录 `config/features/model_training_feature_catalog_v1.json` + 本地 parquet 实际列过滤特征 → `_apply_window_probe()` 自动探测数据覆盖定 train/valid/test 切分 → 写 `admin_training_jobs` 表（status=pending）→ orchestrator 拉起训练容器。
- **训练容器**（`local_docker_orchestrator.py`，镜像 `TRAINING_IMAGE`，容器名 `qm-train-{run_id}`）关键挂载：workspace=`/data/training_jobs/{run_id}`↔`/workspace`；`QUANTDB_DATA_DIR=/tmp/quantdb_data`←`$QM_QUANTDB_DATA_DIR`（QuantDB 全量 parquet，ro）；非 CN 市场挂对应 quantus/quanthk/quantbc/quantfutures 目录；`docker/training/{train.py,preprocessing.py,model_trainers/,data/}` 与 `backend/` 只读挂载。有 GPU 用 DeviceRequest，`mem_limit` 防打爆宿主机，训练前暂停其它非保护容器腾内存。
- 编排双模式：本机 docker-in-docker（挂 `/var/run/docker.sock`）或 AutoDL 远程（`TRAINING_AUTODL_*`，ssh_docker/native_python）。

### 9.2 训练数据从哪来（train.py 三分支）

1. **QuantDB 直读**（factor_source 指定 l1_factors/features_daily/l2_factors）：`QuantDBFactorReader.read_range()` DuckDB 查 `<quantdb>/6_ml_datasets/<dataset>/dt=*/data.parquet`；本地数据滞后超 `TRAINING_COVERAGE_TOLERANCE_DAYS`（默认 35 天）fail fast。
2. **非 A 股**：读 `model_features_{hk,us,crypto,futures}.parquet` 单文件快照。
3. **A 股 parquet 回退**：优先 `model_features_core.parquet`（78 列），否则逐年 `model_features_{year}.parquet`（197 列，2016 起）拼接。

**数据清洗（源码注释里的血泪教训）**：剔除 ST（is_st==0）、北交所、非股票池；**剔除泄漏列（future_return_*/return_* 绝不进特征——曾因别名映射导致标签泄漏、RankIC 虚高）**；剔除假日填充行（volume==0，不剔会破坏 T+N 标签对齐）；行业特征 ind_code_l1 从 instrument_detail 读申万一级转 Categorical。
**标签**：T+1 执行口径（`_EXECUTION_LAG_DAYS=1`，与回测、线上推理统一）；return 模式做截面百分位秩（groupby(trade_date).rank(pct=True)-0.5）。
**切分**：显式三段（train_end < valid_start < test_start，重叠即报数据泄漏）或比例切分；**train/val 尾部各裁 horizon+lag 个交易日做 embargo**；预处理=按日截面中位数填充+winsorize+Z-score。
**因子筛选**：`select_top_factors`（ic_icir/combined/auto_top，n_top 默认 80，IC 阈值 0.01、ICIR 0.15、相关性 0.9）——**只用训练段筛选**。可选 WFA 稳定性诊断、PSI 漂移检测（vs 最近 30 交易日）。

### 9.3 产物与模型注册

- 产物（`data/training_jobs/{run_id}/`）：`model.pkl`（多模型时 model_lgb/xgb/cbm/lin）、**`pred.parquet`（zstd，train/val/test 全量预测）+ `pred.pkl`（Qlib 回测兼容 MultiIndex+score）**、`metadata.json`（特征列/窗口/data_source/factor_schema_hash/metrics/target_horizon/execution_lag 等）、`inference.py`、`result.json`、`shap_summary.csv`（仅 LGB）。
- 回调 `result.json` → `complete_training_run`（X-Internal-Call-Secret 验签）→ `register_model_from_training_run()`：写 PG **`qm_user_models`**（tenant+user+model_id 联合 PK，status candidate→syncing→ready/active）→ 产物拷贝到 `models/users/{tenant}/{user}/{model_id}/`。
- **样本外软门禁**：`test_rank_icir ≥ 0.05`（且 rank_ic>0）才自动激活；不达标保持 candidate 记录 gate_reasons，可手动激活。
- 模型加载（`backend/services/engine/inference/model_loader.py`）：按 model_type 分派 lightgbm/xgboost/catboost/sklearn/pytorch；支持 EnsembleBooster（多种子平均）与 StackingEnsemble；LRU 缓存 + SHA256 校验。
- **推理服务四层**（`backend/services/engine/inference/`）：InferenceService（序列模型走 30 日滑窗）→ ModelLoader → DataAdapter（实时行情→与训练一致的特征，保证离在线一致）→ HistoryBuffer；缺口回补 gap_backfill.py 逐日执行模型目录 inference.py 合并回 pred.parquet（symbol 统一后缀式）。

### 9.4 模型选型速查（13 个，详见训练指南）

- **树 5**：LightGBM（首选基线，lr 0.02/num_leaves 31/min_child_samples 150/feature_fraction 0.7）、XGBoost（异构集成）、CatBoost（行业类别特征原生）、RandomForest（对照）、Ridge（线性诊断）。
- **DL 8**：GRU（入门）、LSTM、ALSTM（注意力/事件）、Transformer（大数据量）、TabNet（自动特征选择）、TCN（快，波动突变）、NativeTFT（自研轻量）、MLP（诊断）。
- **四阶段**：基线（LGB+Ridge+RF）→ 特征工程（多周期标签 T+5/T+10、截面预处理必开、因子筛选）→ 进阶（LGB+XGB Stacking +10-15%、CatBoost+行业、GRU）→ 验证上线（软门禁、多周期多模型融合动态权重）。
- **参数**：lr 0.01-0.03；特征 60-80/169；训练窗 3-5 年；GPU 训练、CPU 推理；市场 CN/HK/US/CRYPTO/FUTURES。

## 10. Docker 部署与运维

OSS 版**单镜像部署**：api/engine/trade/stream 全在 `quantmind` 一个容器（`SERVICE_MODE=all`）。compose 项目名 `quantmind`，网络 `quantmind-net`。

### 10.1 服务清单（docker-compose.yml，783 行）

| 服务 | 容器 | 镜像 | 端口 | 说明 |
|---|---|---|---|---|
| db | quantmind-db | postgres:15-alpine | 127.0.0.1:5432 | 业务库；pg_isready healthcheck |
| redis | quantmind-redis | redis:7-alpine | 127.0.0.1:6379 | Celery broker+缓存；**appendonly + maxmemory 512mb + noeviction（必须）** |
| quantmind | quantmind | quantmind-oss:latest（本地构建） | **8000-8003 对外** | 四服务单容器，CMD `python backend/main_oss.py` |
| celery-worker | quantmind-celery | 同上 | 无 | 消费 qlib_backtest_srv，concurrency=4 |
| celery-beat | quantmind-celery-beat | 同上 | 无 | 定时调度（资讯 enrich 60s / matcher 600s / 数据同步） |
| data-gateway | quantmind-data-gateway | quantmind-data-gateway:latest | 127.0.0.1:8004 | akshare/eastmoney 行情网关 |
| huntly | quantmind-huntly | lcomplete/huntly | 127.0.0.1:8090 | 财经资讯/RSS 聚合 |
| rsshub | quantmind-rsshub | diygod/rsshub | 127.0.0.1:1200 | RSS 源生成，缓存写 redis db3；离线包不含，需在线拉取 |
| qwenpaw | qwenpaw | agentscope/qwenpaw 扩展镜像 | 8088（`QWENPAW_BIND` 控制，默认 0.0.0.0，**免登录，务必防火墙限 IP**） | QuantBot；网络别名 copaw |

依赖：quantmind/celery-* 等 db+redis healthy；db/redis/qwenpaw 更新时不强制重启。

### 10.2 关键挂载（quantmind/celery 共用）

| 宿主 | 容器 | 说明 |
|---|---|---|
| `${QM_DATA_HOST_PATH}` | `/data` | **核心数据卷**（quantdb/quantus/quanthk/quantbc/quantfutures/quantcustom、reports、backtest_results、qlib…） |
| `./backend` | `/app/backend` | **代码 bind mount → 纯代码改动免重 build** |
| `./db` | `/app/db` | Qlib 历史数据 |
| `./config` `./scripts` `./strategy_templates` `./alphaagent` `./docker/training` | `/app/...` | 配置/脚本/模板/AlphaAgent/训练 |
| `./docs` | `/app/docs:ro` | 文档 |
| `./docker/conda-shim`、`./docker/litellm_sitecustomize.py` | 容器内 | RD-Agent conda shim、litellm+pydantic 兼容补丁 |
| `/var/run/docker.sock` | 同路径 | 容器内起训练容器（≈宿主 root 能力，注意安全） |
| qwenpaw 三 named volume + `./docker/training/fonts:ro` | qwenpaw 容器 | 数据/密钥/备份 + **PDF 中文字体** |

named volumes：postgres-data、redis-data、qwenpaw-*。

### 10.3 环境变量（.env）

- 必需：`DB_HOST/PORT/NAME/USER/PASSWORD`、`REDIS_HOST/PORT`、`SECRET_KEY`、`JWT_SECRET_KEY`（**生产必须 `openssl rand -hex 32` 换随机值**）、`STORAGE_MODE=local`。
- 数据目录：`QM_QUANTDB_DATA_DIR=/data/quantdb`、QUANTUS/QUANTHK/QUANTBC/QUANTFUTURES/QUANTCUSTOM 各自路径、`QUANTDB_STATE_DIR=/data/quantdb/.sync_state`（**固定到 bind mount，容器重建不丢同步状态**）、`QM_REPORT_ARCHIVE_DIR=/data/reports/stock_reports`。
- 行情：`REMOTE_QUOTE_REDIS_HOST=quantmindai.cn`（db3，模拟盘新鲜行情源）。
- LLM：`AI_IDE_LLM_API_KEY/BASE_URL/MODEL` 等；**embedding 相关变量故意裸透传不写默认值**——compose 禁止 `${VAR:-}` 空串注入（显式空串会屏蔽向用户"AI 服务配置"的回落）。
- 训练：`TRAINING_IMAGE`、`TRAINING_DOCKER_NETWORK`、`HOST_PROJECT_PATH`、`TRAINING_AUTODL_*`、`TORCH_DEVICE=cpu`（默认；GPU 走 `deploy/enable-gpu.sh`）。

### 10.4 Dockerfile 要点

- `docker/Dockerfile.oss`：python:3.10-slim；**torch 独立成层**（在 requirements 之前命中缓存），构建期 `TORCH_DEVICE=auto/cpu/gpu/skip` 探测；patch_qlib.py 修 qlib 0.9.7 bug；末尾打依赖指纹 Label `qm.req.sha`（供 update.sh 智能跳过 build）。
- `docker/Dockerfile.qwenpaw`：上游镜像 + **reportlab**（PDF 能力）+ 静态 docker CLI（让 QwenPaw 能 `docker exec quantmind` 跑重依赖脚本）。研报 MD→PDF 设计在 qwenpaw 容器内执行（backend bind mount + /data 共享）。

### 10.5 更新 SOP：`sudo bash deploy/update.sh`

1. pg_dump 备份 → `data/backups/`（失败不阻断）
2. git 同步代码，写 `backend/shared/version.json`（commit/rev_count，gitignore，容器内无 .git；"落后 N 提交"对比逻辑在 `backend/shared/version.py`，读 Gitea release-index）
3. **智能构建判定**：对 requirements*/Dockerfile*/compose build 段+TORCH_DEVICE 求 sha256 与 `.update/deps.sha256` 比对——没变就跳过 build（5min→30s）
4. `docker compose up -d --no-deps --force-recreate quantmind celery-worker celery-beat`
5. 数据库增量升级：`schema_migrations` 幂等版本表 + `data/upgrade_*.sql` 按 sort -V 逐个应用（主结构以 `backend/shared/db_init.sql` 幂等重放为准）
6. QwenPaw 技能同步：**只允许** `deploy/sync-qwenpaw-skills.sh`（`docker exec qwenpaw curl 127.0.0.1:8088/health` 探活 + `docker exec qwenpaw bash /app/scripts/quantbot_init.sh` + restart + `qwenpaw skills list` 验证）。**为什么必须 docker exec**：Web 一键更新的 updater 容器在 bridge 网络里，`127.0.0.1` 不是宿主，直接 curl 会打到错误目标 → "升级成功但技能未同步"。**禁止手工 rsync/docker cp 技能目录**（与 skill.json 清单漂移 → 全量 conflict 且无法经 API 删除）
7. 健康检查最长 180s（API /health + 两个 celery 容器 healthy）

首次部署用 `deploy/full-deploy.sh`（Ubuntu 22.04/24.04）：装 Docker → CDN 离线包（~6GB，SHA256 校验）→ 导镜像 → Gitea 拉代码 → 恢复 data/PG/qwenpaw 卷 → TORCH_DEVICE 判定 → 指纹闸门（代码指纹 vs 镜像 Label，一致秒级复用）→ `up -d --pull never`。默认账号 admin/admin123。

### 10.6 部署注意点（速查）

1. **纯代码 vs 重 build**：未动 requirements*/Dockerfile*/compose build 段/TORCH_DEVICE → 只需重启容器；新增 pip 依赖/升级 torch/qlib/duckdb/换 GPU/新机器 → `docker compose build quantmind`（利用 build cache）。本地 Windows 不能构建 linux/amd64，重 build 一律在服务器。
2. **数据持久化**：PG/Redis/qwenpaw 用 named volumes；业务数据在 `:/data`、`./db`、`./models`、`./logs`；update.sh 的 git clean 豁免这些目录。
3. **时区**：容器 TZ=Asia/Shanghai；DB 瞬时列 TIMESTAMPTZ+aware UTC，禁止 naive UTC 与上海墙钟混写。
4. **备份还原**：`gunzip -c xxx.sql.gz | docker exec -i quantmind-db psql -U <user> -d <db>`。
5. **安全**：docker.sock 挂载 ≈ 宿主 root；qwenpaw 免登录必须限源 IP；db/redis/网关只绑 127.0.0.1。

## 11. 实战踩坑记录（新人必读）

1. **通达信运行时周期回写 user.ini**——改配置先退出客户端；user.ini 是 GBK 编码（`[System.Text.Encoding]::GetEncoding('GBK')` 读写）。
2. **FirstGPType 决定 CJBS 供应**：=6 自动拉全市场 L2 统计，=15 只有点开过的股票有数据。"恢复出厂"曾导致当日 CJBS 全市场断供。
3. **vipdoc 锁目录假空**：TdxW 运行时 `Get-ChildItem -SilentlyContinue` 返回 0 文件，别误判数据丢失。
4. **增量扫描只看文件尾日期会漏中间交易日**——必须全量读日期列去重。
5. **内存教训**：全市场分钟历史一次性解析进内存 = GB 级。流式：先扫日期集合，逐日逐文件只留当日切片。
6. **分钟线新格式** `<IfffffI4x>`：日期 `(yy-2004)*2048+mm*100+dd`；解析出 2036 年即布局判别错误。
7. **Windows 沙箱 python 管道 EPERM**：`python x.py *> log` 失败，改前台直跑或 stdio inherit。
8. **复权数据滞后**：daily_backward/forward 依赖除权因子，可能落后于不复权日线。
9. **Celery"随机不执行"**：先查 worker 是否唯一（brpop 客户端数）、Redis 是否 noeviction。
10. **单位对账**：日线 vol（股）应等于分钟线 vol_sum（股）；快照 Volume（手）×100=股。跨表对不上先查单位与复权口径。
11. **数据目录**：`data/` 10 万+ 文件禁止递归遍历，精确路径访问；临时探针放 `temp/`。

## 12. 建议学习路径

1. 通读 AGENTS.md + 本文 §2-3 → 架构与规范
2. 跑一遍 §6.4 盘后 SOP + verify_sync → 数据链路全程
3. 读 tools/tdx_reader.py、tdx_min_to_quantdb.py → 二进制格式与流式范式
4. 读 quantdb_hub.py、update_feature_parquet.py → 湖的读取与因子计算
5. 按 docs/QuantMind_模型训练完全指南.md 跑 LightGBM 基线 → 训练中心
6. 试 AlphaAgent / RD-Agent 挖掘（§8）→ 理解因子生命周期
7. 在测试机走一遍 deploy/update.sh → 部署与运维
8. 遇到问题：先查 §11 踩坑记录，再查 docs/通达信数据同步_目录约定与踩坑.md

---

*本文基于 2026-11 实战整理：通达信分钟线新格式破解与流式重写、日K/分K/5分K 三日全量入湖验收（min1 135.5 万行/日、min5 27.1 万行/日、日线 5,639 只/日，000001.SZ 日线=分钟线互证）、CJBS FirstGPType 根因修复、l2_snapshot_collector 200 只/批重写，以及 sync/deploy/ml 四路代码调研。*
