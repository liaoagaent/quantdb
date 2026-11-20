# QuantMind 数据同步链路与因子体系调研笔记

> 依据：`backend/scripts/`、`backend/services/engine/data_platform/`、`backend/services/engine/qlib_data_builder.py`、`config/data_sources/field_routing.yaml`、`config/features/model_training_feature_catalog_v1.json` 源码逐文件阅读整理（未递归遍历 `data\` 数据目录）。所有结论以源码为准。

---

## 1. 全景：数据流分层

```
远程数据源                     本地 parquet（按市场分目录）            PG / Qlib / 因子数据集
─────────────                ──────────────────────────           ─────────────────────
QuantDB SDK(付费CDN)   ──►   data/quantdb/  (A股, QM_QUANTDB_DATA_DIR)  ──► PG stock_daily_latest
akshare / baostock /         data/quanthk/  (港股, QM_QUANTHK_DATA_DIR)  ──► Qlib binary 缓存
mootdx(eltdx) / Binance /    data/quantus/  (美股, QM_QUANTUS_DATA_DIR)  ──► 6_ml_datasets/l1_factors
yfinance / 东财push2 /       data/quantbc/  (区块链, QM_QUANTBC_DATA_DIR) ──► model_features_*.parquet(遗留)
新浪 / 上金所 …               data/quantfutures/(期货, QM_QUANTFUTURES_DATA_DIR)
```

- **parquet 是主存储**，Hive 分区：`1_kline_data/{daily_forward,daily_backward,daily_unadjusted,index_daily}/dt=YYYYMMDD/data.parquet`、`min1_kline`、`min5_kline/{symbol}.parquet`；`2_base_sector/*`、`3_financial_data/*/{symbol}.parquet`、`4_analyst/`、`4_options/`、`5_technical_derived/{valuation,technical_indicators,market_sentiment}/dt=*/data.parquet`、`6_ml_datasets/{features_daily,l1_factors,l1_l2_factors,alpha_library,factor_dictionary}/dt=*/data.parquet`。
- **PG `stock_daily_latest` 是服务层行情表**（撮合/展示用），由 parquet 回填。
- **Qlib binary 是派生产物**（可随时重建），路径由 `resolve_qlib_provider_uri(market)` 决定（A 股默认 `/data/qlib/cn_data`）。
- **训练/推理直读 `6_ml_datasets/l1_factors`（QuantDB 因子 reader）**；`db/feature_snapshots/model_features_*.parquet` 为遗留特征快照（A 股已默认停更，见 §3.3 Phase4）。

---

## 2. A 股主同步链路：`backend/scripts/quantdb_daily_sync.py`（1511 行）

**定位**：A 股唯一日常数据同步入口。数据流：QuantDB SDK → `data/quantdb/` parquet → PG `stock_daily_latest` → Qlib 缓存。

### 2.1 Phase 1：`sync_parquet()` —— 从 QuantDB 云端拉分区文件
- 客户端：`_make_client()` 每次 `quantdb_sdk.QuantDBClient(api_key=get_secret("QUANTDB_API_KEY"), timeout=(15,300), max_retries=3)`（密钥动态读取）。
- **V2 数据集**（`dt=YYYYMMDD` 分区，增量按 release 对账）：
  - `cat1/1_kline_data`：`daily_unadjusted`、`daily_forward`、`daily_backward`、`index_daily`
  - `cat2/2_base_sector`：`margin_trading`
  - `cat5/5_technical_derived`：`valuation`、`technical_indicators`、`market_sentiment`
  - `cat6/6_ml_datasets`：`features_daily`、`l1_factors`、`factor_dictionary`、`l1_l2_factors`
  - `FULL_REWRITE_V2_DATASETS = {daily_forward, daily_backward, daily_unadjusted, index_daily}`（无 sha256 时全量重写兜底）
- **V1 数据集**（按 symbol 单文件，全量 ETag/md5 对账）：
  - `cat2`：`sector_concept`、`instrument_detail`、`index_weights`、`trading_calendar`
  - `cat3/3_financial_data`：`balance`、`income`、`cashflow`、`capital`、`pershare_index`、`per_share`、`dividend_factors`、`holder_num`
- `_sync_v2_dataset` 流程：`GET /api/v1/data/releases?datasets=X&after_release=` 取新 release 对象（跳过 `patches/`、`releases/` 前缀与 kind=control）→ `query_manifest` 补漏（manifest 可能比 release 新）→ 跳过判定：服务端 sha256 与本地登记一致且 size 相符才跳过 → 8 线程算 sha256 校验 → `_download_stream(layout="v2", object_key=...)` 下载，`.part` 临时文件 + 原子 replace + sha256/size 校验 → 每 100 个对象 commit 一次 sqlite 状态（`~/.quantdb_state/quantdb_sync_<数据根>.sqlite`，表 `objects`/`releases`）→ 支持 `should_cancel` 协作式取消。
- `_sync_v1_dataset`：manifest 的 etag(md5) 对账，`layout=v1&symbol=...` 下载，md5 校验。
- `reseed_state`：换数据目录后用本地文件哈希重建状态库，防全量重下。
- Phase 1 还会执行 `backfill_l1_ohlcv(QUANTDB_DATA_DIR, start=today-10d)`：上游历史 `l1_factors` 分区只有因子没有 OHLCV，用 `1_kline_data/daily_backward` 的后复权 OHLCV 按分区原子回填（`backend/scripts/backfill_l1_ohlcv.py`），使训练直读能构造未来收益标签。
- Phase 1.5 `_sync_extra_sources`：北向 `quantdb_north_sync.sync(latest=True)`（2024-08-19 起北向个股改季度披露）、`akshare_north_daily.sync`（仅显式请求）、南向 `quanthk_south_sync.sync(days=5)`；开关来自 `is_source_enabled("A","hsgt_north"/"hsgt_south")`。

### 2.2 Phase 2：PG 回填 `fill_pg_from_parquet()`
- 增量起点 = PG `MAX(trade_date)` − 7 自然日，否则 `QUANTDB_EPOCH = 2016-01-04`；`batch_days=20` 按批。
- DuckDB SQL：`daily_forward k LEFT JOIN features_daily f ON symbol+dt`（`qdb_features_daily` 实为 technical_indicators + valuation 合并视图；`qdb_technical_indicators` 仅 595 天故不直接用）。
- 直取列映射 `_FEATURE_COLS`：`pe_ttm, pb, total_mv, float_mv, future_return_1d→return_1d, future_return_3d/5d/10d/20d→return_3d/5d/10d/20d, vol_std_5/20/60, rsi_14, rsi_6, macd_hist, kdj_k, beta_20, volume_ma_3→volume_ma_5, amount_ma_5, pct_change, vol_to_ma5→volume_ratio_5, vol_to_ma20→volume_ratio_20`。
- 价格派生列 `_PRICE_DERIVED_COLS`（用前复权 close 重算，因 features_daily 的同名列是后复权口径，2026-08-17 修复混用）：`ma5/ma10/ma20/ma60, ma_gap_5/10/20, vol_atr_14`（ATR 用 Wilder `ewm(alpha=1/14)`）。
- symbol 经 `StockCodeUtil.normalize` 转后缀式；`adj_factor=1.0`；psycopg2 `execute_values(page_size=5000)` upsert `ON CONFLICT(trade_date,symbol) DO UPDATE`。
- 非 full 模式走 `stock_daily_latest_refresh.refresh_stock_daily_latest(days=env PG_REFRESH_DAYS 默认30)`（滚动刷新最近 30 天，口径与 fill 完全一致，并富化 `stock_name/industry/is_st/idx_*/concept_*/roe/bp/ep_ttm/ln_mv_total/turnover_rate/listed_days`，附带清 Redis 缓存 `qm:admin:data_status`；自带「QuantDB 未推进则跳过」门控）。

### 2.3 Phase 3：Qlib 缓存 `update_qlib_cache()` → `ensure_qlib_cache(QUANTDB_DATA_DIR)`（见 §6.3）。

### 2.4 Phase 4：特征快照（默认跳过）
仅当 `QM_ENABLE_LEGACY_FEATURE_SNAPSHOT ∈ {1,true,yes}` 时执行 `generate_feature_snapshots._build_snapshot(当年)`；默认 `skipped, reason="direct QuantDB factor reader is active"`。

### 2.5 运维与 CLI
- `repair_partitions(recent_days=30)`：扫每个 V2 数据集最近 30 天分区，行数 < 近 5 日均值 60% → 删文件 + 删状态记录待重下。
- `main()` 参数：`--parquet-only --skip-parquet --skip-pg --skip-qlib --skip-snapshot --full --datasets --dry-run --status --reseed-state --repair --since YYYY-MM-DD`；结束 `record_system_event(event_type="data_sync")`（异常也落 error 事件）。

---

## 3. A 股多源兜底同步：`backend/scripts/daily_data_sync.py`（1537 行）+ `run_daily_sync.py`

### 3.1 数据源优先级
1. **QuantDB parquet**（`data/quantdb/`）
2. **baostock**（T-1）：`query_history_k_data_plus(adjustflag=2 前复权, fields=date,open,high,low,close,volume,amount,adjustflag,turn,pctChg)`
3. **akshare**（T-1）：先 `stock_zh_a_hist`（东财，qfq），失败退 `stock_zh_a_daily`（新浪，qfq）
4. **eltdx**（mootdx，仅当日，工作日）

### 3.2 `run_sync()` 流程
`_ensure_future_partitions`（自动建 `stock_daily_new_YYYY_MM` 月分区，预建 3 个月）→ 股票池 = PG `stock_daily_latest` DISTINCT symbol ∪ qlib `instruments/all.txt`（小写 sh600519 → 600519.SH）→ 逐股对比 `pg_latest=MAX(trade_date)` → Phase1 批量读 QuantDB parquet（`QuantDBDataHub.fetch_daily_kline(qfq)`，source=quantdb_parquet）→ Phase2 baostock/akshare 补至昨日、eltdx 补当日 → `_upsert_to_pg`：`INSERT INTO stock_daily_latest ... ON CONFLICT(trade_date,symbol) DO UPDATE`（核心列 trade_date,symbol,open,high,low,close,volume,amount,adj_factor）→ `ensure_qlib_cache(quantdb_dir)`。
其他：股票名称（CN=baostock `query_stock_basic`，HK/US=yfinance，crypto=USDT 前缀）；`_sync_market_stock_tables` 把 `model_features_{hk,us,crypto}.parquet` 最新行灌 PG `stock_daily_latest_{hk,us,crypto}`；`_calibrate_indicators(days=90)` 回写 ma5/10/20/60、ma_gap_5/10/20、return_1d/3d/5d/10d/20d/60d、vol_std_5/20/60、pct_change；`update_investment_data()` 从 GitHub 下载 investment_data qlib_bin（遗留兜底）；`update_feature_parquet(since,until)` 包装 §4 的特征计算并合并回写。Redis 进度键 `quantmind:sync:progress`（init/pg_query/data_sync/qlib_bin/calibrate/parquet/done）。CLI：`--market A/CN --incremental/--full --symbols --no-qlib-bin --no-calibrate --calibrate-days --update-investment-data --calibrate-only --status --start-date`。

### 3.3 `run_daily_sync.py`：5 步编排（旧入口）
`investment_data`（GitHub qlib_bin，`--quick` 跳过）→ `pg_sync`（即 `daily_data_sync.run_sync`）→ `qlib_bin`（校验 Qlib 日历与 PG 落后天数）→ `calibrate`（指标校准）→ `parquet`（A 股已跳过："A 股训练已直读 QuantDB"；HK/US/Crypto 子进程调 `update_market_features.py --market X`）。文档里的「51 维特征」为旧口径。

---

## 4. 特征/因子计算：`backend/scripts/update_feature_parquet.py`（1541 行）【重点】

### 4.1 输入数据与读取函数
- 输出目标：`db/feature_snapshots/model_features_{year}.parquet`（容器内 `/app/db/feature_snapshots`）；按年分文件，增量合并（重叠日期覆盖）。
- 数据源 = QuantDB 本地 parquet，**行情口径 = 前复权 `daily_forward`**（撮合层用 `daily_unadjusted`，有意区分）；`DEFAULT_LOOKBACK_DAYS=250` 预热窗口。
- 读取函数与原始列：
  - `_read_kline_forward` ← `1_kline_data/daily_forward/dt=*/**.parquet`：`symbol,time,open,high,low,close,volume,amount`
  - `_read_valuation` ← `5_technical_derived/valuation`：`pe_ttm,pb,total_mv,float_mv,net_profit_ttm,equity,circulating_capital,total_capital`；衍生 `ep_ttm=1/pe_ttm、bp=1/pb、ln_mv_total=log(total_mv)、roe=net_profit_ttm/equity(截断±5)`
  - `_read_technical_indicators` ← `5_technical_derived/technical_indicators`：`return_1d/5d/20d, ma5/10/20/60, rsi_6/14, kdj_k/d/j, macd_dif/dea/hist, vol_std_5/20/60, vol_atr_14, vol_to_ma5/20→volume_ratio_5/20, volume_ma_3, amount_ma_5, beta_20, pct_change`
  - `_read_instrument_detail` ← `2_base_sector/instrument_detail/instrument_{detail,list}.parquet`：`Symbol,IsSTGP→is_st, rs_hycode_sim→ind_code_l1(CatBoost编码), rs_hyname→industry, ZAF, tdx_dycode→listing_market(1,7=SH;2,8=SZ;else BJ), tdx_dyname, BelongHS300→idx_hs300, BelongRZRQ→idx_margin`；补 `idx_all=1, idx_zz1000=0, idx_chinext=0, ind_code_l2=-1, adj_factor=1.0`
  - `_read_sector_concepts` ← `2_base_sector/sector_concept/sector_members.parquet`，按 `CONCEPT_MAP` 关键词 pivot 成 0/1：人工智能/AI→concept_ai、芯片/半导体→concept_chip、新能源→concept_new_energy、光伏→concept_pv、军工/国防→concept_military、医药/医疗→concept_medical、金融科技/互金→concept_fintech、消费→concept_consumption、国企/央企→concept_state_owned、锂电/锂电池→concept_lithium
- `fetch_data_from_quantdb(since, until, lookback_days=250)`：按 symbol+trade_date 合并上述全部，静态列按 symbol 广播。**股票池 = QuantDB parquet 全市场**；**日期范围 = since..until**（默认「现有 parquet max_date+1 .. 今天」，`--rebuild` 从年初起）。

### 4.2 因子分组清单（`FEATURE_COLS`，L497-587）
**代码实况：201 项、去重后 200 个唯一因子**（`vol_atr_14` 在「波动率」与「DB 技术指标」两组各出现一次）。分组与完整因子名如下：

**① 动量（10）**：`mom_ret_1d, mom_ret_5d, mom_ret_10d, mom_ret_20d, mom_ma_gap_5, mom_ma_gap_20, mom_macd_hist, mom_rsi_14, mom_kdj_k, mom_breakout_20d`
**② 波动率（8）**：`vol_std_20, vol_atr_14, vol_parkinson_20, vol_gk_20, vol_rs_20, vol_downside_20, vol_realized_rv, vol_jump_zadj`
**③ 流动性（11）**：`liq_volume, liq_amount, liq_turnover_os, liq_volume_ma_20, liq_volume_ratio_5, liq_amount_ma_20, liq_amount_ratio_5, liq_mfi_14, liq_amihud_20, liq_amihud_60, liq_accdist_20`
**④ 资金流（6）**：`flow_net_amount, flow_net_amount_ratio, flow_large_net_amount, flow_vpin, flow_vpin_ma_5, flow_vpin_ma_20`
**⑤ 风格（6）**：`style_ln_mv_total, style_ln_mv_float, style_beta_20, style_beta_60, style_idio_vol_20, style_residual_ret_20`
**⑥ 行业（18）**：`ind_ret_1d, ind_ret_5d, ind_ret_10d, ind_ret_20d, ind_strength_20, ind_strength_60, ind_momentum_rank_20, ind_vol_20, ind_turnover_20, ind_amount_20, ind_dispersion_20, ind_up_breadth_20, ind_down_breadth_20, ind_relative_volume_20, ind_relative_volatility_20, ind_relative_flow_20, ind_value_rank, ind_size_rank`
**⑦ 新增动量（9）**：`mom_ret_3d, mom_ret_60d, mom_ret_120d, mom_ma_gap_10, mom_ma_gap_60, mom_ma_gap_120, mom_ema_gap_12, mom_ema_gap_26, mom_roc_12`
**⑧ 新增波动率（12）**：`vol_std_10, vol_atr_20, vol_true_range, vol_parkinson_10, vol_gk_10, vol_rs_10, vol_upside_20, vol_realized_rrv, vol_realized_rskew, vol_realized_rkurt, vol_jump_rjv_ratio, vol_jump_sjv_ratio`
**⑨ 新增流动性（9）**：`liq_turnover_tl, liq_volume_ma_5, liq_volume_ma_10, liq_volume_ratio_20, liq_amount_ma_5, liq_amount_ma_10, liq_amount_ratio_20, liq_obv_20, liq_obv_60`
**⑩ 新增资金流（4）**：`flow_vpin_delta_5, flow_net_order_count, flow_net_order_ratio, flow_pressure_index`
**⑪ 新增风格（4）**：`style_beta_120, style_idio_vol_60, style_bp, style_ep_ttm`
**⑫ 辅助（2）**：`factor, pctchange`
**⑬ DB 基本面（8）**：`pe_ttm, pb, roe, bp, ep_ttm, ln_mv_total, float_mv, total_mv`
**⑭ 指数成分/概念（15）**：`idx_all, idx_hs300, idx_zz1000, idx_chinext, idx_margin, concept_ai, concept_chip, concept_new_energy, concept_pv, concept_military, concept_medical, concept_fintech, concept_consumption, concept_state_owned, concept_lithium`
**⑮ DB 技术指标（31）**：`return_1d, return_5d, return_20d, ma5, ma20, ma60, rsi_14, rsi_6, kdj_k, kdj_d, kdj_j, macd_hist, macd_dif, macd_dea, ma_gap_5, ma_gap_20, vol_std_5, vol_std_60, vol_atr_14, volume_ratio_5, volume_ratio_20, volume_ma_5, volume_ma_3, amount_ma_5, turnover_rate, beta_20, mom_macd_dif, mom_macd_dea, mom_rsi_6, mom_kdj_d, mom_kdj_j`
**⑯ Alpha158 K线（9）**：`kline_kmid, kline_klen, kline_kmid2, kline_kup, kline_kup2, kline_klow, kline_klow2, kline_ksft, kline_ksft2`
**⑰ Alpha158 价格相对（4）**：`prel_open0, prel_high0, prel_low0, prel_vwap0`
**⑱ 价格位置（5）**：`price_position_20, price_position_60, dist_to_high_20, dist_to_low_20, ret_rank_20`
**⑲ 波动率调整动量（4）**：`mom_sharpe_5, mom_sharpe_20, mom_sharpe_60, mom_risk_adj_20`
**⑳ 量价配合（4）**：`pv_corr_20, pv_corr_10, up_volume_ratio_20, pv_divergence_20`
**㉑ 趋势质量（3）**：`trend_r2_20, trend_slope_20, consecutive_updown_5`
**㉒ 时序滞后（2）**：`ret_1d_lag1, ret_1d_lag2`
**㉓ 第六梯队（13）**：`vol_smile_20, vol_term_structure, vol_of_vol, tech_bollinger_position, tech_williams_r_14, tech_cci_20, alpha_decay_ret_10, alpha_corr_cv_20, alpha_tsrank_ret_20, alpha_tsrank_volume_20, alpha_high_20d_ratio, alpha_low_20d_ratio, alpha_close_open_gap`
**㉔ 基本面补充（2）**：`fund_pe_percentile, fund_pb_percentile`
**㉕ 行业编码（2，CatBoost cat_features）**：`ind_code_l1, ind_code_l2`
（另有分类列 `industry, is_st, listing_market` 保留原值、不填 NaN。）

### 4.3 每类因子依赖的原始数据列
| 因子类 | 依赖原始列 |
|---|---|
| 动量/趋势质量/价格位置/时序滞后 | close（前复权）；`mom_breakout_20d` 另需 high；均线/MACD/RSI/KDJ 全部由 close 派生 |
| 波动率 | high, low, close（Parkinson/GK/RS/ATR/TR）；`vol_realized_*`、`vol_jump_*` 来自 L2 高频派生（l1_l2_factors 口径，2026 年起有数据） |
| 流动性 | volume, amount, close（Amihud=\|ret\|/amount）、high/low（MFI 典型价）、换手率（`turnover_rate` 缺失时 fallback=vol/250 日均量） |
| 资金流 | amount×sign(Δclose)（5 日净流入）、volume（VPIN=\|买量−卖量\|20 日和/量）、大单阈值=单日额>3×20 日均额 |
| 风格 | total_mv/float_mv（ln_mv，缺失用 log(amount) 替代/0.9×ln_mv_total）、个股与市场收益（beta=ret_ma/ret_std 20/60/120）、pb/pe（bp/ep 缺失时 1/pb、1/pe） |
| 行业 | `industry`（instrument_detail.rs_hyname）+ 各股 mom_ret_1d、flow_net_amount、volume、amount、liq_turnover_os、ln_mv_total 的行业日聚合（中位数/均值/求和/截面 rank/涨跌家数/离散度） |
| Alpha158 K线/价格相对 | open, high, low, close, volume, amount（vwap=amount×10000/volume×复权因子 f，f=close_forward/close_unadjusted） |
| 量价配合 | close 收益率与 log(volume) 滚动相关、上涨日成交量占比 |
| 第六梯队 | high/low/close/volume（波动率曲面、布林、Williams %R、CCI、Alpha101/360 风格）+ pe_ttm/pb（历史分位数） |
| DB 基本面/技术指标/指数/概念/行业编码 | valuation、technical_indicators、instrument_detail（BelongHS300/BelongRZRQ、rs_hycode_sim）、sector_members 直取或简单变换 |

### 4.4 计算与输出流程
1. `main()`：读现有 `model_features_{year}.parquet` → 定 `since/until` → `fetch_data_from_quantdb` → `compute_all_features(db_df, target_dates)` → 类型感知合并（字符串列填 None、整型列 pd.NA、浮点 NaN）→ 按 `trade_date,symbol` 排序写 parquet → 写 `model_features_{year}.metadata.json`（含 `implemented_feature_count`、`feature_columns`、source=quantdb、adjust=qfq）→ 打印分组覆盖率。
2. `compute_all_features`：`_normalize_industry`（去 CSRC 前缀/罗马数字）→ 逐 symbol `compute_features_for_group`（**停牌判定 close<=0 或 volume==0，停牌行特征 NaN；有效行<2 全 NaN**；`_add_nan_features` 补齐列）→ `_compute_industry_features`（跨股票行业因子，有效行业行<100 跳过）→ `_compute_industry_codes`（rs_hycode_sim → `pd.Categorical.codes`，缺失 -1）→ 只保留 `target_dates`。
3. 关键公式：`ret=close.pct_change.clip(-1,10)`；MACD=EMA12−EMA26（DIF/DEA/HIST）；RSI 用 `ewm(alpha=1/period)`；KDJ n=9（RSV→K ewm 1/3→D→J=3K−2D）；Parkinson=sqrt(mean(ln(H/L)²)/4ln2)；GK/RS/下行波动率；RV=sqrt(mean(lr²)×252)；MFI14 典型价×量正负流；Amihud=mean(|ret|/amount)；VPIN 20 日；Alpha158 kline=(c−o)/o、(h−l)/o 等；`prel_vwap0=(amt/v)/c`（缺失用 (h+l+c)/3）；布林位置=(c−mid)/(4σ)；Williams %R=（H14−c)/(H14−L14)×−100；CCI=(TP−SMA20)/(0.015×MAD20)。

### 4.5 关于「151 维 / 115 维」的口径说明
- 文档中的「151 维」仅出现在 CLAUDE.md（两处描述 `update_feature_parquet.py`），**代码实况是 201 项 / 200 个唯一因子**（文档数字已过时）。AGENTS.md 的「48 维特征写入 market_data_daily」是另一条外部服务链路的旧口径。
- 仓库中其他真实维度口径：`alpha_library_factors.py` 产出 **429 个经典因子**（Alpha158 + Alpha101 + GTJA170，assert 校验）；QuantDB **V5 因子字典 571 列**（`quantdb_factor_v5.py` 快照，schema v5-571）；`l1_factors` 分区实测 **119 列**、`features_daily` **78 列**（hub `get_data_summary` 注释：历史硬编码 101/52 已漂移）；`sync_quantdb_data.py` 文档提到「AI 因子 315 维 L1/L2」；训练特征目录 `model_training_feature_catalog_v1.json`（v2.0）声明 `feature_count=534`、164 个可用特征，来源= daily_forward + features_daily + l1_factors + l1_l2_factors。**没有发现名为「115 维」的清单。**

---

## 5. 因子工厂与因子评估

### 5.1 `backend/scripts/factor_factory.py`（847 行）—— 表达式因子批量生成
- 定位：QuantDB 富字段 × 算子 × 窗口（+二元组合）→ 组合因子 → IC 筛选 → 训练 parquet。与 `alpha_library_factors.py`（仅用 OHLCV 生成 429 个经典因子）互补：因子工厂用 `l1_factors`/`features_daily` 的几百个数值字段组合生成上千「表达式因子」。
- 基字段 `L1_FIELDS`（l1_factors，58 个）：换手 `turn_5/20, turn_std_20, turn_z_20, turn_ratio_1_5, turn_ratio_1_20, turn_trend_5_20, turn_acc_5`；资金流 `amt_net_flow_5/20, amt_z_20, amt_ratio_1_5, amt_ratio_5_20, amt_skew_20, amt_up_ratio_20, amt_vol_ratio_20, mfi_14, obv_slope_20`；动量 `mom_ret_1d/3d/5d/10d/20d/60d, mom_ma_gap_5/20, mom_macd_hist, mom_rsi_14, mom_kdj_k`；波动率 `vol_std_5/20, vol_atr_14, vol_parkinson_20, vol_gk_20, vol_amp_20`；技术 `tech_bb_width, tech_bb_pos, tech_cci_20, tech_adx_14, tech_close_to_high_20, tech_max_drawdown_20`；基本面 `fun_float_mv, fun_total_mv, fun_mv_rank, fun_pe, fun_pb, fun_bp, fun_ep, fun_value_zscore, fun_roe, fun_peg, fun_np_growth`；筹码 `chip_profit_ratio_20/60, chip_concentration_20, chip_floating_ratio, chip_cost_90_width, chip_profit_delta_5`；风格 `style_beta_20/60, style_idio_vol_20/60, style_residual_ret_20`；行业 `ind_ret_5/20, ind_rotation_speed_20, ind_strength_20, ind_dispersion_20, ind_breadth_up_20, ind_crowding_20, ind_relative_momentum_20, ind_relative_pe`；概念 `concept_hot_score, concept_momentum_top3, concept_rotation_score, concept_crowding_max, concept_flow_rank, concept_leader_score`。
- `FEATURES_FIELDS`（features_daily，32 个）：`rsi_14, kdj_k/d/j, macd_hist, vol_atr_14, beta_20, pe_ttm, pb, ps_ttm, dividend_rate, total_mv, float_mv, net_profit_ttm, revenue_ttm` + 2026-09 新增字符串列 `hs_turnover, seal_strength, zaf, beta_now, dyna_pe, static_pe_ttm, div_yield, pb_mrq, ever_zt_count, year_zt_days, total_cap_yi, float_mv_yi, free_float_shares, ipo_price, zt_price, dt_price`（先 DESCRIBE 探 schema，缺列跳过；字符串 to_numeric）。
- 算子：窗口 `tsrank, tsstd, delta, roc, zscore, decay(DECAYLINEAR), slope(REG)`，窗口 5/10/20/60；截面 `csrank, cszscore`；二元组合 `BINARY_ANCHORS`（14 个：fun_bp, fun_ep, fun_roe, fun_np_growth, fun_peg, mom_ret_20d, mom_ret_60d, vol_std_20, turn_20, mfi_14, amt_net_flow_20, style_idio_vol_20, ind_strength_20, concept_hot_score）两两配对生成 `csdiff/csratio/tscorr`。命名 `ff_{op}{w}_{field}` / `ff_bin{op}_{a}_{b}`。
- IC 筛选：全 numpy 向量化秩相关（按日 Spearman，有效股票≥5），coverage<0.5 剔除；两遍流式（pass1 逐字段算指标、multiprocessing fork COW；候选池=top_n×3 按 |score|；pass2 重算池内→|corr|>0.85 贪心去重→保留 top_n=200）。
- 输出：`<out>/dt=YYYYMMDD/data.parquet`（symbol+date+OHLCV+因子 float32，.tmp 原子替换，zstd）+ `MANIFEST.csv`（factor_name,expression,ic,icir,coverage,kept）+ `PROPOSALS.json`；默认目录 `QM_QUANTCUSTOM_DATA_DIR(/data/quantcustom)/6_ml_datasets/l1_factors`。CLI：`--smoke --start-date --end-date --horizon --score ic|icir --top-n --min-coverage --corr-threshold --out --rebuild --dry-run --jobs`。

### 5.2 `backend/scripts/evaluate_factors.py`（1042 行）—— 因子科学筛选
- 评估维度：IC（因子值与未来收益 Spearman/Pearson）、ICIR、Rank IC、因子衰减（IC 随 1/3/5/10/20d）、相关性去冗余（|corr|>0.8）、分 5 组收益单调性（qcut，Q5−Q1 多空）。
- 分类 `FACTOR_CATEGORIES`（前缀）：momentum(mom_)、volatility(vol_std/true/atr/parkinson/gk/rs)、liquidity(liq_)、flow(flow_)、style(style_)、kline(kline_,prel_)、tech(tech_)、alpha(alpha_)、fundamental(fund_,pe_,pb_,roe,bp,ep_)、industry(ind_)、trend(trend_,consecutive_)、price_position(price_position,dist_to_,ret_rank) + **L2 微观结构 15 子类**（l2_micro_liquidity、l2_vpin、l2_informed、l2_spread、l2_realized_vol、l2_flow、l2_depth、l2_cancel、l2_order、l2_zone、l2_auction、l2_jump、l2_impact、l2_trade_micro、l2_volume_dist、l2_adverse，各带前缀映射）。
- 数据源：L1 经 `backend.shared.feature_source.read_factor_source("l1_factors","CN")` 直读分区，抽样 500 股，主看 fwd_ret_5d；L2 直读 `data/quantdb/6_ml_datasets/l1_l2_factors/dt=*/data.parquet`，抽样 2000 股，主看 fwd_ret_3d（超短线）。L2 评估向量化（截面 rank+矩阵化相关），衰减只算 top-60、分层只算 top-50，`net_ic = gross_ic − turnover×6bp`。
- 推荐门槛：L1=|IC|>0.02 且 |ICIR|>0.3 且 null_rate<0.3 且单调 且 |corr|<0.8；L2=|ICIR|>0.15 且 IC>0 占比>55% 且 net_ic>0 且 NaN<30%，IC 序列 corr<0.6 去冗余，top_n=40。报告输出 `data/quantdb/reports/`（recommended_factors.csv、l2_factor_eval_report.csv、l2_recommended_factors.csv、factor_correlation.csv）。

### 5.3 `backend/scripts/alpha_library_factors.py`（1221 行）—— 三库经典因子
Alpha158 + Alpha101 + GTJA170 = **429 个因子**（assert 校验），输入 `daily_forward` + `index_daily`（基准 000001.SH）+ `valuation` + `instrument_detail`（128 细分行业中性化）。单位契约：amount 万元→元；复权一致性修正 f=close_forward/close_unadjusted，amount/vwap/adv 统一到复权基准。无未来函数铁律（min_periods=窗口长、禁负滞后、截面只用当日、标签不写特征表、缺失传播 NaN、冒烟含截断不变性断言）。输出 `6_ml_datasets/alpha_library/dt=*/data.parquet`，支持 `--smoke --start-year --max-symbols`。

---

## 6. data_platform 数据平台（`backend/services/engine/data_platform/`）

### 6.1 `quantdb_hub.py`（1536 行）—— A 股数据读取单一入口
- 目录结构与分区视图见 §1；DuckDB 视图：`qdb_daily_forward/backward/unadjusted, qdb_index_daily, qdb_valuation, qdb_technical_indicators, qdb_market_sentiment, qdb_features_daily, qdb_margin_trading, qdb_l1_l2_factors, qdb_l1_factors, qdb_alpha_library`（hive_partitioning=1, union_by_name=true）、北向 `qdb_hsgt_north_daily`（daily_freq）与季度 `qdb_hsgt_north`。
- 数据目录解析 `_resolve_data_dir()`：env `QM_QUANTDB_DATA_DIR` → `/data/quantdb` → `/app/data/quantdb` → `D:/quant_data` → 项目根 `data/quantdb`（须存在且非空）；单例目录为空时自动重解析并按代重建线程本地 DuckDB 连接。
- 性能关键路径：`_exact_conn()` 裸连接（免 glob 视图首查 35s）；`_partition_dates()` 只枚举目录名；`partition_columns()` 读 parquet footer 列集合（供列裁剪，防 Binder Error）；`_read_partitioned()` 按精确分区路径读（实测 0.78s→0.007s）。
- 业务 API：`fetch_daily_kline(adjust=qfq|hfq|none)`、`fetch_daily_kline_batch`、`fetch_index_kline`、`fetch_minute_kline(1min/5min)`、`fetch_stock_list`、`fetch_instrument_industry`、`fetch_sector_members`、`fetch_calendar`、`fetch_index_weights`、`fetch_financial(statement_type)`、`fetch_valuation/technical_indicators/market_sentiment/features_daily/l1_factors/l1_l2_factors/alpha_factors/margin_trading`、`fetch_ml_columns(dataset,columns,...)`（列裁剪+字符串转数值）。
- 列标准化：`time→trade_date, wind_code→symbol`；volume 别名 volinstock/vol_in_stock；`_NUMERIC_STRING_COLUMNS`（features_daily 2026-09 起的字符串数值列：in_hs300/is_hsgt/is_margin/is_kcb_creatable/is_st/is_quit_risk/is_hk、total_cap_yi/float_mv_yi/free_float_shares、ipo_price/zt_price/dt_price、hs_turnover/seal_strength/zaf/ever_zt_count/year_zt_days、beta_now/dyna_pe/static_pe_ttm/div_yield/pb_mrq、sector_code/region_area_code）。
- 股票池：`UNIVERSE_MAP`（csi300/csi500/csi1000/sse50/gem/star/csi800/all_a）由 `backend.shared.stock_pool.builtins` 派生；`fetch_universe_stocks`。
- 因子类别目录：`fetch_l1_factor_categories()` 优先读 `config/features/model_training_feature_catalog_v1.json`，失败用 `_fallback_l1_categories()` 15 类硬编码（base6、momentum38、volatility58、liquidity42、moneyflow62、style19、industry32、microstructure175、gtja191 16、chip8、concept10、technical27、fundamental28、holding8、custom5）。
- `get_data_summary()/get_summary()`：分区日期范围、股票池计数、各数据集实测列数、5 大类文件数与大小、可挖掘数据集探测（rd_agent.mining_datasets）。

### 6.2 适配器体系（`data_platform/adapters/`，19 个）
- **`quantdb_local_adapter.py`（458 行）**：`QuantDBLocalAdapter`，name=`quantdb_local`，markets=["A"]，**A 股优先主数据源**（本地 parquet、无 Key、无网络）。fields：daily_kline, minute_kline, stock_list, calendar, financial_report, valuation, ai_factors, adj_factor, sector, market_sentiment, technical_indicators, margin_trading, dividend, index_kline, shareholder_count, share_change, dupont, growth, operation。内部持 `QuantDBDataHub`；symbol 经 `StockCodeUtil.to_suffix` 转后缀；`ai_factors` 的 sub_category：l1_factors / l1_l2_factors|l2_factors / features_daily；adj_factor=qfq_close/unadj_close 现算；dupont/growth/operation 从 `pershare_index` 按列前缀切分（du_*、inc_*、inventory_turnover 等）。`register()`：hub.available 为真才注册。
- **`quantdb_adapter.py`（271 行）**：`QuantDBAdapter`，name=`quantdb`，markets=["A"]，**远程 SDK 实时查询**（付费 CDN、API Key、流量配额；parquet 经 CDN 两跳 302 分发）。仅用于当日补全/特定 symbol 查询。fields：daily_kline, tick, stock_list, calendar, financial_report, valuation, ai_factors。`get_sdk_info()` 供管理台（installed/api_key/version/account/usage/connected）。
- 其他适配器（备选/兜底源）：akshare、baostock、easyquotation、efinance、eltdx(mootdx)、freestockdb、injoyai_tdx、investment_data、mootdx、openbb、qstock、simonlin_a_stock、simonlin_global、tdx_api、yahoo_finance。

### 6.3 `backend/services/engine/qlib_data_builder.py`（934 行）—— Qlib binary 构建
- Qlib 格式：`calendars/day.txt`、`instruments/all.txt`（symbol\tstart\tend）、`features/{symbol}/{field}.day.bin`（4 字节 float32 start_idx + N×float32）。`QLIB_FIELDS=open,high,low,close,volume,amount,factor`；A 股额外写 `change`（涨跌停判定用）。
- 市场前缀：CN=原生小写 `sh600036`、US=`us_AAPL`、HK=`hk_00700.HK`、CRYPTO=`bc_BTCUSDT`、FUTURES=`fut_CL.FUT`；`CN_QLIB_INDEX_SYMBOLS=[000300.SH,000905.SH,000852.SH]` 从 index_daily 单独补建。
- `QlibDataBuilder.for_market(market,...)`：CN→QuantDBDataHub（默认 `/data/qlib/cn_data`）、US/HK/CRYPTO/FUTURES→各自 Hub（/data/qlib/{us_data,hk_data,bc_data,futures_data}）。
- `build_all = build_calendar → build_instruments → build_features`。日历=hub.fetch_calendar() ∪ daily_forward 最近 14 分区（上游日历可能滞后 1-2 天），裁剪到 daily_forward 最新分区日；instruments 优先 stock_list，缺失从 daily_forward 推导，A 股补 3 指数，并为内置股票池生成 `instruments/{universe}.txt`。
- `build_features_bulk`：CN 用 `daily_backward JOIN daily_unadjusted`（raw_* 未复权价，factor=hfq/raw）；非 CN 只读 daily_forward，factor=1.0。**A 股复权因子用 `_multiplicative_factor`**：从除权事件表 `dividend_factors` 累乘 PROD(p0/ref) 得阶梯型乘法因子（hfq/raw 加法口径会逐日漂移导致整仓卖出股数偏差；事件表缺失/有效行<30/日期重复/漏行时回退上游比值并计数 factor_fallback）。价格=未复权价×乘法因子；每标的写 7 个 .day.bin + CN 的 change.day.bin；目录名强制小写。
- `ensure_qlib_cache(market="CN",...)`：qlib_dir 缺省用 `resolve_qlib_provider_uri(market)`（写回系统实际读取的那份，避免影子缓存）；未建则全量 build，已建则增量（日历→instruments→features）。

### 6.4 `config/data_sources/field_routing.yaml` + `aggregator.py`
- 路由表 v1（位于**项目根** `config/data_sources/`，非 backend/config）：每个 market(A/HK/US) × field 给出 `primary + fallbacks + consensus + cleanup + tier`（T1 离线日级/T2 分钟/T3 实时报价/T4 逐笔/T5 资金流）。
- A 股全部字段 primary=`quantdb_local`、fallbacks=[]（含日K/复权因子/分钟线/指数K线/财务/估值/分红/两融/板块/股票列表/日历/技术指标/市场情绪/AI因子/股东户数/股本变动/杜邦/成长/运营）；daily_kline/adj_factor consensus=true。
- HK：daily_kline primary=`yahoo_finance` fallbacks=[efinance, akshare, simonlin_global]；minute/realtime primary=efinance；财务/估值等 primary=yahoo_finance fallbacks=[akshare]。US：daily_kline primary=yahoo_finance fallbacks=[simonlin_global, efinance, akshare]。
- `aggregator.py`：`FieldRoutingTable` 加载 YAML（路径 env `QM_FIELD_ROUTING_PATH` → /app/config/... → /opt/quantmind/... → 项目根），按 (market, field) 取 `FieldRoute`，经 `SourceRegistry` 按 primary→fallbacks 顺序拉数，可选 DataCleaner 清洗 + 共识投票（`default_consensus_threshold=0.02`，`default_min_consensus_sources=2`），返回 `AggregationResult`（source_used、fallbacks_tried、consensus_sources、cleaning_report）。健康监控 `HealthMonitor`。

---

## 7. 各市场数据来源脚本一览（backend/scripts/）

### 7.1 A 股
| 脚本 | 数据源 | 写入 |
|---|---|---|
| `quantdb_daily_sync.py` | QuantDB SDK（付费 CDN） | data/quantdb 全部分区 + PG + Qlib（主链路，见 §2） |
| `daily_data_sync.py` | quantdb parquet → baostock → akshare → eltdx(mootdx) | PG stock_daily_latest + Qlib + 指标校准（兜底链路，见 §3） |
| `sync_quantdb_data.py` | QuantDB SDK 直连（旧版独立脚本） | PG stock_daily_latest + 日历 JSON + 特征 parquet（文档称 315 维 L1/L2 AI 因子） |
| `sync_investment_data.py` | GitHub investment_data qlib_bin | /data/third_party/investment_data（遗留兜底） |
| `em_ticks_daily.py` | 东财逐笔 push2.eastmoney.com（含 09:25 竞价+盘后固定价；限流 0.35s、熔断） | `data/quantdb/5_technical_derived/trade_count_daily.parquet`（symbol,date,num_trades,avg_trade_amt,volume,amount,src） |
| `tencent_ticks_daily.py` | 腾讯分笔 stock.gtimg.cn（缺竞价，茅台实测少 3.4%） | 同上 schema，可互相覆盖 |
| `l2_ticks_daily.py` | Wind L2 tick 快照（前置 `wind_l2_import.py` 导入 7z → 1_kline_data/tick_data/{symbol}_{date}.parquet） | 同上 schema（num_trades=末笔累计 transactionNum） |
| `minute_5m_sync.py` | 东财 push2his（每日增量，1488 根≈31 交易日）或 baostock（2020 起回补） | `1_kline_data/minute_5m/{symbol}.parquet`；派生 `5_technical_derived/minute_derived.parquet`（pm_open、vwap_all、vwap_top20 聪明钱） |
| `wind_l2_import.py` / `wind_l2_trade_counts.py` | Wind L2 7z | tick_data 分区 / 成交笔数统计 |
| `stock_daily_latest_refresh.py` | QuantDB parquet | PG stock_daily_latest 最近 30 天滚动刷新+富化 |
| `backfill_l1_ohlcv.py` | daily_backward | l1_factors 历史分区 OHLCV 回填 |
| `hsgt` 系列（quantdb_north_sync / akshare_north_daily / quanthk_south_sync） | QuantDB/akshare/爬虫 | 2_base_sector/hsgt_north（季度）、hsgt_north_daily、hsgt_south |

### 7.2 港股（`quanthk_daily_sync.py` 编排，data 目录 `data/quanthk`）
- `quanthk_akshare_kline.py`：akshare `stock_hk_daily` 日线增量回拉（不复权，daily_forward 唯一 K 线来源）→ `1_kline_data/daily_forward/dt=`
- `quanthk_south_sync.py`：南向资金独立爬虫（勾选控制）→ `2_base_sector/hsgt_south`
- `quanthk_ccass_sync.py`：CCASS 中央结算持仓（ccass_top50）
- `akshare_hk_fundamental.py`：valuation→`2_base_sector/akshare_valuation`、financial→akshare_financial、profile→akshare_profile、dividend→`3_financial_data/dividend`（东财接口，按 symbol 单文件）
- `akshare_index_sync.py`：新浪 `stock_hk_index_daily_sina`（HSI/HSCEI/HSCCI/HSTECH→HSI.HK 等）→ `1_kline_data/index_daily/dt=`
- `global_market_sync.py`（雅虎段，skip_kline=True）：估值/财务/分红/拆分/分析师/期权等 → `3_financial_data/*`、`4_analyst/`、`4_options/`、`5_technical_derived/valuation`
- K 线后 `build_ml_l1_dataset.build_l1("hong_kong")` + `build_ml_signal_datasets.refresh_signal_datasets`（南向/CCASS 因子）

### 7.3 美股（`quantus_daily_sync.py` 编排，data 目录 `data/quantus`）
- `global_market_sync.run("US")`（yfinance）：daily_forward（qfq，amount=close×volume）、valuation、sector、f10、income/balance/cashflow/dividend/splits、recommendations、upgrades_downgrades、earnings_*、analyst_price_targets、major_holders、mutual_fund_holders、calendar、insider_transactions、options_chain；429 限流指数退避
- `akshare_kline.py`（US 段）：`stock_us_daily` 不复权全历史 → daily_forward（release_id=akshare）
- `akshare_index_sync.py`（US）：新浪 `index_us_stock_sina`（.IXIC/.INX/.DJI/.NDX/.SOX→IXIC.US 等）→ index_daily
- `sync_macro_usa.py`：美国宏观 → `5_technical_derived/macro_usa/macro_usa.parquet`（update_market_features 会 left join）
- K 线后 `build_l1("us_stock")`

### 7.4 区块链（`quantbc_daily_sync.py` → `blockchain_sync.py`，data 目录 `data/quantbc`）
- Binance 公开 API（data-api.binance.vision）；标的池 top-35（BTCUSDT…BONKUSDT）；日线→daily_forward、分钟线（5m/1m）→min5_kline/min1_kline、估值快照（仅 close，市值/PE 记 None）→`5_technical_derived/valuation`；K 线后 `build_l1("crypto")`

### 7.5 期货/贵金属（`quantfutures_daily_sync.py`，data 目录 `data/quantfutures`）
- `akshare_futures_sync.py`：foreign_daily（新浪外盘 30 品种 `futures_foreign_hist`，CL/GC/SI/XAU…→{code}.FUT）、cn_daily（82 品种主力连续 `futures_main_sina`，V0/TA0/IF0…→{code}.CN）、sge_daily（上金所 `spot_hist_sge`，Au99.99/Ag(T+D)…）、foreign_realtime/cn_realtime→`2_base_sector/futures_realtime`；日 K 统一写 `1_kline_data/daily_forward/dt=`（amount=close×volume，release_id=akshare）
- `akshare_futures_extra.py`：warehouse_receipts（交易所仓单）、member_positions（会员持仓排名）、contracts_daily（分合约日K，82×15≈1200 请求，仅显式勾选）、cftc（CFTC 持仓）、fx_daily（中行牌价汇率）
- K 线后 `build_l1("futures")`

### 7.6 多市场通用
- `update_market_features.py`（707 行）：crypto/hong_kong/us_stock/futures 特征工程，复用 `update_feature_parquet.compute_features_for_group` 算纯 OHLCV 特征（剔除 A 股专属列 concept_*/idx_*/is_st/ind_code_*），输出 `db/feature_snapshots/model_features_{crypto|hk|us|futures}.parquet`；分批（100 只/批）+ pyarrow 流式合并防 OOM；美股合并宏观因子
- `build_ml_l1_dataset.py`（270 行）：非 A 股市场 L1 因子日频分区（`{market}/6_ml_datasets/l1_factors/dt=`），对齐 A 股 QuantDB 契约；增量模式向前留 190 天预热、已有分区跳过；`materialize_from_snapshot` 可从旧 model_features 快照切片回填历史
- `market_cn_names.py`：证券主表（新浪全市场快照）+ 中文名回填
- `run_daily_sync.py`：5 步旧编排（见 §3.3）
- 调度：无内置 cron；前端「同步调度」写 Redis `quantmind:sync_schedule:{market}`，未配置全部 enabled=false（见 CLAUDE.md/AGENTS.md 架构要点）

---

## 8. 维度口径速查表

| 口径 | 数值 | 出处 |
|---|---|---|
| update_feature_parquet FEATURE_COLS | **201 项 / 200 唯一**（文档写 151，已过时） | 代码 L497-587 实测 |
| alpha_library 经典因子 | 429 = Alpha158 + Alpha101 + GTJA170 | `alpha_library_factors.py` assert |
| QuantDB V5 因子字典 | 571 列（schema v5-571，2026-10-31） | `quantdb_factor_v5.py` 快照 |
| l1_factors 分区实际列数 | 119 | `quantdb_hub.get_data_summary` 注释（硬编码 101 已漂移） |
| features_daily 实际列数 | 78 | 同上（硬编码 52 已漂移） |
| sync_quantdb_data 文档口径 | 315 维 L1/L2 AI 因子 | 该脚本 docstring |
| 训练特征目录 v2.0 | feature_count=534，164 可用 | `config/features/model_training_feature_catalog_v1.json` |
| market_data_daily 外部特征 | 48 维 | AGENTS.md（外部服务链路，旧口径） |
| 「115 维」 | 未发现对应清单 | 全库检索无结果 |
