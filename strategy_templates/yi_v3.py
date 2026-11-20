# -*- coding: utf-8 -*-
"""
壹V3 · 周线定趋势 · 日线定买点 · T+N 调出 · 止损止盈 / 大盘避险（全部 T-1 判定，无前视）

在「壹策略」骨架（周线趋势闸门 + 模型日频买点 + T+N 到期优先调出）之上新增三组纪律：

  1. 个股止损 / 止盈：持仓成本相对「上一交易日收盘价」的盈亏达到阈值即强制卖出
     （decide = T-1 收盘，deal = T 开盘，与 deal_price=open / signal_lag_days=1 一致）
  2. 止损冷却：止损卖出后 N 个交易日内不再买入该标的
  3. 大盘避险闸门：参考指数（默认沪深300）T-1 收盘跌破其 MA(market_ma) 时清仓避险，
     闸门恢复后再按模型挑票买入

成本口径：最近一次买入成交价（与平台内置止损一致）。
所有判定只用 T-1 及更早的数据；对照用 sltp_ref="same" 才读当日收盘（含前视，仅调参对照）。
"""

import numpy as np
import pandas as pd

from qlib.backtest.decision import Order, OrderDir, TradeDecisionWO
from backend.services.engine.qlib_app.utils.recording_strategy import RedisRecordingStrategy


class WeeklyTrendDailyEntryV3Strategy(RedisRecordingStrategy):
    """周线趋势闸门 + 日线模型买点 + T+N 调出 + T-1 判定止损止盈 + 大盘避险。"""

    def __init__(self, *args, **kwargs):
        self.weekly_ma = int(kwargs.pop("weekly_ma", 20))
        self.weekly_slope = int(kwargs.pop("weekly_slope", 4))
        self.hold_days = int(kwargs.pop("hold_days", 5))
        self.use_stop_loss = int(kwargs.pop("use_stop_loss", 1))
        self.use_take_profit = int(kwargs.pop("use_take_profit", 1))
        self.stop_loss = float(kwargs.pop("stop_loss", -0.01))
        self.take_profit = float(kwargs.pop("take_profit", 0.40))
        self.sl_cooldown_days = int(kwargs.pop("sl_cooldown_days", 0))
        self.sltp_ref = str(kwargs.pop("sltp_ref", "prev"))
        self.market_symbol = str(kwargs.pop("market_symbol", "") or "")
        self.market_ma = int(kwargs.pop("market_ma", 0))
        self.cb_dd = float(kwargs.pop("cb_dd", 0.0))
        self.cb_cooldown = int(kwargs.pop("cb_cooldown", 10))
        self.dd_t1 = float(kwargs.pop("dd_t1", 0.0))
        self.dd_t2 = float(kwargs.pop("dd_t2", 0.0))
        self.dd_topk1 = int(kwargs.pop("dd_topk1", 0))
        self.dd_topk2 = int(kwargs.pop("dd_topk2", 0))
        self.trail_stop = float(kwargs.pop("trail_stop", 0.0))
        self.no_sell_limitup = int(kwargs.pop("no_sell_limitup", 1))
        self.limitup_mode = str(kwargs.pop("limitup_mode", "yizi"))
        self.weekly_exit = int(kwargs.pop("weekly_exit", 0))
        self._high = {}
        self._topk_full = None
        self._entry_step = {}
        self._cost = {}
        self._cooldown_until = {}
        self._peak_value = None
        self._cb_until = -1
        super().__init__(*args, **kwargs)

    # ---------------- 周线趋势 ----------------
    def _weekly_trend_ok(self, stocks, ref_date):
        weeks_needed = self.weekly_ma + self.weekly_slope + 2
        span = int(weeks_needed * 7 * 1.6) + 40
        start = pd.Timestamp(ref_date) - pd.Timedelta(days=span)
        daily = self._close_matrix(stocks, start, ref_date)
        if daily is None or daily.empty:
            return None
        weekly = daily.resample("W-FRI").last()
        if len(weekly) < weeks_needed:
            return None
        ma = weekly.rolling(self.weekly_ma, min_periods=max(3, self.weekly_ma // 2)).mean()
        latest_close = weekly.iloc[-1]
        latest_ma = ma.iloc[-1]
        base_ma = ma.iloc[-1 - self.weekly_slope]
        return (latest_close > latest_ma) & (latest_ma > base_ma)

    # ---------------- 大盘避险 ----------------
    def _market_ok(self, ref_date):
        if not self.market_symbol or self.market_ma <= 0 or ref_date is None:
            return True
        span = int(self.market_ma * 2.5) + 30
        start = pd.Timestamp(ref_date) - pd.Timedelta(days=span)
        try:
            frame = self._close_matrix([self.market_symbol], start, ref_date)
        except Exception:
            return True
        if frame is None or frame.empty:
            return True
        series = frame.iloc[:, 0].dropna()
        if len(series) < max(3, self.market_ma // 2):
            return True
        ma = series.rolling(self.market_ma, min_periods=max(3, self.market_ma // 2)).mean().iloc[-1]
        if not np.isfinite(ma):
            return True
        return float(series.iloc[-1]) > float(ma)

    # ---------------- 步数 / 时点 ----------------
    def _trading_step(self):
        try:
            return int(self.trade_calendar.get_trade_step())
        except Exception:
            return int(getattr(self, "trade_step", 0) or 0)

    def _current_bar_time(self):
        try:
            t, _ = self.trade_calendar.get_step_time(self._trading_step())
            return pd.Timestamp(t)
        except Exception:
            return None

    def _position(self):
        return getattr(self, "trade_position", None)

    def _held(self, sid):
        pos = self._position()
        if pos is None:
            return 0.0
        try:
            return float(pos.get_stock_amount(sid))
        except Exception:
            return 0.0

    # ---------------- 成本维护 ----------------
    def _update_cost(self, execute_result, step):
        if not execute_result:
            return
        for item in execute_result:
            if not item:
                continue
            order = item[0]
            price = float(item[3]) if len(item) > 3 and item[3] is not None else None
            try:
                dealt = float(getattr(order, "deal_amount", 0) or 0)
            except Exception:
                dealt = 0.0
            if dealt <= 0:
                continue
            sid = str(order.stock_id)
            if order.direction == OrderDir.BUY:
                if sid not in self._entry_step:
                    self._entry_step[sid] = step
                if price:
                    self._cost[sid] = price
            else:
                if self._held(sid) <= 1e-6:
                    self._cost.pop(sid, None)
                    self._entry_step.pop(sid, None)
                    self._high.pop(sid, None)

    # ---------------- 止损止盈判定（T-1 收盘，无前视） ----------------
    def _forced_exits(self, step):
        if not (self.use_stop_loss or self.use_take_profit or self.trail_stop < 0) or not self._cost:
            return set()
        held = [s for s in list(self._cost.keys()) if self._held(s) > 1e-6]
        if not held:
            return set()
        ref_date = self._current_bar_time() if self.sltp_ref == "same" else self._prev_bar_time(step)
        if ref_date is None:
            return set()
        try:
            closes = self._close_matrix(held, ref_date, ref_date)
        except Exception:
            return set()
        if closes is None or closes.empty:
            return set()
        row = closes.iloc[-1]
        out = set()
        for sid in held:
            try:
                px = row.get(sid)
            except Exception:
                px = None
            cost = self._cost.get(sid)
            if px is None or cost is None or cost <= 0:
                continue
            try:
                px = float(px)
            except Exception:
                continue
            if not np.isfinite(px) or px <= 0:
                continue
            ret = (px - cost) / cost
            if self.trail_stop < 0:
                # 移动止盈/止损：从持仓期间最高收盘价回撤（只用历史已实现收盘）
                high = max(self._high.get(sid, px), px)
                self._high[sid] = high
                if high > 0 and (px / high - 1.0) <= self.trail_stop:
                    out.add(sid)
                    continue
            if self.use_stop_loss and ret <= self.stop_loss:
                out.add(sid)
            elif self.use_take_profit and ret >= self.take_profit:
                out.add(sid)

        # 周线趋势出场：持仓跌破周线趋势（周线收盘 < 周线 MA 或 MA 走平/下行）即卖出（T-1 判定）
        if self.weekly_exit:
            try:
                ok = self._weekly_trend_ok(held, self._prev_bar_time(step))
            except Exception:
                ok = None
            if ok is not None:
                for sid in held:
                    try:
                        if not bool(ok.get(sid, False)):
                            out.add(sid)
                    except Exception:
                        continue
        return out

    # ---------------- 组合级回撤熔断 ----------------
    def _portfolio_value(self, ref_date):
        """按 T-1 收盘估值的组合净值（现金 + 持仓市值），只用已实现信息。"""
        pos = self._position()
        if pos is None or ref_date is None:
            return None
        try:
            cash = float(pos.get_cash())
            stocks = [s for s in pos.get_stock_list() if self._held(s) > 1e-6]
        except Exception:
            return None
        if not stocks:
            return cash
        try:
            closes = self._close_matrix(stocks, ref_date, ref_date)
        except Exception:
            return None
        if closes is None or closes.empty:
            return None
        row = closes.iloc[-1]
        mv = 0.0
        for s in stocks:
            px = row.get(s)
            try:
                px = float(px)
            except Exception:
                continue
            if np.isfinite(px) and px > 0:
                mv += self._held(s) * px
        return cash + mv

    def _risk_off(self, step, ref_date):
        """大盘闸门 或 组合回撤熔断 触发 → 清仓避险。"""
        if not self._market_ok(ref_date):
            return True
        if self.cb_dd < 0:
            val = self._portfolio_value(ref_date)
            if val is not None and val > 0:
                self._peak_value = val if self._peak_value is None else max(self._peak_value, val)
                if self._peak_value and (val / self._peak_value - 1.0) <= self.cb_dd:
                    self._cb_until = max(self._cb_until, step + self.cb_cooldown)
        return step < self._cb_until

    def _exit_all_decision(self, protected=None):
        now = self._current_bar_time()
        pos = self._position()
        if now is None or pos is None:
            return None
        try:
            held = [s for s in pos.get_stock_list() if self._held(s) > 1e-6]
        except Exception:
            held = []
        held = [s for s in held if not (protected and s in protected)]
        if not held:
            return None
        orders = [
            Order(stock_id=sid, amount=self._held(sid), direction=OrderDir.SELL,
                  start_time=now, end_time=now)
            for sid in held
        ]
        for sid in held:
            self._cost.pop(sid, None)
            self._entry_step.pop(sid, None)
        return TradeDecisionWO(orders, self)

    def _apply_dd_throttle(self, ref_date):
        """组合回撤分级降仓：回撤越深，目标持仓数越少（全部用 T-1 已实现净值判定）。"""
        if self.dd_t1 >= 0 or ref_date is None:
            return
        if self._topk_full is None:
            self._topk_full = int(getattr(self, "topk", 5) or 5)
        val = self._portfolio_value(ref_date)
        if val is None or val <= 0:
            return
        self._peak_value = val if self._peak_value is None else max(self._peak_value, val)
        if not self._peak_value:
            return
        dd = val / self._peak_value - 1.0
        target = self._topk_full
        if self.dd_t2 < 0 and dd <= self.dd_t2 and self.dd_topk2 > 0:
            target = max(1, self.dd_topk2)
        elif dd <= self.dd_t1 and self.dd_topk1 > 0:
            target = max(1, self.dd_topk1)
        self.topk = target

    # ---------------- 一字涨停不卖（最高优先级，T-1 判定，无前视） ----------------
    @staticmethod
    def _limit_threshold(sid: str) -> float:
        s = str(sid).lower()
        code = s[2:] if s[:2] in ("sh", "sz", "bj") else s
        if code.startswith(("300", "301", "688", "689")):
            return 0.195          # 创业板 / 科创板 20cm
        if code.startswith(("8", "4", "920")):
            return 0.295          # 北交所 30cm
        return 0.095              # 主板 10cm

    def _limitup_hold_set(self, ref_date):
        """T-1 日为「一字涨停」（开=高=低=收 且涨幅达板）或「收盘封板」的持仓。

        只用 T-1 及更早数据（ref_date = 上一交易日），因此 T 日开盘撤单不构成前视。
        """
        if not self.no_sell_limitup or ref_date is None:
            return set()
        pos = self._position()
        if pos is None:
            return set()
        try:
            held = [s for s in pos.get_stock_list() if self._held(s) > 1e-6]
        except Exception:
            return set()
        if not held:
            return set()
        start = pd.Timestamp(ref_date) - pd.Timedelta(days=15)
        try:
            frame = self._price_frame(held, ["$open", "$high", "$low", "$close"], start, ref_date)
        except Exception:
            return set()
        if frame is None or frame.empty:
            return set()
        if frame.index.names and frame.index.names[0] != "datetime":
            try:
                frame = frame.swaplevel("datetime", "instrument").sort_index()
            except Exception:
                return set()
        out = set()
        for sid in held:
            try:
                sub = frame.xs(sid, level="instrument").dropna(how="any")
            except Exception:
                continue
            if len(sub) < 2:
                continue
            last = sub.iloc[-1]
            prev = sub.iloc[-2]
            try:
                o, h, l, c = (float(last["$open"]), float(last["$high"]),
                              float(last["$low"]), float(last["$close"]))
                pc = float(prev["$close"])
            except Exception:
                continue
            if pc <= 0:
                continue
            th = self._limit_threshold(sid)
            chg = c / pc - 1.0
            if self.limitup_mode == "seal":
                if chg >= th:
                    out.add(sid)
            else:  # yizi：一字板（当日无振幅 + 达板）
                if chg >= th and h <= l * 1.0005 and abs(o - c) <= l * 0.0005:
                    out.add(sid)
        return out

    def _drop_protected_sells(self, orders, protected):
        if not protected:
            return orders
        return [o for o in orders
                if not (str(o.stock_id) in protected and o.direction == OrderDir.SELL)]

    # ---------------- 主决策 ----------------
    def generate_trade_decision(self, execute_result=None):
        step = self._trading_step()
        self._update_cost(execute_result, step)

        ref_date = self._prev_bar_time(step)
        # 一字涨停不卖（最高优先级）：T-1 一字板/封板的持仓，当日撤掉一切卖单
        protected = self._limitup_hold_set(ref_date)

        # 大盘避险 / 回撤熔断：T-1 信息判定 → T 日开盘清仓，不买入（一字板持仓除外）
        if self._risk_off(step, ref_date):
            decision = self._exit_all_decision(protected)
            if decision is not None:
                return decision
            return TradeDecisionWO([], self)

        # 回撤分级降仓
        self._apply_dd_throttle(ref_date)

        decision = super().generate_trade_decision(execute_result)

        # 一字板持仓的卖单（无论来自调仓还是止损）一律撤销
        if protected:
            try:
                decision = TradeDecisionWO(
                    self._drop_protected_sells(list(decision.get_decision()), protected),
                    decision.strategy,
                )
            except Exception:
                pass

        forced = self._forced_exits(step) - protected
        if not forced:
            return decision

        now = self._current_bar_time()
        if now is None:
            return decision
        try:
            original_orders = list(decision.get_decision())
        except Exception:
            original_orders = []

        new_orders = []
        for order in original_orders:
            sid = str(order.stock_id)
            if sid in forced and order.direction == OrderDir.BUY:
                continue
            new_orders.append(order)

        for sid in forced:
            if any(str(o.stock_id) == sid and o.direction == OrderDir.SELL for o in new_orders):
                continue
            amount = self._held(sid)
            if amount > 1e-6:
                new_orders.append(
                    Order(stock_id=sid, amount=amount, direction=OrderDir.SELL,
                          start_time=now, end_time=now)
                )
                self._cost.pop(sid, None)
                self._entry_step.pop(sid, None)
                if self.sl_cooldown_days > 0:
                    self._cooldown_until[sid] = step + self.sl_cooldown_days

        return TradeDecisionWO(new_orders, decision.strategy)

    # ---------------- 信号调整 ----------------
    def _adjust_signal(self, score, ref_date):
        """1) 周线趋势闸门 2) T+hold_days 到期置底 3) 止损冷却期置底。ref_date = 上一交易日。"""
        if isinstance(score, pd.DataFrame):
            score = score.iloc[:, 0]
        if score is None or score.empty:
            return score
        score = score.copy()

        mask = self._weekly_trend_ok(list(score.index), ref_date)
        if mask is not None:
            keep = [s for s in score.index if bool(mask.get(s, False))]
            if keep:
                score = score.loc[keep]

        floor = float(score.min()) - 1.0
        if self.hold_days > 0 and self._entry_step:
            step = self._trading_step()
            aged = [
                s for s in score.index
                if s in self._entry_step and (step - self._entry_step[s]) >= self.hold_days
            ]
            if aged:
                score.loc[aged] = floor

        if self._cooldown_until:
            step = self._trading_step()
            cooling = [s for s in score.index if self._cooldown_until.get(s, -1) > step]
            if cooling:
                score.loc[cooling] = floor

        return score


STRATEGY_CONFIG = {'class': 'WeeklyTrendDailyEntryV3Strategy',
 'kwargs': {'signal': '<PRED>',
            'only_tradable': True,
            'topk': 5,
            'n_drop': 1,
            'rebalance_days': 1,
            'weekly_ma': 10,
            'weekly_slope': 3,
            'hold_days': 5,
            'use_stop_loss': 0,
            'use_take_profit': 0,
            'stop_loss': -0.03,
            'take_profit': 0.4,
            'sl_cooldown_days': 0,
            'sltp_ref': 'prev',
            'market_symbol': '',
            'market_ma': 0,
            'cb_dd': 0,
            'cb_cooldown': 10,
            'dd_t1': 0,
            'dd_t2': 0,
            'dd_topk1': 0,
            'dd_topk2': 0,
            'trail_stop': -0.08,
            'no_sell_limitup': 1,
            'limitup_mode': 'seal',
            'weekly_exit': 0,
            'f_is_st_not': 1,
            'f_total_mv_min': 300000000.0,
            'f_amount_ma_5_min': 1000}}
