# PYTHONCONTEXT.md — signaler_lbank_v8_2.py Deep Logic Reference

Last updated: 2026-05-28. Always cross-check against FILE_MAP.md for line numbers.

---

## 1. Architecture Overview

```
main loop (FULL-5M, ~every 5min)
  └─ OHLC prefetch (7 workers, 2s/req, 55s budget, 240 candles each)
  └─ evaluate_strategy() per symbol  → stage machine + signal emission

BUY-1M loop (every 9.6s = 0.16 min)
  └─ evaluate_strategy_v2() per symbol → Pine 30m + PMA/GF+ force-buy

Both loops share per-symbol state dict (default_symbol_state())
Signal delivery via event_message_sent.json ledger (dedup)
Notifications via Bale Messenger (Telegram-compatible API)
Exchange: LBank REST (https://api.lbkex.com)
```

**Scan timing:**
- `SCAN_INTERVAL_MINUTES = 5` (L168) — FULL-5M
- `BUY_SIGNAL_SCAN_INTERVAL_MINUTES = 0.16` (L169) — BUY-1M (~9.6s)
- OHLC prefetch: 7 workers, 2s timeout, 55s budget, up to 240 plan items

---

## 2. Pine 30m System — Full Math

### 2.1 Tehran Daily Bucketing

Function: `pine_30m_build_daily_price_buckets()` (L8228)

- All OHLC candles are binned into Tehran-timezone calendar days.
- Each 30m candle contributes to its Tehran-day bucket.
- The system uses the last 4 completed days + a live (partial) day.

### 2.2 CVD Rotation

Function: `pine_30m_simulate_cvd_rotation()` (L8249)

For each candle, polarity is determined by whether close > open (bullish) or close ≤ open (bearish):
- Bullish: `buy_vol += volume`, `sell_vol += 0`
- Bearish: `buy_vol += 0`, `sell_vol += volume`

On new Tehran calendar day detected: rotate `b_values[0..3]`, `s_values[0..3]` (ring buffer).
Current day is index 0, yesterday is index 1, etc.

### 2.3 GF / ratio10 / DF per Day

Function: `pine_30m_calc_gf_ratio10_df()` (L8294)

```python
weighted_close = sum(c * v for c, v in candles)  / sum(v)
weighted_open  = sum(o * v for o, v in candles)  / sum(v)
gf       = min(weighted_close / weighted_open, 8.0)    # capped at 8.0
ratio10  = min(max_buy_vol / min_sell_vol, 8.0)         # or sell/buy; capped at 8.0
df       = ratio10 * 1.618
```

### 2.4 GF+ / GF- / Predict Upper / Lower / DF0

Function: `compute_pine_30m_system_status()` (L8449)

Using 3 most recent completed days (index 0,1,2):

```
GF_sum   = ((gf[0] + gf[1] + gf[2]) / 6) * 3.69        → GF+
GF_minus = computed from sell-side weighting (inverse)   → GF-
Delta_sum = ((ratio10[0] + ratio10[1] + ratio10[2]) / 6) * 1.618  → Predict Upper
DF0 = df[0]  (current day DF, = ratio10[0] * 1.618)
```

**PMA (Pine Moving Average):**
- `pine_30m_pma_from_series()` with `length=1`
- Takes last value from series of closed candles + live candle
- Result: single float representing the current live GF momentum

**GF+ trigger condition:**
```
live_PMA > GF+ + 0.060
```

**Critical gate — `ONLY_ALLOW_PMA_GF_PLUS_FORCE_BUYS = True` (L594):**
ALL buys (not just GF+ buys) must pass GF+ qualification. No legacy/pump path can fire unless GF+ is active.

---

## 3. Stage Machine

### 3.1 Stage Reference

| Stage | Meaning | Max Age |
|-------|---------|---------|
| `IDLE` | No signal | — |
| `watch` | Price approaching SMA stack | 8h (L306) |
| `possible_buy` | SMA stack bullish, entry criteria not yet met | 8h (L307) |
| `pending_buy` | BUY attempted, in pumpbox confirmation window | 8h (L310) |
| `buy` / `pumped` | Real active BUY (legacy path) | — (held until sell) |
| `pumpbox` | Intermediate: waiting for +0.2% hold for 72s | 8h (L310) |
| `sandbox` | Legacy 2h-confirm path (rarely used now) | — |
| `shadow` | Promoted from sandbox without full confirm | — |

Priority order: `buy/pumped > pending_buy > possible_buy > watch > IDLE`

### 3.2 BUY Styles (taxonomy)

1. **GF+ Force-Buy (pumping)** — The primary live path. Triggered when live PMA > GF+ + 0.060 AND OHLC_data passes all gates. This is what `ONLY_ALLOW_PMA_GF_PLUS_FORCE_BUYS=True` enforces. Stage becomes `pumping` / `buy` with `is_pumping_style=True`.

2. **Legacy BUY** — SMA3/SMA7/SMA18 stack + candle body-sit. Stage `buy` with `is_pumping_style=False`. Blocked by `ONLY_ALLOW_PMA_GF_PLUS_FORCE_BUYS`.

3. **Sandbox BUY** — 2-hour survival path for uncertain entries. `EXACT_TWO_HOUR_SANDBOX_CONFIRM_SCANS = 0` (disabled, L284).

4. **Shadow BUY** — Promoted from sandbox when price crosses +1.9% breakout (`SANDBOX_INSTANT_BUY_BREAKOUT_PCT = 1.0%`, L947).

5. **Pumpbox pending** — Every BUY attempt goes through pumpbox gate first (if `ENABLE_PUMPBOX_GATE=True`, L467).

### 3.3 Pumpbox Gate (L462-L492)

```
Every attempted BUY → stage = pumpbox (pending_buy)
Confirmation: price must stay ≥ +0.2% above baseline for 72 continuous seconds
  AND green_streaks >= 1
  AND closed candles consumed ≤ 2

Special: PUMPBOX_NO_BUY_2H gate (L490-492)
  If no accepted BUY in past 2h → next pumpbox triggers final GF+ confirm check
  (reads/writes pumpbox_no_buy_2h_pma_gf_gate.json)
```

---

## 4. Force-Buy Decision Tree (evaluate_strategy_v2, L29326)

```
BUY-1M fires (every ~9.6s)
  1. Fetch current price (live candle from prefetch cache)
  2. Compute Pine 30m status → GF+, GF-, DF0, PMA values
  3. Run pumping_live_pma_gf_plus_message_status() (L27838):
       a. live PMA > GF+ + 0.060?  → NO → reject (show offset below trigger)
       b. closed PMA already above GF+? → reject (already priced in, late entry)
       c. live candle is flat (PMA unchanged)? → reject, keep state warm
       d. OMA filter: OMA >= 0.16 AND OMA_delta >= 0.10 with 7 non-flat samples?
       e. live price vs SMA18 within -1.50% tolerance?
       f. scan_mode_key == "buy_signal"? → pma_gf_plus_force_buy_allowed
  4. If allowed → pumpbox gate → pending_buy/buy
```

**OMA (Offset Moving Average):**
- Length = 4 (L735)
- Threshold = 0.16 (L736)
- Requires 7 non-flat delta samples with delta >= 0.10 per sample
- `OMA_ratio >= 0.32` (32% of samples must be positive)

---

## 5. FULL-5M Strategy Evaluator (evaluate_strategy, L25102)

```
FULL-5M fires every 5 minutes
  1. Pine DF0 force-sell check first (highest priority)
  2. Per-symbol stage machine:
       IDLE: check SMA stack → watch?
       watch: check body-sit → possible_buy?
       possible_buy: check breakout + SMA7 above threshold → pending_buy/pumpbox?
       pending_buy/pumpbox: confirm pumpbox → buy?
       buy/pumped: sell evaluation (fib zone, force-sell triggers)
  3. SMA rules:
       watch: SMA7 ≥ SMA18 + 0.1%, SMA3 ≥ SMA7 - 0.1%
       possible_buy→buy: SMA7 ≥ SMA18 + 0.3%, SMA3 ≥ SMA7 - 0.35%
       price must close ≥ 2 candles above SMA7
```

---

## 6. Sell Mechanisms (all active)

### 6.1 Universal Hard Stop-Loss
```
FORCE_SELL_FROM_BUY_SIGNAL_PCT = -1.50  (L353)
Trigger: live_pnl <= -1.50% from entry
Min hold: 1 minute
Applies to: pumping AND legacy (L362-363)
Skip: GF+ force-buy positions (UNIVERSAL_ACTIVE_BUY_FORCE_SELL_SKIP_PMA_GF_PLUS_FORCE_BUY=True, L366)
Skip: GF+-qualified positions (L369)
```

### 6.2 Peak Trailing Drawdown
```
GLOBAL_ACTIVE_BUY_PEAK_DRAWDOWN_FORCE_SELL_ENABLED = True (L375)
Arms when: peak_pnl >= +1.618% (= ACTIVE_BUY_GREEN_TRIGGER_PCT, L332, L380)
Trigger: price drops -3.20% from peak within 30s of last peak scan
Applies to: pumping AND legacy
Skip: GF+ force-buy positions (L376)
```

### 6.3 Elapsed Drawdown (time-gated)
```
ACTIVE_BUY_ELAPSED_DRAWDOWN_FORCE_SELL_PCT = -0.16  (L498)
ACTIVE_BUY_ELAPSED_DRAWDOWN_FORCE_SELL_MINUTES = 2.0
Trigger: PnL < -0.16% AND position held > 2 minutes
Applies to: pumping AND legacy
```

### 6.4 GF-Minus Elapsed Sell
```
ENABLE_PMA_GF_PLUS_GF_MINUS_ELAPSED_FORCE_SELL = True (L920)
PMA_GF_PLUS_GF_MINUS_ELAPSED_FORCE_SELL_SECONDS = 120  (2 minutes)
PMA_GF_PLUS_GF_MINUS_ELAPSED_FORCE_SELL_MARGIN = 1.40
Trigger: live PMA stays below GF- * 1.40 margin for 2 continuous minutes
GF+ force-buy positions only
```

### 6.5 DF0 Cross Sell
```
ENABLE_PINE_30M_DF0_CROSS_FORCE_SELL = True (L562)
PINE_30M_DF0_FORCE_SELL_BELOW_CONFIRM_SECONDS = 720  (12 minutes)
PINE_30M_DF0_FORCE_SELL_BELOW_CONFIRM_MAJORITY_RATIO = 0.5
Trigger: PMA stays below DF0 for majority of 12-min window
Skip: GF+ lifecycle positions (L577)
Min hold before arm: 5 minutes (L579)
```

### 6.6 Fib Sell Zone
```
FIB_SELL_TRIGGER_RATIO = 0.382  (L1297)
FIB_SELL_MIN_ZONE_PCT = 1.00    (L1302)
Arms: when price peaks above entry + 1.00% (fib top established)
Zone: price retraces to fib_top - (fib_top - entry) * 0.382
Confirm timing:
  - ≥ 2 green streaks: 4 minutes elapsed in zone
  - < 2 streaks: 4 minutes elapsed
  - Below 0.236 level: carry 60s timer
BLOCK_REBUY_AFTER_FIB_SELL_SAME_LIVE_CANDLE = True (L586)
```

### 6.7 Non-Flat Sample Sell
```
ENABLE_ACTIVE_BUY_NON_FLAT_SAMPLE_FORCE_SELL = True (L508)
Window: PINE_30M_TIMEFRAME_SECONDS + 7 seconds
Trigger: if price samples are non-flat (trending) and direction is negative
```

### 6.8 No-Streak Age Exit
```
PUMPING_NO_STREAK_AGE_EXIT_MAX_MINUTES (= LEGACY_NO_STREAK_AGE_EXIT_MAX_MINUTES) = 45 min
If a position has been held 45+ minutes with 0 green streaks → force sell
LEGACY_NO_STREAK_AGE_EXIT_ENABLED = True (L451)
```

---

## 7. Fib Zone Mechanics

```
Entry price E, peak price P.
fib_top = P (locked to highest wick high if ACTIVE_BUY_FIB_TOP_LOCK_TO_HIGHEST_WICK_HIGH=True, L534)
fib_bottom = E

zone_pct = (P - E) / E * 100
Required: zone_pct >= FIB_SELL_MIN_ZONE_PCT = 1.00%

fib_sell_trigger = fib_top - (fib_top - fib_bottom) * FIB_SELL_TRIGGER_RATIO
                 = fib_top - range * 0.382    → 61.8% retracement level

fib_red_zone = fib_top - range * FIB_RED_ZONE_MAX_RATIO
             = fib_top - range * 0.236

State: sell_zone_pending_since_ts, sell_zone_scan_count, sell_zone_timer_bucket
FIB_SELL_ZONE_KEEP_TIMER_THROUGH_FIB_READJUST = True  (timer survives peak readjust)
```

---

## 8. State Machine Fields (default_symbol_state, L10625)

### Core Stage Fields
```python
stage               # current stage string
stage_ts            # timestamp when stage was set
pumping_ts          # timestamp of GF+ pumping event
pumping_mark        # bool: is this a pumping signal mark
is_pumping_style    # bool: GF+ path vs legacy path
```

### Entry Tracking
```python
active_buy_entry_price          # price at BUY confirmation
active_buy_entry_ts             # timestamp of confirmed BUY
active_buy_reference_price      # reference for force-sell calc
active_buy_peak_price           # running peak price
active_buy_peak_pnl_pct         # running peak PnL %
active_buy_peak_scan_ts         # timestamp of last peak update
```

### Fib Zone Fields
```python
sell_zone_pending_since_ts      # when fib zone was first entered
sell_zone_scan_count            # number of scans inside fib zone
sell_zone_timer_bucket          # which confirm timing bucket applies
active_buy_fib_top              # locked fib top price
active_buy_fib_bottom           # fib bottom (entry)
```

### Pine 30m GF+ Fields
```python
pma_gf_plus_qualified           # bool: this BUY is GF+ qualified
pma_gf_plus_force_buy           # bool: entered via GF+ force-buy path
pma_gf_plus_buy_ts              # timestamp of GF+ qualification
gf_plus_at_buy                  # GF+ value when BUY was entered
gf_minus_at_buy                 # GF- value when BUY was entered
df0_at_buy                      # DF0 value when BUY was entered
pma_at_buy                      # PMA value when BUY was entered
```

### BUY-1M Owned State Prefixes
Keys starting with `pma_gf_plus_buy_1m_*` are owned by the BUY-1M scan loop:
- setup tracking, history windows, OMA samples, lane/sample references

### Cooldown Fields
```python
high_peak_sell_cooldown         # bool: in cooldown after high-peak sell
high_peak_sell_cooldown_peak_pnl_pct
high_peak_sell_cooldown_threshold_pct
sell_cooldown_ts                # timestamp of last sell (2min cooldown)
```

### Pumpbox Fields
```python
pumpbox_ts                      # when pumpbox stage started
pumpbox_baseline_price          # price when pumpbox reset
pumpbox_green_streak_count      # consecutive green streaks in pumpbox
pumpbox_hold_start_ts           # start of 72s hold window
pumpbox_closed_candle_count     # candles consumed during pumpbox
```

---

## 9. Event Deduplication System

### How It Works
- File: `event_message_sent.json` (L1360)
- Mutex-protected read/write via `load_event_message_sent_state()` / `save_event_message_sent_state()`
- Key: `_event_message_sent_key(symbol_udf, stage, event_payload, candle_ts)` (L3177)
- Stages tracked: watch, possible_buy, pending_buy, buy, possible_sell, sell, mark, active_buy_alert

### Lifecycle
```
try_reserve_event_message_delivery() → atomic CAS-style reservation
  → if already reserved: skip (dedup)
  → if new: write pending, send notification, mark delivered
is_event_message_delivery_pending() → check TTL = EVENT_MESSAGE_PENDING_TTL_SECONDS
```

### Candle Identity
Stages in `EVENT_MESSAGE_CANDLE_IDENTITY_STAGES` use the candle timestamp as part of their key, so the same stage on a new candle generates a fresh signal.

---

## 10. BUY_BLOCK_SUMMARY Fields (displayed per scan)

| Field | Meaning |
|-------|---------|
| `ready` | True if BUY signal is ready to fire |
| `trigger` | Exact trigger price for GF+ entry |
| `offset` | How far (in GF units) current PMA is below trigger |
| `price_avg` | OMA-based average price used for OMA filter |
| `price_nonflat` | Fraction of price samples that are non-flat |
| `price_ratio` | Ratio of up-moves to down-moves in price history |
| `OMA` | Current OMA value (threshold: 0.16) |
| `OMA_delta` | Rate of OMA change (threshold: 0.10) |
| `OMA_nonflat` | Count of non-flat OMA delta samples (need 7) |
| `OMA_ratio` | Ratio of positive OMA samples (need 0.32) |
| `lane` | Which data lane: live_future, live_mid_lower, closed, etc. |
| `sample` | Which price sample reference is being used |
| `setup` | GF+ setup warmup state: live_mid_lower, live_future, etc. |
| `live_slot_ok` | True if current 30m slot is valid for entry |
| `flat_pma` | True if live PMA is flat/unchanged this scan |
| `reason` | Human-readable explanation of current decision |

---

## 11. OHLC Data Pipeline

```
Each FULL-5M scan:
  1. Build prefetch plan: all active symbols × timeframes
  2. 7 parallel workers fetch /v2/kline (240 candles of 30m)
     Timeout: 2s per request, 12s for first request
     Budget: 55s total
  3. Cache into OHLC_PREFETCH_LOCAL (thread-local)
  4. Failed requests: exactly 1 retry during prefetch phase
  5. After prefetch: analysis reads from in-memory cache (no blocking)

BUY-1M scan reads from OHLC_PREFETCH_LOCAL populated by most recent FULL-5M
```

---

## 12. Key Constants Quick Reference

```python
# Pine thresholds
GF_PLUS_TRIGGER_OFFSET     = 0.060       # live PMA must exceed GF+ by this
GF_CAP                     = 8.0         # max GF value
ACTIVE_BUY_GREEN_TRIGGER   = 1.618%      # peak-drawdown arm level
FIB_SELL_TRIGGER_RATIO     = 0.382       # fib retracement sell trigger
FIB_SELL_MIN_ZONE_PCT      = 1.00%       # minimum fib range to arm

# Force-sell thresholds
UNIVERSAL_STOP_LOSS        = -1.50%      # hard stop
PEAK_DRAWDOWN_DROP         = -3.20%      # from peak, after +1.618% arm
ELAPSED_DRAWDOWN           = -0.16%      # after 2 minutes
GF_MINUS_HOLD_SECONDS      = 120         # 2 min below GF- margin
DF0_CROSS_CONFIRM_SECONDS  = 720         # 12 min below DF0

# Pumpbox
PUMPBOX_CONFIRM_PCT        = +0.2%
PUMPBOX_HOLD_SECONDS       = 72
PUMPBOX_MAX_CANDLES        = 2

# SMA stack
SMA7_OVER_SMA18_MIN        = +0.3%       # possible_buy threshold
SMA7_OVER_SMA18_WATCH      = +0.1%       # watch threshold
SMA3_BELOW_SMA7_MAX        = -0.35%      # allowed SMA3 lag

# Cooldowns
SELL_COOLDOWN              = 2 minutes
HIGH_PEAK_COOLDOWN         = 2 minutes   # after ≥ +2.5% peak sell
LEGACY_BLOCK_AFTER_PUMP    = 2 minutes   (L268)

# Stage max ages
WATCH_MAX_AGE              = 8h
POSSIBLE_BUY_MAX_AGE       = 8h
PUMPBOX_MAX_AGE            = 8h
NO_STREAK_EXIT             = 45 minutes
```

---

## 13. Scan Mode Context

`scan_mode_key` is either `"full"` or `"buy_signal"`:
- `"full"` — FULL-5M evaluator, can do full stage transitions
- `"buy_signal"` — BUY-1M evaluator, only allowed to fire GF+ force-buy

`pma_gf_plus_force_buy_allowed_this_scan = (scan_mode_key == "buy_signal")` (L29326 area)

This means GF+ force-buys can ONLY fire in the BUY-1M loop, never in FULL-5M.

---

## 14. Signal Flow Summary

```
Market data (LBank /v2/kline, /v2/ticker/24hr)
  ↓ OHLC prefetch (7 workers, 55s budget)
  ↓
FULL-5M: evaluate_strategy()
  ├─ Pine DF0 force-sell (highest priority, L25102)
  ├─ SMA stack analysis → watch → possible_buy
  ├─ Pumpbox gate confirmation
  ├─ Fib sell zone management
  └─ Sell trigger evaluation (all 7 mechanisms)

BUY-1M: evaluate_strategy_v2()
  ├─ Compute Pine 30m (GF+, GF-, DF0, PMA)
  ├─ pumping_live_pma_gf_plus_message_status()
  │    → OMA filter, SMA18 guard, flat-PMA guard, entry gate
  └─ If allowed: pumpbox → pending_buy → buy (GF+ qualified)

Signal delivery:
  └─ event_message_sent.json dedup
  └─ Bale Messenger notification
  └─ strategy_state.json (per-symbol state persistence)
```
