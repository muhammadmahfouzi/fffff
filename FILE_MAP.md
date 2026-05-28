# signaler_lbank_v8_2.py — Complete File Map

**37,560 lines | Python 3.11 | LBank USDT Cryptocurrency Trading Bot**  
**Strategy:** PMA/GF+ Pine force-buy | Multi-timeframe (1m BUY scans + 5m FULL scans)  
**Notifications:** Bale Messenger (Telegram-compatible) | Local web UI on port 8765

---

## TABLE OF CONTENTS
1. [Version History](#version-history)
2. [Imports & Thread-Local Guards](#section-1--imports--thread-local-guards-lines-163)
3. [.env Loader & Helpers](#section-2--env-loader--helpers-lines-65122)
4. [Exchange & API Config](#section-3--exchange--api-config-lines-125197)
5. [Scan & Timing Constants](#section-4--scan--timing-constants-lines-165303)
6. [Pump Detection Constants](#section-5--pump-detection-constants-lines-200243)
7. [Buy/Sell Strategy Thresholds](#section-6--buysell-strategy-thresholds-lines-219415)
8. [Pumpbox Gate](#section-7--pumpbox-gate-lines-462492)
9. [Active-BUY Safety Exits](#section-8--active-buy-safety-exits-lines-494538)
10. [Pine 30m Daily System](#section-9--pine-30m-daily-system-lines-540588)
11. [PMA/GF+ Force-Buy Engine](#section-10--pmagf-force-buy-engine-lines-589952)
12. [Bale / Notification Config](#section-11--bale--notification-config-lines-13281457)
13. [Global State / Locks / Queues](#section-12--global-state--locks--queues-lines-17851810)
14. [Web & Scheduler Config](#section-13--web--scheduler-config-lines-16881703)
15. [ANSI Colours](#section-14--ansi-colours-lines-17051714)
16. [Function Groups](#section-15--key-function-groups)
17. [Classes](#section-16--classes)
18. [Threading Architecture](#section-17--threading-architecture)
19. [Strategy Signal Flow](#section-18--strategy-signal-flow)
20. [Required .env Variables](#section-19--required-env-variables)
21. [Runtime Files](#section-20--runtime-files-created)

---

## Version History

```
v8-safety  fix last-price stability guard (quality_* aliases); keep +8.00% upper limit + same-candle guard
v6-safety  block repeated PMA/GF+ BUYs on same reference/live candle; add last-price stability guard
v5-lbank   fix LBank REST kline/current-candle adapter; synthesize live current-slot candle
v102       stop BUY retry duplicates; BUY attempts count as open
v101       fix duplicate BUY/rebuy: pending reservations count as open; prefer real active entry ts
v99        global active-BUY non-flat sample liquidity force-sell (PMA/GF+ included)
v97        disable PMA-line pumping marks; enable daily-PnL-only pumping marker at >3%
v9         LBank direct HTTPS; proxy/CA/TLS overrides disabled; synced PMA/GF+ force-buy logic
v58        cap Pine GF values at PINE_30M_RATIO_CAP=8.0; flat candles included
v57        PMA/GF+ DF0 sell: 5m maturity + 3m below-DF0 confirmation
v55        restore OMA positive-ratio gate; v54 midpoint OMA and price/split-average
v48        PMA/GF+ force-buy instant offset +0.060; post-close PMA buy grace 120s
v43        isolate BUY-1M PMA/GF+ setup/history/lifecycle from FULL-5M scans
```

---

## SECTION 1 — Imports & Thread-Local Guards (lines 1–63)

```python
# Standard library
json, copy, math, random, statistics, time, os, re, threading, traceback
queue, sys, socket, ssl, http.client, concurrent.futures
BaseHTTPRequestHandler, ThreadingHTTPServer
datetime, timedelta, timezone
ZoneInfo (optional, Python 3.9+)
urllib.request, urllib.parse, urllib.error

# Thread-local: controls whether BUY-1M or FULL-5M scan can mutate PMA/GF+ state
PMA_GF_PLUS_SKIP_MUTATION_CONTEXT = threading.local()   # L43
set_pma_gf_plus_skip_mutation_allowed(allowed)           # L45
pma_gf_plus_skip_mutation_allowed() -> bool              # L51
# Rule: BUY-1M owns PMA/GF+ lifecycle mutations; FULL-5M is read-only
```

---

## SECTION 2 — .env Loader & Helpers (lines 65–122)

```python
load_env_file(filename=".env")   # L65  shell env vars win over .env
_env_float(name, default)        # L97
_env_int(name, default)          # L108
_env_text(name, default="")      # L118
```

---

## SECTION 3 — Exchange & API Config (lines 125–197)

```python
BASE_URL      = "https://api.lbkex.com"                 # L125
EXCHANGE_NAME = "LBank"                                  # L126
LBANK_USER_AGENT = os.getenv("LBANK_USER_AGENT",
                   "TraderBot/LBankMARadar")             # L127
NOBITEX_USER_AGENT = LBANK_USER_AGENT                   # L129  backward compat alias

LBANK_KLINE_TYPE_BY_RESOLUTION = {                      # L130
    "1": "minute1", "5": "minute5", "15": "minute15",
    "30": "minute30", "60": "hour1", "240": "hour4",
    "480": "hour8", "720": "hour12",
    "D"/"1D": "day1", "W"/"1W": "week1", "M"/"1M": "month1"
}
LBANK_KLINE_INTERVAL_SECONDS = { ... }                  # L146  resolution → seconds

OHLC_FETCH_COUNT = 240          # L185  30m candles fetched per request (covers 5 days)
OHLC_PREFETCH_MAX_PLAN_ITEMS = 240                       # L181
OHLC_PREFETCH_STRICT = True                              # L182
OHLC_PREFETCH_FAILED_RETRY_COUNT = 1                     # L184
ENABLE_CLOSED_30M_OHLC_CACHE = True                      # L190  V9: cache closed bars only
CLOSED_30M_LIVE_FETCH_COUNTBACK = 4                      # L191  live bar refresh count
CACHE_4H = {}                                            # L207  no persistent 4h cache
CACHE_30M_CLOSED = {}                                    # L209  V9 closed-30m cache

# Runtime files
LOG_FILE                  = "log.txt"                    # L192
BUY_SIGNAL_LOG_FILE       = "log_1m.txt"                 # L193
STATE_FILE                = "strategy_state.json"        # L194
LOCAL_CHAT_STORE_FILE     = "strategy_local_chat_store.json"  # L195
TODAY_LEDGER_FILE         = "pump_bale_today_sent.json"  # L196
MONTHLY_REALIZED_PNL_FILE = "monthly_realized_pnl.json"  # L197
```

---

## SECTION 4 — Scan & Timing Constants (lines 165–303)

```python
MIN_DAYCHANGE_PCT = 10.0          # L165  24h gain floor for scan inclusion
MAX_DAYCHANGE_PCT = 280.0         # L166  24h gain ceiling (anti-rug filter)
MAX_CANDIDATES = 500              # L167
SCAN_INTERVAL_MINUTES = 5         # L168  FULL scan cadence
BUY_SIGNAL_SCAN_INTERVAL_MINUTES = 0.16  # L169  ~9.6s BUY-1M cadence
FULL_SCAN_TRIGGER_WINDOW_SECONDS = 70    # L170
BUY_SIGNAL_SCAN_PREEMPT_SECONDS = 0     # L171  disabled
FULL_SCAN_PER_COIN_ANALYZE_DELAY_SECONDS = 0.001   # L172
BUY_SIGNAL_PER_COIN_ANALYZE_DELAY_SECONDS = 0.001  # L173

# HTTP (env-overridable)
HTTP_TIMEOUT = 25.0               # L174  env: HTTP_TIMEOUT
RETRY_COUNT = 2                   # L175  env: RETRY_COUNT

# OHLC prefetch (env-overridable)
OHLC_PREFETCH_BUDGET_SECONDS = 55.0       # L176  env: OHLC_PREFETCH_BUDGET_SECONDS
OHLC_PREFETCH_WORKERS = 7                 # L177  env: OHLC_PREFETCH_WORKERS
OHLC_PREFETCH_REQUEST_TIMEOUT = 2.0       # L178  env: OHLC_PREFETCH_REQUEST_TIMEOUT
OHLC_PREFETCH_FIRST_REQUEST_TIMEOUT = 12.0 # L179
OHLC_PREFETCH_REQUEST_GAP_SECONDS = 0.02  # L180

# Log config
LOG_RETENTION_HOURS = 1           # L295  log.txt trimmed hourly
LOG_KEEP_LAST_SCANS = None        # L296
DISABLE_SCAN_LOGS = False         # L297  True = suppress BUY-1M file logs only
DISABLE_DEBUG_OUTPUT = True       # L302  suppress per-symbol verbose logs
BUY_SIGNAL_SUMMARY_INTERVAL_MINUTES = 30 # L303

# Stage freshness windows
WATCH_STAGE_MAX_AGE_HOURS = 8          # L306
POSSIBLE_BUY_STAGE_MAX_AGE_HOURS = 8   # L307
BUY_STAGE_MAX_AGE_HOURS = 24           # L308
PUMP_STAGE_MAX_AGE_HOURS = 24          # L309
PUMPBOX_STAGE_MAX_AGE_HOURS = 8        # L310
POSSIBLE_SELL_EVENT_MAX_AGE_HOURS = 24  # L311
SELL_EVENT_MAX_AGE_HOURS = 24           # L312
```

---

## SECTION 5 — Pump Detection Constants (lines 200–243)

```python
PUMP_MIN_GAP_PCT = 0.3                  # L200
TREND_24H_MAX_DROP_PCT = -3.0           # L201
OHLC_4H_FETCH_COUNT = 40               # L202
CANDLE_30M_PREV_FOR_AVG = 6            # L203
CANDLE_4H_FETCH = 4                    # L204

PUMP_MIN_30M_LAST_VS_AVG_PCT = 1.2     # L212  30m move vs avg of prior 30m moves
PUMP_MIN_4H_PCT = 1.2                  # L213  closed 4h vs previous 4h
PUMP_ALT_30M_PCT = 2.4                 # L214  fallback if 4h is 0%
MIN_Z_SCORE = 1.618                    # L215  30m move z-score floor
RECENT_PUMP_DIFF_PCT = 2.4             # L216  recent-pump marker threshold
PUMP_FROM_POSSIBLE_BUY_PCT = 2.4       # L217  BUY-signal-price pump promotion

LOOKBACK_4H_CANDLES = 8               # L220  = 4h of 30m candles
RULE2_MIN_GAP_CANDLES = 3             # L221  after watch candle
RULE3_MIN_GAP_CANDLES = 2             # L222  after possible-buy candle
```

---

## SECTION 6 — Buy/Sell Strategy Thresholds (lines 219–415)

```python
POSSIBLE_BUY_TO_BUY_COOLDOWN_MINUTES = 2    # L223
POST_BUY_MIN_WAIT_CANDLES = 2               # L224
RULE2_MIN_7_OVER_18_PCT = 0.3              # L225  SMA7 must be +0.3% over SMA18
MA_TOUCH_TOLERANCE_PCT = 0.35              # L226  kept for compat; body-bottom checks used
BODY_SIT_TOLERANCE_PCT = 1.0               # L230  body-bottom MA sit threshold
BODY_SIT_TOLERANCE_BOTH_SIDES = False      # L231  only above MA counts
WATCH_BYPASS_WINDOW_HOURS = 3              # L236
WATCH_BYPASS_WINDOW_CANDLES = 6            # L237  = 3h / 30m
POSSIBLE_BUY_RECOVERY_WINDOW_HOURS = 4    # L238
STRATEGY_MIN_CLOSED_CANDLES = 18          # L239  minimum data to run strategy
BYPASS_PUMP_WAIT_CANDLES = 1              # L240
WATCH_RECENT_PUMP_LOOKBACK_HOURS = 24     # L241
WATCH_RECENT_PUMP_WINDOW_HOURS = 14       # L242
WATCH_RECENT_PUMP_THRESHOLD_PCT = 109     # L243

SELL_COOLDOWN_HOURS = 2/60               # L244  = 2 minutes
HIGH_PEAK_SELL_COOLDOWN_ENABLED = True   # L249
HIGH_PEAK_SELL_COOLDOWN_TRIGGER_PCT = 2.5  # L250
HIGH_PEAK_SELL_COOLDOWN_HOURS = 2/60     # L251

ACTIVE_BUY_PEAK_SCALE_REPAIR_ENABLED = True        # L260  repair legacy decimal scales
ACTIVE_BUY_PEAK_SCALE_REPAIR_FACTORS = (1.0, 0.1, 0.01, 0.001, 0.0001)  # L261

ENTRY_RED_CANDLE_SKIP_RATIO = 0.16      # L272
ENTRY_CLOSED_FLAT_CHANGE_PCT = 0.01     # L273
ENTRY_SKIP_HOURS = 2/60                 # L274  = 2 minutes
POSSIBLE_BUY_REISSUE_MAX_COUNT = 4      # L275

BUY_WINDOW_EXACT_HOURS = 2             # L276
BUY_WINDOW_EXACT_CANDLES = 4           # L277  = 2h / 30m
BUY_WINDOW_MIN_GREEN_COUNT = 3         # L278
BUY_WINDOW_MIN_NOT_FLAT_COUNT = 3      # L279
ENABLE_EXACT_TWO_HOUR_BUY_RULE = True  # L283

RECENT_SMA18_GUARD_LOOKBACK_HOURS = 2  # L289
RECENT_SMA18_GUARD_MIN_PCT = -1.618    # L290
MARKET_BREADTH_MIN_24H_PCT = 5.0       # L292  min coins at +5% for confirmation
MARKET_BREADTH_MIN_UNIQUE_COINS = 2    # L293
ENTRY_MIN_24H_CHANGE_PCT = 3.0         # L294

# Active-BUY alert thresholds
ACTIVE_BUY_GREEN_TRIGGER_PCT = 1.618   # L332  first green streak threshold
ACTIVE_BUY_GREEN_STEP_PCT = 1.618      # L333
ACTIVE_BUY_ALERT_MIN_PNL_CHANGE_PCT = 1.0  # L338  min PnL move to resend alert
ACTIVE_BUY_RED_BREAK_MIN_STREAK = 2    # L343
ACTIVE_BUY_OPEN_GREEN_LOOKBACK_HOURS = 3  # L344
ACTIVE_BUY_OPEN_GREEN_MIN_STREAK = 3   # L346

# Sell zone
ACTIVE_BUY_RANGE_SELL_UPPER_PCT = 1.618   # L349  normal take-profit
ACTIVE_BUY_RANGE_SELL_LOWER_PCT = -1.49   # L350  range lower exit
ACTIVE_BUY_RANGE_SELL_AFTER_CANDLES = 1   # L351
ACTIVE_BUY_HARD_STOP_PCT = None           # L352  disabled
FORCE_SELL_FROM_BUY_SIGNAL_PCT = -1.50    # L353  universal hard stop

# Universal force-sell
UNIVERSAL_ACTIVE_BUY_FORCE_SELL_ENABLED = True      # L359
UNIVERSAL_ACTIVE_BUY_FORCE_SELL_PCT = -1.50         # L360
UNIVERSAL_ACTIVE_BUY_FORCE_SELL_MIN_HOLD_MINUTES = 1 # L361
UNIVERSAL_ACTIVE_BUY_FORCE_SELL_SKIP_PMA_GF_PLUS_FORCE_BUY = True   # L366
UNIVERSAL_ACTIVE_BUY_FORCE_SELL_SKIP_PMA_GF_PLUS_QUALIFIED = True   # L369

# Peak drawdown trailing stop
GLOBAL_ACTIVE_BUY_PEAK_DRAWDOWN_FORCE_SELL_ENABLED = True  # L375
GLOBAL_ACTIVE_BUY_PEAK_DRAWDOWN_FORCE_SELL_ARM_PCT = 1.618 # L380
GLOBAL_ACTIVE_BUY_PEAK_DRAWDOWN_FORCE_SELL_DROP_PCT = 3.20 # L381
GLOBAL_ACTIVE_BUY_PEAK_DRAWDOWN_FORCE_SELL_SECONDS = 30    # L382

# Profit pullback guard
ENABLE_ACTIVE_BUY_PROFIT_PULLBACK_BEFORE_SECOND_STREAK_SELL = True  # L391
ACTIVE_BUY_PROFIT_PULLBACK_ARM_PCT = 2.01           # L392
ACTIVE_BUY_PROFIT_PULLBACK_DISABLE_AFTER_PCT = 2.00  # L396
ACTIVE_BUY_PROFIT_PULLBACK_WINDOW_MINUTES = 7        # L398

# Progress & streak timeouts
ACTIVE_BUY_MIN_PROGRESS_EXIT_ENABLED = True           # L415
ACTIVE_BUY_MIN_PROGRESS_EXIT_MINUTES = 10             # L416
ACTIVE_BUY_MIN_PROGRESS_EXIT_PCT = 0.90               # L417
ACTIVE_BUY_SECOND_STREAK_TIMEOUT_ENABLED = True        # L421
ACTIVE_BUY_SECOND_STREAK_TIMEOUT_MINUTES = 40          # L422

# Fib sell zone
FIB_SELL_ZONE_FAST_CONFIRM_STREAK_COUNT = 2            # L432
FIB_SELL_ZONE_CONFIRM_MINUTES_AT_TWO_STREAKS = 4.0    # L434
FIB_SELL_ZONE_BELOW_0382_CARRY_ELAPSED_SECONDS = 60   # L438

# Age-based no-streak exits
PUMPING_NO_STREAK_AGE_EXIT_ENABLED = True              # L445
PUMPING_NO_STREAK_AGE_EXIT_MAX_MINUTES = 40            # L446
PUMPING_NO_STREAK_AGE_EXIT_MIN_GREEN_STREAKS = 2       # L447
LEGACY_NO_STREAK_AGE_EXIT_ENABLED = True               # L451

# Breakout thresholds
BUY_BREAKOUT_STEP_PCT = 1.0                           # L460
BUY_BREAKOUT_TOTAL_PCT = 1.0                          # L461

# Market breadth confirmation boost
MARKET_BREADTH_CONFIRM_BOOST_MIN_24H_PCT = 200.0      # L1005
MARKET_BREADTH_CONFIRM_BOOST_MIN_UNIQUE_COINS = 1     # L1006
MARKET_BREADTH_CONFIRM_BOOST_PENDING_BUY_CONFIRM_SCANS = 2  # L1007
MARKET_BREADTH_CONFIRM_BOOST_BUY_CONFIRM_SCANS = 5   # L1008
PENDING_BUY_CONFIRM_SCANS = 1                          # L997
BUY_CONFIRM_SCANS = 1                                  # L998
```

---

## SECTION 7 — Pumpbox Gate (lines 462–492)

```python
ENABLE_PUMPBOX_GATE = True              # L467
# Every attempted BUY first becomes a pumpbox flag (not a real BUY).
# Real BUY allowed only after: body-top holds +0.2% from baseline for 72s.
PUMPBOX_CONFIRM_PCT = 0.2               # L471  confirmation threshold
PUMPBOX_CONFIRM_HOLD_SECONDS = 72       # L472  must hold continuously for 72s
PUMPBOX_REQUIRED_GREEN_STREAKS = 1      # L473
PUMPBOX_MAX_CLOSED_CANDLES = 2          # L474  auto-expires after 2 closed bars
ENABLE_PUMPBOX_START_LIVE_SMA_STACK_GUARD = False   # L480  disabled
ENABLE_PUMPBOX_CONFIRM_LIVE_SMA_STACK_GUARD = False # L481
ENABLE_PENDING_BUY_LIVE_SMA_STACK_GUARD = False     # L482

# 2-hour drought confirmer: if no BUY in 2h, next pumpbox must pass Pine PMA > GF+
ENABLE_PUMPBOX_NO_BUY_2H_PMA_GF_PLUS_FINAL_CONFIRM = True    # L490
PUMPBOX_NO_BUY_2H_FINAL_CONFIRM_WINDOW_SECONDS = 7200         # L491
PUMPBOX_NO_BUY_2H_FINAL_CONFIRM_FILE = "pumpbox_no_buy_2h_pma_gf_gate.json"  # L492
```

---

## SECTION 8 — Active-BUY Safety Exits (lines 494–538)

```python
# Elapsed drawdown: sell if PnL stays <= -0.16% for 2+ real minutes
ACTIVE_BUY_ELAPSED_DRAWDOWN_FORCE_SELL_ENABLED = True      # L497
ACTIVE_BUY_ELAPSED_DRAWDOWN_FORCE_SELL_PCT = -0.16         # L498
ACTIVE_BUY_ELAPSED_DRAWDOWN_FORCE_SELL_MINUTES = 2.0       # L499

# Liquidity/non-flat sample force-sell (V99)
# Within first 10 min of any active BUY, must have 4+ non-flat price samples
ENABLE_ACTIVE_BUY_NON_FLAT_SAMPLE_FORCE_SELL = True        # L508
ACTIVE_BUY_NON_FLAT_SAMPLE_WINDOW_MINUTES = 10.0           # L509
ACTIVE_BUY_NON_FLAT_SAMPLE_MIN_COUNT = 4                   # L510
ACTIVE_BUY_NON_FLAT_SAMPLE_MIN_PNL_DELTA_PCT = 0.001       # L511
ACTIVE_BUY_NON_FLAT_SAMPLE_ROLLING_WINDOWS = True          # L515  every 6m window

# Fib sell zone robustness (V4/V5)
FIB_SELL_ZONE_KEEP_TIMER_THROUGH_FIB_READJUST = True       # L525
ACTIVE_BUY_PEAK_TRACK_LIVE_WICK_HIGH_AFTER_BUY_SCAN = True # L533
ACTIVE_BUY_FIB_TOP_LOCK_TO_HIGHEST_WICK_HIGH = True        # L534
REPAIR_ACTIVE_BUY_TO_IDLE_WITH_MISSING_SELL = True         # L538

# Same-candle rebuy blocks
BLOCK_REBUY_AFTER_FIB_SELL_SAME_LIVE_CANDLE = True         # L586
BLOCK_BUY_AFTER_CLOSED_PMA_DF0_DOWNCROSS_UNTIL_PREDICT_UPPER = True  # L587
```

---

## SECTION 9 — Pine 30m Daily System (lines 540–587)

```python
ENABLE_PINE_30M_CONFIRM_GUARD = True                 # L543
PINE_30M_PMA_LENGTH = 1                              # L544
PINE_30M_MIDDLE_LINE = 0.0                           # L545
PINE_30M_CONFIRM_GUARD_LINE_MODE = "predict_upper"   # L549  PMA must be > Predict Upper
PINE_30M_PMA_GUARD_ADD = 0.0                         # L550  fallback only
PINE_30M_USE_QUOTE_VOLUME = True                     # L551
PINE_30M_RATIO_CAP = 8.0                             # L552  cap GF values (v58-safety)
PINE_30M_VIBRATION_MULT = 1.0                        # L553
PINE_30M_EPS = 1e-10                                 # L554
PINE_30M_GOLDEN = 1.618                              # L555
PINE_30M_REQUIRED_CLOSED_DAYS = 4                    # L556  min 4 days of 30m data
PINE_30M_TIMEFRAME_SECONDS = 1800                    # L557

# DF0 force-sell (Purple line = Daily Frequency)
# Once PMA was above DF0, sell if PMA crosses back below DF0
ENABLE_PINE_30M_DF0_CROSS_FORCE_SELL = True          # L562
PINE_30M_DF0_FORCE_SELL_STRICT_ABOVE = True          # L563
PINE_30M_DF0_FORCE_SELL_STRICT_BELOW = True          # L564
PINE_30M_DF0_FORCE_SELL_BELOW_CONFIRM_SECONDS = 720  # L569  12-min confirmation window
PINE_30M_DF0_FORCE_SELL_BELOW_CONFIRM_MAJORITY_RATIO = 0.5  # L570
PINE_30M_DF0_FORCE_SELL_SKIP_PMA_GF_PLUS_LIFECYCLE = True   # L577  GF+ has its own DF0 rule

# Flat/stale PMA handling
PINE_30M_PMA_MARK_FLAT_NEUTRAL = True                # L833
PINE_30M_PMA_FLAT_NEUTRAL_THRESHOLD_PCT = 0.01       # L834
PMA_GF_PLUS_ACTIVE_SELL_SUPPRESS_FLAT_NEUTRAL_LIVE_PMA = True  # L835
```

---

## SECTION 10 — PMA/GF+ Force-Buy Engine (lines 589–952)

```python
# Master switches
ENABLE_PMA_GF_PLUS_FORCE_BUY = True       # L593
ONLY_ALLOW_PMA_GF_PLUS_FORCE_BUYS = True  # L594  ← ALL buys must be GF+ qualified

# Entry offset: live PMA must exceed GF+ by at least +0.060
PMA_GF_PLUS_FORCE_BUY_INSTANT_OFFSET = 0.06   # L597

# --- Setup branches ---
# 1. Closed-PMA direct bypass: if latest CLOSED PMA is already > GF+ and
#    live PMA is > GF+ + 0.060, allow force-BUY without setup-age gates
ENABLE_PMA_GF_PLUS_FORCE_BUY_CLOSED_PMA_GF_PLUS_DIRECT = True   # L611
PMA_GF_PLUS_FORCE_BUY_CLOSED_PMA_DIRECT_MIN_LIVE_ABOVE_CLOSED_PMA = 0.20   # L615
PMA_GF_PLUS_FORCE_BUY_CLOSED_PMA_DIRECT_MIN_NON_FLAT_PRICE_SAMPLES = 2     # L616

# 2. Live-PMA midpoint start: setup begins when live PMA > midpoint(0.0, Predict Lower)
PMA_GF_PLUS_FORCE_BUY_USE_LIVE_PMA_MID_LOWER_START = True        # L623
PMA_GF_PLUS_FORCE_BUY_LIVE_PMA_PREDICT_LOWER_FACTOR = 0.5        # L624

# --- Price/OMA quality gates ---
PMA_GF_PLUS_FORCE_BUY_REQUIRE_PRICE_POSITIVE_RATIO = True         # L627
PMA_GF_PLUS_FORCE_BUY_PRICE_POSITIVE_RATIO_MIN = 0.32             # L628
PMA_GF_PLUS_FORCE_BUY_REQUIRE_OMA_POSITIVE_RATIO = True           # L629
PMA_GF_PLUS_FORCE_BUY_OMA_POSITIVE_RATIO_MIN = 0.32               # L630
PMA_GF_PLUS_FORCE_BUY_POSITIVE_RATIO_MAX_SAMPLES = 240            # L631
PMA_GF_PLUS_FORCE_BUY_POSITIVE_SAMPLE_FLAT_THRESHOLD_PCT = 0.01   # L635  flat = neutral
PMA_GF_PLUS_FORCE_BUY_MIN_NON_FLAT_PRICE_SAMPLES = 7              # L638
PMA_GF_PLUS_FORCE_BUY_MIN_NON_FLAT_OMA_DELTA_SAMPLES = 7          # L639

# --- Price expansion (quality gate) ---
ENABLE_PMA_GF_PLUS_FORCE_BUY_QUALITY_GATE = True                  # L662
PMA_GF_PLUS_FORCE_BUY_REQUIRE_PRICE_EXPANSION = True              # L663
PMA_GF_PLUS_FORCE_BUY_MIN_PRICE_ABOVE_LAST_SCAN_PCT = 0.10        # L664  avg expansion needed
PMA_GF_PLUS_FORCE_BUY_PRICE_EXPANSION_SPLIT_GATE_ENABLED = True   # L671
PMA_GF_PLUS_FORCE_BUY_PRICE_EXPANSION_SPLIT_MIN_HALF_AVG_PCT = 0.01      # L672
PMA_GF_PLUS_FORCE_BUY_PRICE_EXPANSION_SPLIT_MIN_COMBINED_AVG_PCT = 0.32   # L673
PMA_GF_PLUS_FORCE_BUY_PRICE_EXPANSION_SPLIT_MIN_NON_FLAT_PER_HALF = 2     # L674
PMA_GF_PLUS_FORCE_BUY_REQUIRE_LIVE_GREEN_BODY = True              # L679
PMA_GF_PLUS_FORCE_BUY_MAX_REJECTION_WICK_PCT = 6.0               # L680

# --- Pre-entry guards ---
ENABLE_PMA_GF_PLUS_FORCE_BUY_PRE_ENTRY_SMA18_GUARD = True        # L685
PMA_GF_PLUS_FORCE_BUY_PRE_ENTRY_MIN_BODY_BOTTOM_TO_SMA18_PCT = -1.50  # L686
ENABLE_PMA_GF_PLUS_FORCE_BUY_PRE_ENTRY_DF0_GUARD = True          # L691  no entry below DF0

# --- Last-price stability (v8-safety) ---
PMA_GF_PLUS_FORCE_BUY_LAST_PRICE_STABILITY_ENABLED = True         # L714
PMA_GF_PLUS_FORCE_BUY_LAST_PRICE_STABILITY_REQUIRE_PREVIOUS_SAMPLE = True  # L716
PMA_GF_PLUS_FORCE_BUY_LAST_PRICE_STABILITY_MAX_ABS_MOVE_PCT = 8.00  # L718  reject >8% jump
PMA_GF_PLUS_FORCE_BUY_LAST_PRICE_STABILITY_MIN_MOVE_PCT = -0.35     # L720  reject falling entry

# --- Bad-buy score system ---
PMA_GF_PLUS_FORCE_BUY_BAD_SCORE_ENABLED = True                    # L693
PMA_GF_PLUS_FORCE_BUY_BAD_SCORE_ONE_THRESHOLD = 1                 # L695  light penalty
PMA_GF_PLUS_FORCE_BUY_BAD_SCORE_THRESHOLD = 2                     # L699  strong penalty
PMA_GF_PLUS_FORCE_BUY_BAD_SCORE_HARD_THRESHOLD = 4               # L702  hard ban
PMA_GF_PLUS_BAD_BUY_SCORE_LOSS_PCT = -0.80                        # L724  qualifies as loss
PMA_GF_PLUS_BAD_BUY_SCORE_HARD_LOSS_PCT = -1.50                   # L725
PMA_GF_PLUS_BAD_BUY_SCORE_WIN_DECAY_PCT = 2.00                    # L726
PMA_GF_PLUS_BAD_BUY_SCORE_CLEAR_WIN_PCT = 5.00                    # L727  resets bad score

# --- OMA filter ---
ENABLE_PMA_GF_PLUS_OMA_FILTER = True                              # L734
PMA_GF_PLUS_OMA_LENGTH = 4                                        # L735  4-sample moving avg
PMA_GF_PLUS_OMA_MIN_VALUE = 0.16                                  # L736  midpoint-based threshold
PMA_GF_PLUS_OMA_REQUIRE_RISING = False                            # L737

# --- Re-entry gate (V9) ---
ENABLE_PMA_GF_PLUS_REENTRY_GATE = True                            # L799
PMA_GF_PLUS_REENTRY_GATE_MAX_AGE_SECONDS = 7200                   # L800  2 hours
PMA_GF_PLUS_REENTRY_GATE_PMA_IMPROVEMENT = 0.30                   # L805
PMA_GF_PLUS_REENTRY_GATE_OMA_IMPROVEMENT = 0.15                   # L806
PMA_GF_PLUS_REENTRY_GATE_RECLAIM_MIN_OFFSET = 0.06               # L807

# --- Close-transition grace (V48) ---
PMA_GF_PLUS_FORCE_BUY_CLOSE_TRANSITION_GRACE_SECONDS = 120        # L818
PMA_GF_PLUS_ACTIVE_SELL_LIVE_PMA_ROLLOVER_GRACE_SECONDS = 70      # L822

# --- PMA/GF+ force-sell exits ---
# GF-minus elapsed: sell if live PMA < GF+ - 1.40 for 2+ minutes
ENABLE_PMA_GF_PLUS_GF_MINUS_ELAPSED_FORCE_SELL = True             # L920
PMA_GF_PLUS_GF_MINUS_ELAPSED_FORCE_SELL_SECONDS = 120             # L921
PMA_GF_PLUS_GF_MINUS_ELAPSED_FORCE_SELL_MARGIN = 1.40             # L922

# DF0 force-sell for PMA/GF+ lifecycles
ENABLE_PMA_GF_PLUS_BELOW_DF0_FORCE_SELL = True                    # L874
PMA_GF_PLUS_BELOW_DF0_MIN_ELAPSED_SECONDS = 300                   # L877  wait 5 min
PMA_GF_PLUS_BELOW_DF0_CONFIRM_SECONDS = 180                       # L878  3-min confirmation

# Failed upper-predict sell (non-GF+ buys)
ENABLE_NON_GF_PLUS_FAILED_UPPER_PREDICT_FORCE_SELL = True          # L938
NON_GF_PLUS_FAILED_UPPER_PREDICT_FORCE_SELL_SECONDS = 360          # L939  6 minutes

# History window (bounded to current 30m candle + 7s grace)
PMA_GF_PLUS_FORCE_BUY_HISTORY_WINDOW_SECONDS = 1807               # L817
PMA_GF_PLUS_FORCE_BUY_MIN_24H_PCT = 0.0                           # L810
PMA_GF_PLUS_FORCE_BUY_REQUIRE_CLOSED_PMA_ABOVE_HALF_PREDICT_LOWER = True  # L851
```

---

## SECTION 11 — Bale / Notification Config (lines 1328–1457)

```python
PUMP_DELAYED_SELL_SECONDS = 10               # L1328
DAILY_PNL_REPLAY_HOUR = 0                    # L1330
ENABLE_PMA_LINE_PUMPING_MARK = False         # L1334
ENABLE_DAILY_PNL_ONLY_PUMPING_MARK = True    # L1336
BALE_SEND_POSSIBLE_BUY_MESSAGES = False      # L1341
BALE_SEND_WATCH_MESSAGES = False             # L1342
LEGACY_BUY_SURVIVAL_PROMOTES_TO_SHADOW_BUY = True  # L1343
BALE_SEND_LEGACY_SHADOW_BUY_EVENTS = True    # L1344

EVENT_MESSAGE_SENT_FILE = "event_message_sent.json"  # L1360
EVENT_MESSAGE_MAX_RETRIES = 5                # L1361
EVENT_MESSAGE_RETRY_BATCH_SIZE = 20          # L1362
EVENT_MESSAGE_PENDING_TTL_SECONDS = 600      # L1363
EVENT_MESSAGE_NO_RETRY_STAGES = {"buy"}      # L1368
EVENT_MESSAGE_BUY_ATTEMPT_COUNTS_AS_OPEN = True  # L1369
EVENT_MESSAGE_RETENTION_HOURS = 48           # L1370
EVENT_MESSAGE_EQUIVALENT_DEDUPE_MINUTES = 10 # L1371
EVENT_MESSAGE_MERGE_BUY_ALIAS_KEYS = True    # L1379
EVENT_MESSAGE_STORE_COMPACT_PAYLOADS = True  # L1384

BALE_BOT_TOKEN = os.getenv("BALE_BOT_TOKEN", "")  # L1441  ← required for alerts
BALE_CHAT_IDS = ["808704022", "1270763934"]        # L1442
BALE_API_URL = f"https://tapi.bale.ai/bot{BALE_BOT_TOKEN}/sendMessage"  # L1443
BALE_TEXT_CHUNK_MAX_CHARS = 4000                   # L1444
LBANK_PROXY = ""  # L1449  v9: proxy disabled; direct HTTPS
BALE_PROXY = ""   # L1450  v9: proxy disabled; direct HTTPS
BALE_ADMIN_CHAT_ID = "808704022"                   # L1455
SUBSCRIBERS_FILE = "bale_subscribers.json"         # L1456

BALE_SEND_SCAN_DETAILS = False    # L1679
BALE_SCAN_DETAILS_MODE = "compact"  # L1683
```

---

## SECTION 12 — Global State / Locks / Queues (lines 1785–1810)

```python
STATE_LOCK                      = threading.Lock()    # L1785  strategy state R/W
SCAN_EXECUTION_LOCK             = threading.Lock()    # L1786
SCAN_SLOT_STATE_LOCK            = threading.Lock()    # L1787
LAST_FULL_SCAN_FINISHED_SLOT_KEY = None              # L1788
LAST_BUY_SIGNAL_SCAN_FINISHED_SLOT_KEY = None        # L1789
PENDING_SCAN_SLOT_KEYS          = {"full": None, "buy_signal": None}   # L1790
INFLIGHT_SCAN_SLOT_KEYS         = {"full": None, "buy_signal": None}   # L1791
EVENT_MESSAGE_STATE_LOCK        = threading.RLock()   # L1792  reentrant
LOCAL_CHAT_LOCK                 = threading.Lock()    # L1793
LOCAL_SSE_CLIENTS_LOCK          = threading.Lock()    # L1794
LOCAL_SSE_CLIENTS               = set()              # L1795  active SSE connections

BALE_NOTIFICATION_QUEUE         = queue.Queue()       # L1797  async Bale sends
BALE_NOTIFICATION_WORKER_STARTED = False             # L1799
LOG_WRITE_QUEUE                 = queue.Queue()       # L1801  async file writes
LOG_WRITE_WORKER_STARTED        = False              # L1803
EVENT_MESSAGE_RETRY_WORKER_STARTED = False           # L1806
BUY_SIGNAL_LOG_STATS_LOCK       = threading.Lock()   # L1808

# Thread-local storage
NATIVE_HTTP_LOCAL       = threading.local()  # L1719  keep-alive HTTPS connection pool
HTTP_KEEPALIVE_TIMEOUT  = 8                  # L1720  seconds
HTTP_BACKOFF_CAP_SECONDS = 4.0              # L1721
OHLC_PREFETCH_LOCAL     = threading.local()  # L1725  per-scan prefetch context
OHLC_PREFETCH_MISSING   = object()          # L1726  sentinel
SCAN_EVENT_DEDUPE_LOCAL = threading.local()  # L1732  per-scan delivery/dedupe state
```

---

## SECTION 13 — Web & Scheduler Config (lines 1688–1703)

```python
LOCAL_WEB_HOST = "0.0.0.0"                              # L1688
LOCAL_WEB_PORT = int(os.environ.get("PORT", "8765"))    # L1689
LOCAL_WEB_DEFAULT_CHAT_ID = "local-web"                 # L1690
LOCAL_WEB_FETCH_LIMIT_DEFAULT = 200                     # L1691
LOCAL_WEB_FETCH_LIMIT_MAX = 1000                        # L1692
LOCAL_WEB_BODY_LIMIT = 65536                            # L1693
LOCAL_WEB_SSE_QUEUE_SIZE = 200                          # L1694
LOCAL_WEB_SSE_HEARTBEAT_SECONDS = 20                    # L1695
LOCAL_CHAT_RETENTION_HOURS = 48                         # L1696
LOCAL_CHAT_PRUNE_INTERVAL_SECONDS = 300                 # L1697

SCHEDULER_ERROR_BACKOFF_SECONDS = 5                     # L1699
SCHEDULER_IDLE_SLEEP_SECONDS = 1                        # L1700
SCHEDULER_MAX_SLEEP_CHUNK_SECONDS = 1                   # L1701
EVENT_MESSAGE_RETRY_INTERVAL_SECONDS = 120              # L1702
EVENT_MESSAGE_RETRY_MIN_DELAY_SECONDS = 120             # L1703
```

---

## SECTION 14 — ANSI Colours (lines 1705–1714)

```python
RESET    = "\033[0m"
BOLD     = "\033[1m"
COL_POS  = "\033[38;5;28m"   # green  (positive %)
COL_NEG  = "\033[38;5;88m"   # red    (negative %)
COL_NEU  = "\033[38;5;244m"  # grey   (neutral/zero)
COL_HDR  = "\033[38;5;24m"   # blue   (headers)
COL_WARN = "\033[38;5;130m"  # orange (warnings)
COL_INFO = "\033[38;5;240m"  # dark grey (info)
COL_SYMBOL = "\033[38;5;31m" # teal   (symbol names)
COL_PUMP = "\033[38;5;196m"  # bright red (pumping)
```

---

## SECTION 15 — Key Function Groups

### A. Environment & Config Helpers (lines 1011–1215)
| Function | Line | Purpose |
|---|---|---|
| `market_breadth_confirmation_boost_active()` | 1011 | checks if boost threshold met |
| `resolve_buy_confirmation_scans()` | 1023 | returns (pending_scans, buy_scans, boosted) |
| `should_emit_pending_buy_event_for_pumpbox_mode()` | 1052 | |
| `is_usdt_market_symbol()` | 1075 | |
| `should_skip_usdt_market_buys()` | 1091 | |
| `should_include_market_row_for_buy_scan()` | 1095 | |
| `pending_sell_confirm_scans_for_gain()` | 1126 | gain-tiered confirmation counts |
| `pending_sell_confirm_seconds_for_gain()` | 1151 | |
| `sell_zone_confirmation_timing_status()` | 1172 | |
| `best_peak_gain_pct_from_entry()` | 1217 | peak PnL from entry price |

### B. Subscriber & Access Management (lines 1460–1645)
| Function | Line | Purpose |
|---|---|---|
| `load_subscriber_state()` | 1542 | |
| `save_subscriber_state()` | 1557 | |
| `get_last_subscription_end_dt()` | 1570 | |
| `get_user_access_snapshot_from_record()` | 1583 | |
| `get_active_broadcast_chat_ids()` | 1614 | returns chat IDs with active access |
| `persist_last_bot_message_snapshot()` | 1631 | |

### C. Display / Format Helpers (lines 1819–2024)
| Function | Line | Purpose |
|---|---|---|
| `color_pct()` | 1819 | COL_POS/NEG/NEU wrapped % string |
| `plain_pct()` | 1829 | plain ±X.XX% string |
| `message_symbol()` | 1898 | format symbol for Bale messages |
| `price_value_for_display()` | 1949 | USDT/IRT price format |
| `format_duration_compact()` | 2004 | "2h 30m" from seconds |

### D. Log File Management (lines 2025–2210)
| Function | Line | Purpose |
|---|---|---|
| `get_log_path()` | 2025 | returns "log.txt" path |
| `get_buy_signal_log_path()` | 2029 | returns "log_1m.txt" path |
| `ensure_scan_log_files_exist()` | 2040 | creates files if missing |
| `reset_buy_signal_log_stats()` | 2051 | |
| `record_buy_signal_log_stats()` | 2071 | track pass/fail per scan |
| `collect_buy_signal_log_summary()` | 2090 | 30-min summary |
| `enqueue_log_write()` | 2145 | async log write → LOG_WRITE_QUEUE |
| `log_write_worker_loop()` | 2158 | consumes LOG_WRITE_QUEUE |
| `ensure_log_write_worker_started()` | 2184 | |
| `prune_log_file()` | 35042 | trim log.txt to LOG_RETENTION_HOURS=1h |

### E. Pumping Event State (lines 2229–2383)
| Function | Line | Purpose |
|---|---|---|
| `load_pumping_event_sent_state()` | 2273 | |
| `save_pumping_event_sent_state()` | 2285 | |
| `was_pumping_event_sent_for_base()` | 2320 | dedupe pump events |
| `mark_pumping_event_sent_for_base()` | 2348 | |

### F. Event Message Delivery Ledger (lines 2385–4016)
| Function | Line | Purpose |
|---|---|---|
| `get_event_message_delivery_payload()` | 3191 | get stored payload for symbol/stage |
| `is_event_message_delivery_pending()` | 3247 | check if delivery in-flight |
| `can_retry_event_message()` | 3319 | check retry eligibility |
| `try_reserve_event_message_delivery()` | 3334 | atomic reserve before send |
| `was_event_message_sent()` | 3414 | delivered check |
| `is_equivalent_event_message_recently_seen()` | 3419 | dedupe within 10 min |
| `record_event_message_delivery_attempt()` | 3510 | update ledger |
| `mark_event_message_sent()` | 3577 | mark delivered |
| `has_prior_sent_buy_side_signal()` | 3581 | block duplicate BUYs |
| `latest_open_delivered_buy_payload_without_sell()` | 3669 | **anti-duplicate BUY gate** |
| `has_open_delivered_buy_without_sell()` | 3791 | |
| `buy_entry_identity_ts_from_event()` | 3831 | |
| `has_buy_delivery_for_real_entry()` | 3879 | |
| `has_sell_delivery_for_real_entry()` | 3922 | |
| `capture_active_buy_lifecycle_snapshot()` | 4017 | |
| `build_missing_sell_for_orphaned_active_buy()` | 4061 | repair orphaned BUY states |
| `recovered_active_buy_delivery_block_reason()` | 4199 | |

### G. HTTP & TLS Layer (lines 4812–5670)
| Function | Line | Purpose |
|---|---|---|
| `_safe_http_attempt_count()` | 4812 | |
| `_safe_backoff_seconds_from_http_error()` | 4820 | parse backOff from API response |
| `_is_transient_http_status()` | 4843 | 429/500/502/503/504 = retryable |
| `make_ssl_context_for_service()` | 4873 | v9: default TLS, no custom CA |
| `_parse_proxy_url()` | 4886 | parse socks5://host:port |
| `_open_socks5_tunnel()` | 4925 | SOCKS5 tunnel (v9: unused) |
| `http_get_json()` | 5231 | GET with retry logic |
| `http_post_json_absolute()` | 5383 | POST to absolute URL |
| `split_bale_message_text()` | 5471 | chunk by 4000 chars |
| `send_bale_message_raw()` | 5520 | raw Bale API call |
| `send_bale_message()` | 5584 | with error handling |
| `send_bale_message_to_many()` | 5635 | broadcast to BALE_CHAT_IDS |

### H. Bale Notification Worker (lines 5770–5870)
| Function | Line | Purpose |
|---|---|---|
| `send_bale_notification_sync()` | 5770 | sync send (called by worker) |
| `_ensure_bale_notification_worker_started()` | 5857 | start worker thread if needed |

### I. Tehran Timezone (lines 4776–4810)
| Function | Line | Purpose |
|---|---|---|
| `get_tehran_tzinfo()` | 4781 | Asia/Tehran with ZoneInfo fallback |
| `get_tehran_now()` | 4800 | current Tehran datetime |

### J. Market Stats / Universe Builders (lines 6220–6765)
| Function | Line | Purpose |
|---|---|---|
| `fetch_lbank_available_usdt_pairs()` | 6220 | all USDT pairs from LBank |
| `fetch_market_stats()` | 6232 | all pairs + 24h data |
| `fetch_usdt_market_stats()` | 6290 | USDT markets only |
| `build_scan_universe()` | 24548 | FULL: filter 10-280%, sort by 24h |
| `build_buy_signal_scan_universe()` | 24615 | BUY-1M: active + candidates only |
| `build_market_lookup()` | 24463 | symbol → market row dict |
| `build_market_context()` | ~36863 | breadth metrics for scan |
| `sync_pumping_state_across_markets()` | 24503 | propagate pumped state across base |
| `enforce_same_coin_market_rules()` | 24343 | one BUY winner per base coin |

### K. OHLC Fetching & Caching (lines 6892–7640)
| Function | Line | Purpose |
|---|---|---|
| `lbank_kline_type_from_resolution()` | 6892 | "30" → "minute30" |
| `lbank_kline_interval_seconds()` | 6897 | "30" → 1800 |
| `fetch_ohlc_direct()` | 6940 | raw REST call → /v2/kline |
| `fetch_ohlc()` | 7000 | main fetcher with cache + prefetch |
| `fetch_ohlc_with_closed_30m_cache()` | 7640 | V9: closed-bar cache path |
| `prefetch_ohlc_for_scan()` | ~7200 | 7-worker parallel prefetch, 2s timeout |
| `clear_scan_4h_cache()` | ~6891 | reset CACHE_4H between scans |

### L. SMA / Candle Calculations (lines ~8000–10000)
| Function | Line | Purpose |
|---|---|---|
| `compute_sma()` | ~8000 | simple moving average |
| `recent_candles_by_hours()` | ~8050 | filter candles by lookback hours |
| `find_candle_index_by_ts()` | ~8100 | binary search candle by timestamp |
| `enrich_current_forming_candle_with_live_smas()` | 26583 | inject live SMAs |
| `candle_body_on_or_above_ma_range()` | 26618 | body-bottom sit check |
| `average_closed_volume()` | 26641 | avg volume of N closed candles |
| `candle_body_size_pct_value()` | 28744 | signed body % (open→close) |

### M. Pine 30m Calculation Engine (lines ~10800–12000)
| Function | Line | Purpose |
|---|---|---|
| `compute_pine_30m_status()` | ~10800 | **core**: PMA, GF+, GF-, DF0, Predict Upper/Lower |
| Returns dict with: | | `pma`, `gf_plus`, `gf_minus`, `df0`, `predict_upper`, `predict_lower`, `ready` |

### N. Entry Evaluation Guards (lines 8015–9209)
| Function | Line | Purpose |
|---|---|---|
| `evaluate_current_red_entry_skip()` | 8015 | skip red live candle entries |
| `evaluate_closed_entry_skip()` | 8056 | skip flat/red closed candle |
| `evaluate_exact_two_hour_buy_window()` | 9209 | 2h buy window rule |
| `should_allow_entry_attempt()` | 23733 | all entry gates combined |
| `dual_closed_live_24h_requirement_status()` | 23429 | 24h change check |
| `live_candle_green_body_top_above_sma18_status()` | 23558 | |
| `live_body_top_current_reference_breakout_status()` | 23659 | breakout check |

### O. Strategy State Machine (lines 22757–24200)
| Function | Line | Purpose |
|---|---|---|
| `clear_buy_fields()` | 22757 | reset buy-related state fields |
| `reset_state_to_idle()` | 22819 | full state reset, keep cooldown |
| `is_event_fresh()` | 22989 | check event within max-age window |
| `sanitize_state_for_freshness()` | 23003 | expire stale stages |
| `normalize_strategy_states()` | 23266 | bulk sanitize all states |
| `is_symbol_in_entry_skip_cooldown()` | 23297 | |
| `activate_entry_skip_cooldown()` | 23311 | set 2-min skip |
| `evaluate_recent_sma18_guard()` | 23372 | SMA18 guard check |
| `get_state_sell_cooldown_effective_until_ts()` | 23826 | |
| `is_symbol_in_sell_cooldown()` | 24007 | |
| `apply_shared_sell_cooldown()` | 24129 | propagate cooldown across states |
| `set_legacy_buy_block_after_pump_sell()` | 23889 | |
| `is_legacy_buy_blocked_after_recent_pump_sell()` | 23944 | |

### P. Core Strategy Evaluators (lines 25102–31766)
| Function | Line | Purpose |
|---|---|---|
| `evaluate_strategy()` | 25102 | **FULL-5M path**: SMA/MA/pump/candle structure |
| `evaluate_strategy_v2()` | 29326 | **BUY-1M path**: PMA/GF+ force-buy lifecycle |
| `build_pending_buy_setup_snapshot()` | 29152 | capture setup state for breakout tracking |
| `select_pending_buy_reference_closed_candle()` | 29004 | choose reference for breakout top |
| `should_promote_possible_buy_from_today_ledger()` | 25012 | |
| `set_possible_buy_stage_internal()` | 24933 | transition to possible_buy |
| `try_reissue_possible_buy_after_entry_skip()` | 24956 | reissue up to 4 times |

### Q. PMA/GF+ Buy/Sell Engine (lines 27089–28738)
| Function | Line | Purpose |
|---|---|---|
| `buy_rejection_guard_price()` | 27089 | compute rejection guard level |
| `buy_rejection_guard_status()` | 27152 | check if entry is above guard |
| `prepare_breakout_tracking_state()` | 27464 | initialize pumpbox/pending tracking |
| `promote_pending_buy_state()` | 27493 | pending_buy → shadow_pending_buy |
| `promote_buy_state_unchecked()` | 27509 | direct BUY promotion (no gate) |
| `promote_buy_state()` | 27557 | BUY promotion with all gates |
| `pumping_live_pma_gf_plus_message_status()` | 27838 | **core GF+ decision engine** |
| `maybe_emit_pumping_event()` | 28158 | emit pumping BUY/SELL event |
| `activate_legacy_buy_state()` | 27793 | legacy-style BUY activation |
| `try_promote_legacy_buy_to_shadow_buy_fallback()` | 28681 | |
| `promote_legacy_buy_to_pumped_stage()` | 28733 | |

### R. Per-Symbol Analysis (lines 31767–32846)
| Function | Line | Purpose |
|---|---|---|
| `analyze_candidate()` | 31767 | **entry point per coin** — full pipeline |
| `should_live_redo_missing_ohlc()` | 31714 | check if OHLC needs live retry |

`analyze_candidate()` pipeline:
```
1. load + reconcile state from strategy_states
2. check ledger for open BUY without SELL (anti-duplicate)
3. check shared sell cooldown
4. fetch OHLC (from prefetch cache)
5. compute SMA3 / SMA7 / SMA18
6. fetch 4h OHLC → 4h pump metrics
7. compute Pine 30m status (PMA, GF+, DF0, predict bands)
8. synthesize live current-slot candle if needed
9. evaluate_strategy() [FULL-5M] or evaluate_strategy_v2() [BUY-1M]
10. emit event: idle/watch/possible_buy/pending_buy/buy/pumped/sell
11. try_reserve_event_message_delivery() → BALE_NOTIFICATION_QUEUE
12. update STATE_LOCK protected strategy_states
```

### S. Signal Display & Formatting (lines 32887–33500)
| Function | Line | Purpose |
|---|---|---|
| `fmt_num()` | 32887 | numeric formatting with precision |
| `fmt_price_auto()` | 32917 | auto-precision price display |
| `signal_text_from_stage()` | 32993 | stage → "BUY"/"SELL"/etc |
| `signal_emoji_for_display()` | 33032 | stage → emoji |
| `stage_has_signal()` | 33077 | bool: is stage actionable |
| `active_buy_compact_direction()` | 33081 | direction vs entry price |
| `evaluate_active_buy_open_green_reversal_alert()` | 33151 | reversal alert check |
| `active_buy_alert_delivery_delta_allowed()` | 33496 | min PnL change gate for resend |
| `update_active_buy_alert_state()` | 33532 | update alert reference |

### T. Event / Bale Message Building (lines 33744–34270)
| Function | Line | Purpose |
|---|---|---|
| `build_bale_active_buy_line()` | 33760 | format active BUY status line |
| `build_bale_active_alert_rows()` | 33868 | ranked active-BUY alert rows |
| `build_bale_event_rows()` | 34003 | BUY/SELL event rows for broadcast |
| `dedupe_delivery_event_rows()` | 34227 | dedupe by signature |
| `sort_delivery_event_rows()` | 34246 | rank by priority |
| `extend_with_retry_event_rows()` | 34258 | add retryable failed events |
| `build_pma_gf_plus_buy_block_summary_line()` | 34591 | debug BUY_BLOCK_SUMMARY line |

### U. Scan Output (lines 34781–35340)
| Function | Line | Purpose |
|---|---|---|
| `build_terminal_output()` | 34781 | coloured terminal scan table |
| `build_bale_scan_output()` | 34895 | Bale-formatted scan text |
| `build_bale_events_only_output()` | 34974 | events-only Bale output |
| `render_scan_log_block()` | 35122 | log-file version of scan output |
| `append_log()` | 35326 | async write → LOG_WRITE_QUEUE → log.txt |
| `append_buy_signal_30m_summary_log()` | 35370 | write 30m summary to log_1m.txt |

### V. Realized PnL Tracking (lines 35431–36420)
| Function | Line | Purpose |
|---|---|---|
| `_parse_strategy_log_event_records()` | 35522 | parse log.txt BUY+SELL records |
| `find_matching_logged_buy_record_for_sell()` | 36046 | match BUY for each SELL |
| `find_matching_event_buy_record_for_sell()` | 36093 | match from ledger |
| `collect_realized_pnl_report_data()` | 36307 | aggregate trade data |
| `build_realized_pnl_report()` | 36363 | format PnL report |
| `upsert_monthly_realized_pnl_entry()` | 35858 | update monthly_realized_pnl.json |
| `get_monthly_realized_pnl_totals()` | 35826 | read monthly totals |
| `apply_realized_pnl_message_fee()` | 35693 | apply fee to PnL % |

### W. Scan Slot / Scheduler Utilities (lines 36606–36843)
| Function | Line | Purpose |
|---|---|---|
| `interval_slot_key()` | 36606 | dt → "YYYY-MM-DD HH:MM" slot string |
| `is_full_scan_due()` | 36636 | check if FULL-5M slot elapsed |
| `seconds_until_next_full_scan()` | 36640 | |
| `should_suppress_buy_signal_scan_start()` | 36794 | suppress if FULL is pending |
| `mark_scan_slot_pending()` | 36733 | |
| `mark_scan_slot_inflight()` | 36752 | |
| `mark_scan_slot_finished()` | 36711 | |
| `scan_slot_pending_or_inflight()` | 36773 | |
| `has_pending_or_inflight_full_scan()` | 36790 | |
| `collect_scan_touched_symbols()` | 36811 | symbols analyzed this scan |

### X. Main Scan Execution (lines 36845–37100)
| Function | Line | Purpose |
|---|---|---|
| `run_scan_once()` | 36845 | **full scan**: fetch → filter → analyze → output |
| `run_scan_once_safely()` | 37143 | wrapped with traceback catch + backoff |
| `drop_pending_scan_request()` | 37210 | drop stale queue item |
| `enqueue_scan_request()` | 37224 | push scan request to queue |

### Y. Scheduler Loops & Main (lines 37021–37560)
| Function | Line | Purpose |
|---|---|---|
| `is_non_active_buy_blackout_window()` | 37045 | |
| `has_active_buy_positions()` | 37054 | any open BUY in states? |
| `buy_signal_interval_minutes()` | 37063 | adaptive interval |
| `sleep_with_stop()` | 37082 | interruptible sleep |
| `initialize_runtime_services()` | 37103 | start local web + workers |
| `scan_dispatch_loop()` | 37256 | worker: dequeue + run scans |
| `full_scan_scheduler_loop()` | 37294 | fire FULL scan every 5 min |
| `buy_signal_scheduler_loop()` | 37338 | fire BUY-1M scan every ~9.6s |
| `_start_scheduler_thread()` | 37421 | create + start thread |
| `ensure_scheduler_threads_alive()` | 37436 | restart dead threads |
| `start_scheduler_threads()` | 37454 | create all 4 workers |
| `main_worker_only()` | 37492 | main loop, no local web server |
| `main()` | 37526 | main loop + ThreadingHTTPServer :8765 |

---

## SECTION 16 — Classes

### `Socks5HTTPSConnection` (~line 4982)
Custom `http.client.HTTPSConnection` subclass for SOCKS5 proxy tunneling.  
**v9 status:** Proxy disabled (`LBANK_PROXY = ""`), class exists but is not instantiated.

### `StrategyLocalWebHandler` (line 36421)
`BaseHTTPRequestHandler` subclass — local web UI on port 8765.

| Route | Method | Purpose |
|---|---|---|
| `/signaler` | GET | HTML status page with active BUY/signals |
| `/signaler/chat` | GET SSE | Real-time event stream (Server-Sent Events) |
| `/signaler/chat` | POST | Store local chat messages |
| `/signaler/pnl` | GET | Realized PnL report |

```python
local_web_server_loop()   L36591  ThreadingHTTPServer(("0.0.0.0", 8765))
build_local_chat_status() L36363  formats status JSON for web UI
```

---

## SECTION 17 — Threading Architecture

```
Thread 1: full_scan_scheduler_loop (L37294)
  → every 5 minutes: enqueue_scan_request(queue=full_request_queue)

Thread 2: buy_signal_scheduler_loop (L37338)
  → every ~9.6 seconds: enqueue_scan_request(queue=buy_request_queue)
  → suppressed if FULL scan is pending/inflight

Thread 3: scan_dispatch_loop — FULL worker (L37256)
  → dequeues from full_request_queue
  → run_scan_once_safely(scan_mode="full")

Thread 4: scan_dispatch_loop — BUY-1M worker (L37256)
  → dequeues from buy_request_queue
  → run_scan_once_safely(scan_mode="buy_signal")

Worker A: Bale notification worker
  → consumes BALE_NOTIFICATION_QUEUE
  → calls send_bale_message_to_many() for BUY/SELL/alert events

Worker B: Log write worker
  → consumes LOG_WRITE_QUEUE
  → writes to log.txt / log_1m.txt asynchronously

Worker C: Event message retry worker (every 120s)
  → sweeps event_message_sent.json for delivered=False entries
  → retries up to EVENT_MESSAGE_MAX_RETRIES=5 times

Worker D: local_chat_cleanup_loop
  → prunes strategy_local_chat_store.json to 48h retention

ThreadingHTTPServer: port 8765
  → StrategyLocalWebHandler (GET/POST/SSE)
  → runs in background thread via initialize_runtime_services()

Main loop (main() / main_worker_only()):
  → ensure_scheduler_threads_alive() every 1 second
  → ensure_bale_notification_worker_started()
  → ensure_log_write_worker_started()
  → ensure_event_message_retry_worker_started()
```

---

## SECTION 18 — Strategy Signal Flow

```
main()
  └─ initialize_runtime_services()
  └─ start_scheduler_threads() [4 threads + 3 workers]
        │
        ├─ full_scan_scheduler_loop() [every 5m]
        │    └─ enqueue → full_request_queue
        │
        └─ buy_signal_scheduler_loop() [every ~9.6s]
              └─ enqueue → buy_request_queue
                           (suppressed if FULL pending)

scan_dispatch_loop()
  └─ run_scan_once_safely()
        └─ run_scan_once(scan_mode)
              │
              ├─ fetch_usdt_market_stats()          → 947 USDT pairs, 24h data
              │
              ├─ build_scan_universe()               [FULL]
              │  or build_buy_signal_scan_universe() [BUY-1M]
              │    → filter 10-280% 24h change → ~24 candidates
              │    → include any active BUY symbols regardless of 24h
              │
              ├─ prefetch_ohlc_for_scan()            → 7 parallel workers, 2s each
              │
              ├─ for each candidate:
              │    analyze_candidate()
              │      ├─ reconcile state from ledger
              │      ├─ fetch_ohlc(resolution="30", count=240)  ← from prefetch cache
              │      ├─ compute SMA3 / SMA7 / SMA18
              │      ├─ fetch 4h OHLC → pump metrics
              │      ├─ compute_pine_30m_status() → PMA, GF+, GF-, DF0, bands
              │      ├─ synthesize live current-slot candle (if needed)
              │      │
              │      ├─ [FULL-5M]  evaluate_strategy()
              │      │    ├─ watch stage: SMA7>SMA18, body-bottom sit on MA
              │      │    ├─ possible_buy: breakout setup forming
              │      │    ├─ pending_buy: pumpbox confirmation (72s hold)
              │      │    └─ buy: 2h window rule + SMA stack + entry guards
              │      │
              │      └─ [BUY-1M]  evaluate_strategy_v2()
              │           ├─ pumping_live_pma_gf_plus_message_status()
              │           │    ├─ check live PMA > GF+ + 0.06 offset
              │           │    ├─ check price expansion ≥ 0.10% avg
              │           │    ├─ check OMA ≥ 0.16
              │           │    ├─ check bad-buy score < threshold
              │           │    ├─ check last-price stability (max ±8%)
              │           │    ├─ check SMA18 body-bottom guard (-1.50%)
              │           │    └─ check DF0 guard (no entry below DF0)
              │           │
              │           └─ emit force BUY / manage active-BUY exits:
              │                ├─ GF-minus elapsed sell (2 min below GF+-1.40)
              │                ├─ DF0 cross force-sell (12-min confirmation)
              │                ├─ peak drawdown trailing stop (-3.20% from peak)
              │                ├─ universal force-sell (-1.50%)
              │                └─ liquidity/non-flat sample exit
              │
              ├─ enforce_same_coin_market_rules()   → one BUY winner per base coin
              ├─ sync_pumping_state_across_markets()
              ├─ build_terminal_output()            → coloured terminal print
              ├─ append_log()                       → async log.txt write
              └─ deliver events via BALE_NOTIFICATION_QUEUE
```

---

## SECTION 19 — Required .env Variables

| Variable | Default | Required | Notes |
|---|---|---|---|
| `BALE_BOT_TOKEN` | `""` | **Yes** | Bot won't send any alerts without this |
| `HTTP_TIMEOUT` | `25.0` | No | LBank API request timeout (seconds) |
| `RETRY_COUNT` | `2` | No | Retry attempts per failed request |
| `OHLC_PREFETCH_BUDGET_SECONDS` | `55.0` | No | Total prefetch time budget |
| `OHLC_PREFETCH_WORKERS` | `7` | No | Parallel OHLC fetch threads |
| `OHLC_PREFETCH_REQUEST_TIMEOUT` | `2.0` | No | Per-request timeout in prefetch |
| `OHLC_PREFETCH_FIRST_REQUEST_TIMEOUT` | `12.0` | No | First request (cold) timeout |
| `PORT` | `8765` | No | Local web UI port |
| `LBANK_USER_AGENT` | `TraderBot/LBankMARadar` | No | HTTP User-Agent for LBank |

---

## SECTION 20 — Runtime Files Created

| File | Purpose | Retention |
|---|---|---|
| `log.txt` | FULL-5M scan output | 1 hour (auto-trimmed) |
| `log_1m.txt` | BUY-1M scan output | manual |
| `strategy_state.json` | Per-symbol state (stage/buy/sell lifecycle) | persistent |
| `event_message_sent.json` | Bale delivery ledger (BUY/SELL dedupe) | 48 hours |
| `strategy_local_chat_store.json` | Local web chat messages | 48 hours |
| `pump_bale_today_sent.json` | Daily pump marker tracker | daily |
| `pumping_event_sent.json` | Pumping event delivery state | window-based |
| `pumpbox_no_buy_2h_pma_gf_gate.json` | 2h drought PMA/GF+ gate | session |
| `monthly_realized_pnl.json` | Monthly P&L totals by market | persistent |
| `bale_subscribers.json` | Subscriber access state | persistent |
| `bot.pid` | Current bot process ID | runtime |
| `bot_stdout.log` | Combined stdout/stderr | manual |
| `bot_crash.log` | Watchdog crash log | manual |
| `watchdog.log` | Watchdog wrapper output | manual |

---

## Strategy Stage Reference

| Stage | Meaning | Max Age |
|---|---|---|
| `idle` | No signal | — |
| `watch` | Early interest: SMA7>SMA18 + body-bottom on MA | 8h |
| `possible_buy` | Setup forming, not entry-ready | 8h |
| `pending_buy` | Pumpbox confirmation in progress (72s hold) | 8h |
| `buy` | Active real BUY signal, managing position | 24h |
| `pumped` | Active BUY promoted to pumping-style management | 24h |
| `possible_sell` | Sell setup detected | 24h |
| `sell` | Actual SELL signal emitted | 24h |

---

*Generated: 2026-05-28 | Bot running: PID 8915 | Monitor: task b8fojhvac (30-min)*
