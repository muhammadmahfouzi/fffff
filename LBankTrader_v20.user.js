// ==UserScript==
// @name         LBankTrader
// @namespace    local.bale.lbank.1bankbot
// @version      4.0.20-lbank
// @description  پایش زنده و انجام معاملات بازارهای دلاری از جانب شما در LBank
// @match        https://web.bale.ai/*
// @grant        GM_xmlhttpRequest
// @grant        GM_getValue
// @grant        GM_setValue
// @grant        GM_registerMenuCommand
// @grant        GM_notification
// @connect      api.lbkex.com
// @connect      self
// ==/UserScript==

(() => {
  'use strict';

  /******************************************************************
   * تنظیمات اصلی
   ******************************************************************/
  const API_BASE = 'https://api.lbkex.com';
  const LBANK_CREATE_ORDER_TEST_PREFLIGHT_ENABLED = false; // create_order_test.do can reject valid public pairs with 10008; use create_order response as source of truth
  // v19: Only the documented v2 supplement endpoint is used. '/v2/create_order.do' is not an
  // official v2 endpoint and caused redundant 10008 responses. All order retries are now handled
  // via payload-style variants (supplement-style amount vs classic price) within this single path.
  const LBANK_ORDER_ENDPOINTS = ['/v2/supplement/create_order.do'];
  const LBANK_SIGNATURE_HMAC = 'HmacSHA256';
  const LBANK_SIGNATURE_RSA = 'RSA';
  const LBANK_USER_AGENT = 'TraderBot/LBankTraderUserscript';
  const LIVE_PRICE_REFRESH_MS = 10 * 1000;
  const LIVE_PRICE_PROBE_BUFFER = 1.08;
  const ORDER_REQUEST_WINDOW_MS = 10 * 1000;
  const ORDER_REQUEST_DOC_LIMIT = 500;
  const ORDER_REQUEST_SAFE_LIMIT = 450;
  const ORDER_REQUEST_RESERVE = ORDER_REQUEST_DOC_LIMIT - ORDER_REQUEST_SAFE_LIMIT;
  const MARKET_STATS_REQUEST_WINDOW_MS = 10 * 1000;
  const MARKET_STATS_REQUEST_DOC_LIMIT = 200;
  const MARKET_STATS_REQUEST_SAFE_LIMIT = 180;
  const MARKET_STATS_REQUEST_RESERVE = MARKET_STATS_REQUEST_DOC_LIMIT - MARKET_STATS_REQUEST_SAFE_LIMIT;

  const STORAGE_CONFIG = 'lbk_userscript_config_v4_fa';
  const STORAGE_STATE = 'lbk_userscript_state_v4_fa';

  const MIN_USDT_ORDER = 1.50;     // کف امن fallback وقتی LBank حداقل دقیق‌تری برای جفت برنگرداند؛ minTranQua هر جفت با /v2/accuracy.do جداگانه کنترل می‌شود (v19: از 0.70 به 1.50 افزایش یافت تا از رد شدن سفارش‌های زیر حداقل جلوگیری شود)
  const LBANK_PAIR_RULES_CACHE_MS = 10 * 60 * 1000;
  const LBANK_ALLOWED_PAIRS_CACHE_MS = 5 * 60 * 1000;
  const LBANK_MIN_ORDER_QUOTE_BUFFER = 1.02; // بافر برای تبدیل minTranQua به ارزش USDT و کاهش خطای حداقل مقدار
  const HOLD_MAX_MS = 4 * 60 * 60 * 1000; // ۴ ساعت
  const FORCE_SELL_MIN_AGE_MS = 7 * 60 * 1000; // حداقل زمان از آخرین خرید برای فعال‌شدن حد ضرر
  const FORCE_SELL_PNL_THRESHOLD_PERCENT = -2.0; // اگر PnL به این درصد یا کمتر برسد، فروش اجباری می‌زند
  const PUMP_BUY_LOW_GAIN_SELL_THRESHOLD_PERCENT = 1.0; // برای تأمین خرید هنگام پامپ، پوزیشن‌های با سود کمتر از ۱٪ قابل فروش هستند
  const ACTION_DELAY_MS = 10 * 1000; // فاصله عمومی بین هر اقدام فروش/خرید
  const BUY_TRY_DELAY_MS = 5 * 1000; // فاصله بین تلاش‌های خرید/Retry
  const MANUAL_BUY_SWEEP_MAX_ATTEMPTS = 12; // حداکثر تعداد تلاش پشت‌سرهم برای مصرف باقیمانده خرید دستیِ تمام‌موجودی
  const MANUAL_BUY_SESSION_TTL_MS = 10 * 60 * 1000; // مهلت ادامه‌دادن خرید دستیِ تمام‌موجودی
  const PNL_REFRESH_INTERVAL_MS = 20 * 1000; // فاصله به‌روزرسانی تقریبی PnL
  const FEE_RATE_USDT = 0.002; // کارمزد تقریبی LBank برای محاسبه PnL تتری
  const BUY_RETRY_COMPLETE_TOLERANCE = 0.995; // آستانه تکمیل خرید برای retry
  const SELL_KEEP_MAX_USDT_VALUE = 1; // برای فروش، کمتر از این مقدار ارزش تتری از خود دارایی در کیف پول بماند
  const AMOUNT_DECIMALS = 8;        // دقت عمومی مقدار برای سفارش
  const DAILY_REALIZED_PNL_MAX_PAGES = 10; // حداکثر صفحات برای گزارش سود/زیان روزانه
  const TRADE_TX_MATCH_WINDOW_MS = 5 * 60 * 1000; // بازه تطبیق تقریبی معامله و تراکنش کیف‌پول
  const POST_SELL_RESIDUAL_CONVERT_ENABLED = false; // تبدیل خودکار ریزمانده در LBank غیرفعال است
  const POST_SELL_WALLET_SETTLE_DELAY_MS = 1500; // مکث کوتاه برای به‌روزرسانی کیف‌پول بعد از Done شدن فروش
  const POST_SELL_RECENT_INSUFFICIENT_BUY_WINDOW_MS = 5 * 60 * 1000; // بعد از فروش، خریدهای جاافتاده به علت کمبود موجودی تا ۵ دقیقه دوباره امتحان شوند

  const DEFAULT_CONFIG = {
    token: '',        // LBank API Key
    secretKey: '',    // LBank Secret Key یا RSA Private Key
    signatureMethod: LBANK_SIGNATURE_HMAC,
    subTokens: [],
    enableUsdtMarkets: true,
    buyTetherCoinsOnUsdtMarkets: false,
    allowPumpBuys: false,
    sellLowGainHoldingsForPumpBuys: false,
    armed: false,          // اگر روشن باشد اجازه اجرای واقعی دارد
    dryRun: true,          // اگر روشن باشد حتی با armed روشن هم فقط شبیه‌سازی می‌کند
    targetChatName: '1bankbot',
    scanIntervalMs: 4000,
    reconcileIntervalMs: 12000,
    forceSellIntervalMs: 20000,
    maxProcessedKeys: 4000,
    minimized: false,
    signalAlarmEnabled: false,
    signalAlarmVolume: 0.72,
  };

  const DEFAULT_STATE = {
    dayKey: '',
    processedSignals: {},

    latestRadar: {
      blockKey: '',
      timestampText: '',
      text: '',
      lines: [],
      updatedAtMs: 0,
      updatedAtIso: '',
    },

    latestStrategy: {
      blockKey: '',
      timestampText: '',
      text: '',
      lines: [],
      updatedAtMs: 0,
      updatedAtIso: '',
    },

    latestSeenMessage: {
      type: '',
      timestampText: '',
      rawText: '',
      signalKey: '',
      processedAtMs: 0,
      processedAtIso: '',
      base: '',
      quote: '',
    },

    radarMeta: {},          // base -> {star, red, streak, orderIndex}
    currentPriorityList: [],// خروجی مرتب‌شده فعلی
    currentSellList: [],    // فروش‌های پیام استراتژی فعلی
    currentIgnoredBuys: [], // خریدهای نادیده‌گرفته‌شده
    lastBalanceSummary: [], // موجودی‌های غیر صفر

    positions: {},          // BASE|quote -> داده پوزیشن
    pending: {},            // orderId -> سفارش‌های در حال پیگیری
    retryBuys: {},          // BASE|quote -> خریدهای ناقص/ناموفق برای تلاش دوباره

    lastScanAt: 0,
    lastSavedAtMs: 0,
    lastSavedAtIso: '',
  };

  let config = loadJSON(STORAGE_CONFIG, DEFAULT_CONFIG);
  let state = loadJSON(STORAGE_STATE, DEFAULT_STATE);

  normalizeConfigCollections();
  migrateLegacyAccountScopedState();

  let scanBusy = false;
  let reconcileBusy = false;
  let forceSellBusy = false;
  let liveObserver = null;
  let liveScanTimer = 0;
  let lastMutationAt = 0;
  let cachedChatScroller = null;

  let bgAudio = null;
  let bgAudioUrl = null;
  let bgAudioPlaying = false;
  let bgAudioWanted = false;
  let signalAudioContext = null;
  let signalAlarmQueue = Promise.resolve();
  let pnlRefreshBusy = false;
  let lastPnlRefreshAt = 0;
  const lbankPairRulesCache = new Map();
  const activeManualBuyPopupControllers = new Map();
  let activeLivePricePopupTimer = 0;
  const orderRequestBudget = {
    timestamps: [],
    cooldownUntilMs: 0,
    lastBackOffSeconds: 0,
  };
  const marketStatsRequestBudget = {
    timestamps: [],
    cooldownUntilMs: 0,
    lastBackOffSeconds: 0,
  };

  const lbankAllowedPairsCache = {
    pairs: [],
    set: new Set(),
    cachedAtMs: 0,
  };

  const ui = buildPanel();
  ensureDayReset();
  refreshUi();

  GM_registerMenuCommand('تنظیم API اصلی ال‌بانک', () => setupFlow());
  GM_registerMenuCommand('اضافه کردن API فرعی', () => openSubTokenFormPopup());
  GM_registerMenuCommand('لیست API های فرعی', () => openSubTokenListPopup());
  GM_registerMenuCommand('نمایش موجودی', () => showBalances());
  GM_registerMenuCommand('خرید دستی', () => openManualBuyPopup());
  GM_registerMenuCommand('فروش دستی', () => openManualSellPopup());
  GM_registerMenuCommand('لیست سود/زیان', () => showDailyRealizedPnlPopup());
  GM_registerMenuCommand('قیمت لحظه‌ای', () => openLivePricePopup());
  GM_registerMenuCommand('جفت‌های مجاز', () => showAllowedPairsPopup());
  GM_registerMenuCommand('تغییر اجرای واقعی', () => toggleArmed());
  GM_registerMenuCommand('تغییر حالت آزمایشی', () => toggleDryRun());
  GM_registerMenuCommand('تلاش دوباره خریدهای ناقص/ناموفق', () => retryOutstandingBuys());
  GM_registerMenuCommand('ریست پوزیشن‌های محلی', () => resetLocalPositions());
  GM_registerMenuCommand('تغییر اجرای پس‌زمینه', () => toggleBackgroundAudio());
  GM_registerMenuCommand('جمع/باز کردن پنل', () => toggleMinimize());

  log('چت @1bankbot را باز نگه دار.', 'info');
  if (!config.token || !config.secretKey) {
    log('API Key و Secret Key اصلی ال‌بانک هنوز کامل ثبت نشده است.', 'warn');
  }

  setInterval(() => scanVisibleChat().catch(err => logError(err, 'scanVisibleChat')), config.scanIntervalMs);
  setInterval(() => reconcilePendingOrders().catch(err => logError(err, 'reconcilePendingOrders')), config.reconcileIntervalMs);
  setInterval(() => checkForceSell().catch(err => logError(err, 'checkForceSell')), config.forceSellIntervalMs);

  installLiveChatHooks();
  scheduleScan('startup', true, 1400);

  /******************************************************************
   * رابط کاربری
   ******************************************************************/
  function buildPanel() {
    const panel = document.createElement('div');
    panel.id = 'lbk-trader-panel-fa';

    Object.assign(panel.style, {
      position: 'fixed',
      right: '6px',
      top: '70px',
      zIndex: '2147483647',
      width: '97%',
      maxHeight: '78vh',
      minHeight: '22px',
      display: 'flex',
      flexDirection: 'column',
      resize: 'vertical',
      background: '#ffffff',
      color: '#1f2937',
      font: '14px/1.75 Tahoma, Arial, sans-serif',
      border: '1px solid #d7dee8',
      borderRadius: '14px',
      boxShadow: '0 10px 28px rgba(15, 23, 42, 0.14)',
      overflow: 'hidden',
      direction: 'rtl',
    });

    panel.innerHTML = `
      <div id="lbk-header" style="display:flex; align-items:center; justify-content:space-between; gap:8px; padding:10px 12px; background:#f7fafc; border-bottom:1px solid #e5e7eb;">
        <div class="header-corner" style="display: flex; gap: 6px; flex-direction: column;">
        <div style="font-weight:700; font-size:15px; text-wrap:nowrap;">معامله‌گر ال‌بانک 1bankbot@</div>
          <button id="lbk-min-btn" type="button" title="جمع/باز">－</button>
        </div>
        <div style="display:flex; justify-content: flex-end;  align-items:center; gap:6px; flex-wrap:wrap;">
          <button id="lbk-signal-alarm-btn" type="button" class="lbk-header-toggle lbk-icon-toggle" title="آلارم هنگام خرید/فروش">
            <span class="lbk-btn-label">آلارم هنگام خرید/فروش: خاموش</span>
            <span class="lbk-btn-icon" aria-hidden="true">${getSignalAlarmButtonIconSvg(false)}</span>
          </button>
          <button id="lbk-bg-audio-btn" type="button" class="lbk-header-toggle" title="اجرای در پس‌زمینه">اجرا در پس‌زمینه: خاموش</button>
        </div>
      </div>

      <div id="lbk-body" style="display:block; overflow-y:auto; overflow-x:hidden; flex:1 1 auto; min-height:0; overscroll-behavior:contain;">

        <div style="display:flex; gap:6px; flex-wrap:wrap; padding:10px 12px; border-bottom:1px solid #eef2f7;">
          <button id="lbk-setup-btn">تنظیم API اصلی</button>
          <button id="lbk-add-subtoken-btn">اضافه کردن API فرعی</button>
          <button id="lbk-subtokens-btn">لیست API های فرعی</button>
          <button id="lbk-arm-btn">اجرای واقعی</button>
          <button id="lbk-dry-btn">حالت آزمایشی</button>
          <button id="lbk-manual-buy-btn">خرید دستی</button>
          <button id="lbk-manual-sell-btn">فروش دستی</button>
          <button id="lbk-realized-pnl-btn">لیست سود/زیان</button>
          <button id="lbk-live-price-btn">قیمت لحظه‌ای</button>
          <button id="lbk-allowed-pairs-btn">جفت‌های مجاز</button>
          <button id="lbk-scan-btn">اسکن</button>
          <button id="lbk-retry-btn">تلاش دوباره خرید</button>
          <button id="lbk-bal-btn">موجودی</button>
          <button id="lbk-reset-btn">ریست محلی</button>
        </div>

        <div style="padding:10px 12px;">
          <div style="font-weight:700; margin-bottom:6px;">گزارش</div>
          <div id="lbk-log" style="overflow:auto; max-height:24vh; white-space:pre-wrap;"></div>
        </div>

        <div style="padding:10px 12px; border-bottom:1px solid #eef2f7;">
          <div style="font-weight:700; margin-bottom:6px;">موجودی‌های غیرصفر</div>
          <div id="lbk-balances" style="white-space:pre-wrap;"></div>
        </div>

        <div style="padding:10px 12px; border-bottom:1px solid #eef2f7;">
          <div style="font-weight:700; margin-bottom:6px;">پوزیشن‌های باز</div>
          <div id="lbk-positions" style="white-space:pre-wrap;"></div>
        </div>

        <div style="padding:10px 12px; border-bottom:1px solid #eef2f7;">
          <div style="font-weight:700; margin-bottom:6px;">فهرست فعلی</div>
          <div id="lbk-current-list" style="white-space:pre-wrap;"></div>
        </div>

        <div id="lbk-status" style="padding:10px 12px; border-bottom:1px solid #eef2f7; white-space:pre-wrap;"></div>

        <div id="lbk-help" style="padding:10px 12px; border-bottom:1px solid #eef2f7; background:#fbfdff;">
          <div style="font-weight:700; margin-bottom:6px;">توضیح وضعیت‌ها</div>
          <div>• <b>اجرای واقعی</b>: اگر روشن باشد، سیگنال‌ها و اقدام‌های خودکار اجازه ارسال سفارش واقعی دارند.</div>
          <div>• <b>حالت آزمایشی</b>: اگر روشن باشد، همه مسیرها فقط شبیه‌سازی می‌کنند.</div>
          <div>• برای اجرای واقعیِ سیگنال‌ها و اقدام‌های خودکار باید <b>اجرای واقعی روشن</b> و <b>حالت آزمایشی خاموش</b> باشد.</div>
          <div>• <b>خرید دستی</b> و <b>فروش دستی</b> وقتی <b>حالت آزمایشی خاموش</b> باشد، حتی با اجرای واقعی خاموش هم سفارش واقعی می‌فرستند.</div>
          <div>• <b>خرید هنگام پامپ</b>: اگر روشن باشد، سیگنال‌های <b>✴️ | خرید هنگام پامپ</b> هم مثل خرید عادی بررسی و اجرا می‌شوند. سیگنال‌های <b>🔸 | خرید معلق</b> صرفاً نمایش داده می‌شوند و اقدامی روی آن‌ها انجام نمی‌شود.</div>
          <div>• <b>تأمین خرید پامپ با فروش زیر ۱٪ سود</b>: اگر روشن باشد و برای خرید هنگام پامپ موجودی دلاری کافی نباشد، ربات می‌تواند از همان حساب پوزیشن‌های هم‌بازار با سود کمتر از ۱٪ را بفروشد و دوباره خرید پامپ را امتحان کند.</div>
          <div>• فروش اجباری اگر <b>PnL به ٪۲- یا کمتر برسد</b> و حداقل <b>۷ دقیقه</b> از آخرین خرید گذشته باشد، فعال می‌شود؛ همچنین حداکثر زمان نگه‌داری هر پوزیشن هم <b>۴ ساعت</b> است.</div>
          <div>• <b>آلارم هنگام خرید/فروش</b> به شما هنگام خرید و فروش اطلاع می‌دهد.</div>
          <div>• دکمه <b>تلاش دوباره خرید</b> خریدهای ناموفق، ناقص یا جاافتاده را دوباره بررسی و تلاش می‌کند.</div>
          <div>• دکمه <b>قیمت لحظه‌ای</b> قیمت را در پنجره جداگانه نشان می‌دهد و هر ۱۰ ثانیه با درخواست آزمایشی ردشدنی به‌روزرسانی می‌کند.</div>
          <div>• <b>PnL</b> به‌صورت تقریبی از روی قیمت فعلی و با کسر کارمزد فرضی ۰.۲٪ برای بازار دلاری محاسبه می‌شود.</div>
        </div>
      </div>
    `;

    document.documentElement.appendChild(panel);

    const style = document.createElement('style');
    style.textContent = `
      #lbk-trader-panel-fa button {
        border: 1px solid #d3dbe7;
        background: #ffffff;
        color: #0f172a;
        padding: 6px 9px;
        border-radius: 9px;
        cursor: pointer;
        font: 12px/1.35 Tahoma, Arial, sans-serif;
      }
      .lbk-note{
      display: none;
      }
      #lbk-trader-panel-fa button:hover {
        background: #f3f7fb;
      }
      #lbk-trader-panel-fa .lbk-header-toggle {
        min-width: 104px;
        font-weight: 700;
        background: #dbe9ff !important;
      }
      #lbk-trader-panel-fa .lbk-icon-toggle {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        gap: 6px;
        min-width: 176px;
      }
      #lbk-trader-panel-fa .lbk-btn-icon {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        flex: 0 0 auto;
      }
      #lbk-trader-panel-fa .lbk-btn-icon svg {
        width: 15px;
        height: 15px;
        display: block;
        fill: currentColor;
      }
      #lbk-trader-panel-fa .lbk-btn-label {
        display: inline-block;
        white-space: nowrap;
      }
      #lbk-trader-panel-fa #lbk-body {
        scrollbar-width: thin;
      }
      #lbk-trader-panel-fa #lbk-status,
      #lbk-trader-panel-fa #lbk-balances,
      #lbk-trader-panel-fa #lbk-current-list,
      #lbk-trader-panel-fa #lbk-positions,
      #lbk-trader-panel-fa #lbk-log {
        overflow-wrap: anywhere;
        word-break: break-word;
      }
      #lbk-trader-panel-fa .ok { color: #0f8a4b; }
      #lbk-trader-panel-fa .warn { color: #b76e00; }
      #lbk-trader-panel-fa .err { color: #c62828; }
      #lbk-trader-panel-fa .info { color: #1d4ed8; }
      #lbk-trader-panel-fa .muted { color: #64748b; }

      #lbk-signal-alarm-preview{
       border-radius: 10px;
       border: 1px solid #cccccc60;
       background-color: #f5f9ff;
       padding: 6px;
      }

      #lbk-signal-alarm-disable{
       border-radius: 10px;
       border: 1px solid #cccccc60;
       background-color: #f5f9ff;
       padding: 6px;
      }

      .lbk-modal-overlay {
        position: fixed;
        inset: 0;
        z-index: 2147483648;
        background: rgba(15, 23, 42, 0.45);
        display: flex;
        align-items: flex-start;
        justify-content: center;
        padding: max(10px, env(safe-area-inset-top)) 16px max(10px, env(safe-area-inset-bottom));
        direction: rtl;
        overflow-y: auto;
        overscroll-behavior: contain;
      }
      .lbk-modal {
        width: min(560px, calc(100vw - 24px));
        max-height: calc(100dvh - 20px);
        display: flex;
        flex-direction: column;
        overflow: hidden;
        background: #ffffff;
        color: #0f172a;
        border: 1px solid #d7dee8;
        border-radius: 16px;
        box-shadow: 0 18px 42px rgba(15, 23, 42, 0.28);
      }
      .lbk-modal form {
        display: flex;
        flex-direction: column;
        min-height: 0;
        max-height: inherit;
      }
      .lbk-modal-header {
        flex: 0 0 auto;
        padding: 14px 16px 10px;
        border-bottom: 1px solid #e5e7eb;
        font-weight: 700;
        font-size: 18px;
      }
      .lbk-modal-body {
        flex: 1 1 auto;
        min-height: 0;
        overflow-y: auto;
        padding: 16px 20px;
        font-size: 18px;
        line-height: 1.85;
      }
      .lbk-modal-body p {
        margin: 0 0 10px;
      }
      .lbk-modal-body .lbk-note {
        color: #475569;
        font-size: 16px;
        line-height: 1.9;
      }
      .lbk-modal-body .lbk-summary {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 10px 12px;
        margin-bottom: 12px;
        line-height: 1.8;
      }
      .lbk-modal-body label {
        display: block;
        margin-bottom: 8px;
        font-weight: 700;
      }
      .lbk-modal-body input[type="text"],
      .lbk-modal-body input[type="number"],
      .lbk-modal-body select,
      .lbk-modal-body textarea {
        width: 100%;
        box-sizing: border-box;
        border: 1px solid #cbd5e1;
        border-radius: 10px;
        padding: 10px 12px;
        font: 18px/1.6 Tahoma, Arial, sans-serif;
        margin-bottom: 14px;
      }
      .lbk-modal-body textarea {
        min-height: 126px;
        resize: vertical;
        direction: ltr;
        unicode-bidi: plaintext;
        font-family: ui-monospace, SFMono-Regular, Consolas, Menlo, monospace;
        font-size: 13px;
        line-height: 1.55;
      }
      .lbk-modal-body input[type="range"] {
        width: 100%;
        box-sizing: border-box;
        margin: 8px 0 10px;
      }
      .lbk-modal-body .lbk-inline-option {
        display: flex;
        align-items: center;
        gap: 8px;
        font-weight: 400;
        margin-bottom: 8px;
      }
      .lbk-modal-body .lbk-radio-list {
        display: grid;
        gap: 8px;
        margin-bottom: 10px;
      }
      .lbk-live-price-result {
        min-height: 144px;
        white-space: pre-wrap;
        border: 1px solid #dbeafe;
        background: #eff6ff;
        border-radius: 12px;
        padding: 14px 16px;
        margin-top: 12px;
        font-size: 21px;
        line-height: 2.15;
      }
      .lbk-live-price-budget {
        border: 1px dashed #cbd5e1;
        background: #f8fafc;
        border-radius: 10px;
        padding: 8px 10px;
        margin-top: 8px;
        color: #475569;
        font-size: 12px;
        line-height: 1.7;
      }
      .lbk-live-price-budget.is-paused {
        border-color: #fed7aa;
        background: #fff7ed;
        color: #9a3412;
      }
      .lbk-live-price-result.is-error {
        border-color: #fecaca;
        background: #fef2f2;
      }
      .lbk-live-price-result.is-ok {
        border-color: #bbf7d0;
        background: #f0fdf4;
      }
      .lbk-modal-body .lbk-inline-actions {
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        margin-top: 12px;
      }
      .lbk-modal-body .lbk-sell-list {
        max-height: 320px;
        overflow: auto;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 8px;
        background: #f8fafc;
      }
      .lbk-modal-body .lbk-sell-item {
        display: block;
        padding: 9px 10px;
        border-radius: 10px;
        background: #ffffff;
        border: 1px solid #e2e8f0;
        margin-bottom: 8px;
      }
      .lbk-modal-body .lbk-sell-item:last-child {
        margin-bottom: 0;
      }
      .lbk-modal-body .lbk-sell-item small {
        display: block;
        margin-top: 4px;
        color: #475569;
        font-weight: 400;
        line-height: 1.7;
      }
      .lbk-modal-actions {
        flex: 0 0 auto;
        display: flex;
        gap: 8px;
        justify-content: flex-start;
        padding: 12px 16px 16px;
        border-top: 1px solid #e5e7eb;
        background: #ffffff;
      }
      .lbk-modal-actions button {
        border: 1px solid #d3dbe7;
        background: #ffffff;
        color: #0f172a;
        padding: 10px 16px;
        border-radius: 10px;
        cursor: pointer;
        font: 16px/1.35 Tahoma, Arial, sans-serif;
      }
      .lbk-modal-actions button[data-lbk-submit] {
        background: #e8fff1;
        border-color: #9fe4bc;
      }

      .lbk-allowed-pairs-tools {
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        align-items: center;
        margin-bottom: 10px;
      }
      .lbk-allowed-pairs-tools input {
        flex: 1 1 180px;
        min-width: 160px;
        margin-bottom: 0 !important;
      }
      .lbk-allowed-pairs-list {
        max-height: 44vh;
        overflow: auto;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        background: #f8fafc;
        padding: 8px;
        direction: ltr;
        text-align: left;
        font: 13px/1.65 Arial, sans-serif;
      }
      .lbk-allowed-pair-chip {
        display: inline-block;
        margin: 3px;
        padding: 4px 7px;
        border: 1px solid #dbeafe;
        border-radius: 999px;
        background: #eff6ff;
        color: #1e3a8a;
        white-space: nowrap;
      }

      .lbk-manual-buy-live-box {
        display: none;
        margin-top: 12px;
        padding: 12px;
        border: 1px solid #dbeafe;
        border-radius: 12px;
        background: #eff6ff;
      }
      .lbk-manual-buy-live-box.is-active {
        display: block;
      }
      .lbk-manual-buy-live-box.is-finished {
        border-color: #bbf7d0;
        background: #f0fdf4;
      }
      .lbk-manual-buy-live-box.is-error {
        border-color: #fecaca;
        background: #fef2f2;
      }
      .lbk-manual-buy-live-row {
        display: flex;
        align-items: flex-start;
        gap: 10px;
      }
      .lbk-loading-spinner {
        width: 18px;
        height: 18px;
        border: 2px solid #bfdbfe;
        border-top-color: #2563eb;
        border-radius: 9999px;
        animation: lbk-spin 0.8s linear infinite;
        flex: 0 0 auto;
        margin-top: 2px;
      }
      .lbk-manual-buy-live-box.is-finished .lbk-loading-spinner,
      .lbk-manual-buy-live-box.is-error .lbk-loading-spinner {
        display: none;
      }
      @keyframes lbk-spin {
        to { transform: rotate(360deg); }
      }
    `;
    document.documentElement.appendChild(style);

    panel.querySelector('#lbk-setup-btn').addEventListener('click', () => setupFlow());
    panel.querySelector('#lbk-add-subtoken-btn').addEventListener('click', () => openSubTokenFormPopup());
    panel.querySelector('#lbk-subtokens-btn').addEventListener('click', () => openSubTokenListPopup());
    panel.querySelector('#lbk-arm-btn').addEventListener('click', () => toggleArmed());
    panel.querySelector('#lbk-dry-btn').addEventListener('click', () => toggleDryRun());
    panel.querySelector('#lbk-manual-buy-btn').addEventListener('click', () => openManualBuyPopup());
    panel.querySelector('#lbk-manual-sell-btn').addEventListener('click', () => openManualSellPopup());
    panel.querySelector('#lbk-realized-pnl-btn').addEventListener('click', () => showDailyRealizedPnlPopup());
    panel.querySelector('#lbk-live-price-btn').addEventListener('click', () => openLivePricePopup());
    panel.querySelector('#lbk-allowed-pairs-btn').addEventListener('click', () => showAllowedPairsPopup());
    panel.querySelector('#lbk-scan-btn').addEventListener('click', () => handleManualScanClick());
    panel.querySelector('#lbk-retry-btn').addEventListener('click', () => retryOutstandingBuys());
    panel.querySelector('#lbk-bal-btn').addEventListener('click', () => showBalances());
    panel.querySelector('#lbk-reset-btn').addEventListener('click', () => resetLocalPositions());
    panel.querySelector('#lbk-signal-alarm-btn').addEventListener('click', () => handleSignalAlarmButtonClick());
    panel.querySelector('#lbk-bg-audio-btn').addEventListener('click', () => toggleBackgroundAudio());
    panel.querySelector('#lbk-min-btn').addEventListener('click', () => toggleMinimize());

    return {
      panel,
      bodyEl: panel.querySelector('#lbk-body'),
      statusEl: panel.querySelector('#lbk-status'),
      balancesEl: panel.querySelector('#lbk-balances'),
      currentListEl: panel.querySelector('#lbk-current-list'),
      positionsEl: panel.querySelector('#lbk-positions'),
      logEl: panel.querySelector('#lbk-log'),
      minBtn: panel.querySelector('#lbk-min-btn'),
      signalAlarmBtn: panel.querySelector('#lbk-signal-alarm-btn'),
      bgAudioBtn: panel.querySelector('#lbk-bg-audio-btn'),
      armBtn: panel.querySelector('#lbk-arm-btn'),
      dryBtn: panel.querySelector('#lbk-dry-btn'),
      manualBuyBtn: panel.querySelector('#lbk-manual-buy-btn'),
      manualSellBtn: panel.querySelector('#lbk-manual-sell-btn'),
      realizedPnlBtn: panel.querySelector('#lbk-realized-pnl-btn'),
      livePriceBtn: panel.querySelector('#lbk-live-price-btn'),
    };
  }


  async function handleManualScanClick() {
    const scanBtn = ui.panel?.querySelector('#lbk-scan-btn');
    const oldText = scanBtn ? scanBtn.textContent : '';
    if (scanBtn) {
      scanBtn.disabled = true;
      scanBtn.textContent = 'در حال اسکن...';
    }

    try {
      cachedChatScroller = null;
      keepChatScrolledToBottom(true);
      const result = await scanVisibleChat(true);

      if (result?.ok) {
        const buyCount = Array.isArray(state.currentPriorityList) ? state.currentPriorityList.length : 0;
        const sellCount = Array.isArray(state.currentSellList) ? state.currentSellList.length : 0;
        log(`اسکن دستی انجام شد. خریدهای فعلی: ${formatFaNumber(buyCount)} | فروش‌های فعلی: ${formatFaNumber(sellCount)}`, 'ok');
      } else {
        log(`اسکن دستی نتیجه‌ای نداشت: ${result?.reason || 'داده‌ای پیدا نشد.'}`, 'warn');
      }
    } catch (err) {
      logError(err, 'handleManualScanClick');
    } finally {
      if (scanBtn) {
        scanBtn.disabled = false;
        scanBtn.textContent = oldText || 'اسکن';
      }
      refreshUi();
    }
  }

  function toggleMinimize() {
    config.minimized = !config.minimized;
    saveConfig();
    refreshUi();
  }

  function getSignalAlarmButtonIconSvg(enabled) {
    if (enabled) {
      return `
      <svg viewBox="0 0 24 24" focusable="false" aria-hidden="true">
        <path d="M14.5 4.5a1 1 0 0 1 1.7.7v13.6a1 1 0 0 1-1.7.7L9.7 15H6a2 2 0 0 1-2-2v-2a2 2 0 0 1 2-2h3.7l4.8-4.5Z"></path>
        <path d="M18.2 8.2a1 1 0 0 1 1.4 0 5.4 5.4 0 0 1 0 7.6 1 1 0 0 1-1.4-1.4 3.4 3.4 0 0 0 0-4.8 1 1 0 0 1 0-1.4Z"></path>
        <path d="M20.7 5.7a1 1 0 0 1 1.4 0 9 9 0 0 1 0 12.6 1 1 0 1 1-1.4-1.4 7 7 0 0 0 0-9.8 1 1 0 0 1 0-1.4Z"></path>
      </svg>`;
    }

    return `
      <svg viewBox="0 0 24 24" focusable="false" aria-hidden="true">
        <path d="M14.5 4.5a1 1 0 0 1 1.7.7v13.6a1 1 0 0 1-1.7.7L9.7 15H6a2 2 0 0 1-2-2v-2a2 2 0 0 1 2-2h3.7l4.8-4.5Z"></path>
        <path d="M6 6L18 18" stroke="currentColor" stroke-width="2" stroke-linecap="round" fill="none"></path>
      </svg>`;
  }

  function refreshUi() {
    const latestRadarTs = state.latestRadar?.timestampText || 'ندارد';
    const latestStrategyTs = state.latestStrategy?.timestampText || 'ندارد';
    const latestMsgTs = state.latestSeenMessage?.timestampText || 'ندارد';
    const subTokens = getSubTokens(true);
    const activeSubTokens = subTokens.filter(item => item.active && item.token);

    const lines = [
      `چت هدف: @${config.targetChatName}`,
      `API اصلی ال‌بانک: ${config.token && config.secretKey ? 'ثبت شده' : 'ثبت نشده'}`,
      `روش امضای API اصلی: ${lbankSignatureMethodLabel(config.signatureMethod)}`,
      `APIهای فرعی: ${formatFaNumber(activeSubTokens.length)} فعال از ${formatFaNumber(subTokens.length)}`,
      `بازارها: USD-like`,
      `خرید هنگام پامپ: ${config.allowPumpBuys ? 'روشن' : 'خاموش'}`,
      `تأمین خرید پامپ با فروش زیر ۱٪ سود: ${config.sellLowGainHoldingsForPumpBuys ? 'روشن' : 'خاموش'}`,
      `اجرای واقعی: ${config.armed ? 'روشن' : 'خاموش'}`,
      `حالت آزمایشی: ${config.dryRun ? 'روشن' : 'خاموش'}`,
      `آلارم خرید/فروش: ${config.signalAlarmEnabled ? `روشن | حجم ${formatFaNumber(Math.round(getSignalAlarmVolume() * 100))}٪` : 'خاموش'}`,
      `اجرای پس‌زمینه: ${bgAudioPlaying ? 'در حال پخش' : (bgAudioWanted ? 'در حال شروع' : 'خاموش')}`,
      `رادار آخر: ${latestRadarTs}`,
      `استراتژی آخر: ${latestStrategyTs}`,
      `آخرین سیگنال پردازش‌شده: ${latestMsgTs}`,
      `پوزیشن باز: ${Object.values(state.positions).filter(p => num(p.qty) > 0).length}`,
      `سفارش در حال پیگیری: ${Object.keys(state.pending).length}`,
      `خریدهای آماده تلاش دوباره: ${countRetryBuysNeeded()}`,
      `آخرین ذخیره: ${state.lastSavedAtIso || '—'}`,
    ];
    ui.statusEl.textContent = lines.join('\n');

    ui.armBtn.textContent = `اجرای واقعی: ${config.armed ? 'روشن' : 'خاموش'}`;
    ui.dryBtn.textContent = `حالت آزمایشی: ${config.dryRun ? 'روشن' : 'خاموش'}`;

    ui.armBtn.style.background = config.armed ? '#e8fff1' : '#ffffff';
    ui.armBtn.style.borderColor = config.armed ? '#9fe4bc' : '#d3dbe7';

    ui.dryBtn.style.background = config.dryRun ? '#fff7e8' : '#ffffff';
    ui.dryBtn.style.borderColor = config.dryRun ? '#f2cc7b' : '#d3dbe7';

    ui.bodyEl.style.display = config.minimized ? 'none' : 'block';
    ui.bodyEl.style.overflowY = config.minimized ? 'hidden' : 'auto';
    ui.minBtn.textContent = config.minimized ? 'بازکردن پنل  ＋' : 'کوتاه‌کردن پنل  －';

    if (ui.signalAlarmBtn) {
      const signalAlarmVolumePercent = formatFaNumber(Math.round(getSignalAlarmVolume() * 100));
      ui.signalAlarmBtn.innerHTML = `

        <span class="lbk-btn-label">آلارم هنگام خرید/فروش: ${config.signalAlarmEnabled ? 'روشن' : 'خاموش'}</span>
        <span class="lbk-btn-icon" aria-hidden="true">${getSignalAlarmButtonIconSvg(config.signalAlarmEnabled)}</span>
      `;
      ui.signalAlarmBtn.style.background = config.signalAlarmEnabled ? '#e8fff1' : '#ffffff';
      ui.signalAlarmBtn.style.borderColor = config.signalAlarmEnabled ? '#9fe4bc' : '#d3dbe7';
      ui.signalAlarmBtn.style.color = config.signalAlarmEnabled ? '#0f8a4b' : '#0f172a';
      ui.signalAlarmBtn.title = config.signalAlarmEnabled ? 'برای تنظیم صدا یا غیرفعال‌کردن آلارم بزن' : 'آلارم هنگام خرید/فروش را روشن کن';
    }

    if (ui.bgAudioBtn) {
      ui.bgAudioBtn.textContent = bgAudioPlaying ? 'اجرا در پس‌زمینه: روشن' : (bgAudioWanted ? 'اجرا در پس‌زمینه: شروع...' : 'اجرا در پس‌زمینه: خاموش');
      ui.bgAudioBtn.style.background = bgAudioPlaying ? '#e8fff1' : (bgAudioWanted ? '#eff6ff' : '#ffffff');
      ui.bgAudioBtn.style.borderColor = bgAudioPlaying ? '#9fe4bc' : (bgAudioWanted ? '#93c5fd' : '#d3dbe7');
      ui.bgAudioBtn.style.color = bgAudioPlaying ? '#0f8a4b' : (bgAudioWanted ? '#1d4ed8' : '#0f172a');
      ui.bgAudioBtn.title = bgAudioPlaying ? 'اجرای پس‌زمینه روشن است' : (bgAudioWanted ? 'در حال شروع اجرای پس‌زمینه' : 'اجرای پس‌زمینه خاموش است');
    }

    renderBalances();
    renderCurrentList();
    renderPositions();
    refreshOpenPositionsPnl(false).catch(() => {});
  }


  async function refreshOpenPositionsPnl(force = false) {
    const openPositions = Object.values(state.positions || {})
      .filter(p => num(p.qty) > 0);

    if (!openPositions.length) return;
    if (pnlRefreshBusy) return;

    const now = Date.now();
    if (!force && now - lastPnlRefreshAt < PNL_REFRESH_INTERVAL_MS) return;

    pnlRefreshBusy = true;

    try {
      let changed = false;

      for (const p of openPositions) {
        let stats = null;
        try {
          stats = await getMarketStats(p.base, p.quote);
        } catch (err) {
          logError(err, `refreshOpenPositionsPnl ${p.base}/${p.quote}`);
          continue;
        }

        const price = num(stats?.bestBuy || stats?.latest || stats?.mark || stats?.dayClose || p.lastKnownPrice);
        if (!(price > 0)) continue;

        const qty = Math.max(0, num(p.qty));
        const grossValue = qty * price;
        const netValue = Math.max(0, grossValue - feeValueForQuote(grossValue, p.quote));
        const cost = Math.max(0, num(p.entryCostQuote));
        const pnlValue = cost > 0 ? netValue - cost : 0;
        const pnlPercent = cost > 0 ? (pnlValue / cost) * 100 : 0;

        p.lastKnownPrice = price;
        p.lastMarketValue = grossValue;
        p.lastNetValue = netValue;
        p.lastPnlValue = pnlValue;
        p.lastPnlPercent = pnlPercent;
        p.lastPnlAt = Date.now();
        if (!p.entryCostMode) {
          p.entryCostMode = cost > 0 ? 'tracked' : 'unknown';
        }
        changed = true;
      }

      lastPnlRefreshAt = Date.now();
      if (changed) saveState();
    } finally {
      pnlRefreshBusy = false;
    }
  }

  function bgAudioLog(...args) {
    console.log('[معامله‌گر ال‌بانک: اجرا در پس‌زمینه]', ...args);
  }

  function makeBackgroundArtworkDataUrl() {
    const svg = `
      <svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512">
        <defs>
          <linearGradient id="g" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stop-color="#1d4ed8"/>
            <stop offset="100%" stop-color="#16a34a"/>
          </linearGradient>
        </defs>
        <rect width="512" height="512" rx="96" fill="url(#g)"/>
        <circle cx="256" cy="256" r="120" fill="rgba(255,255,255,0.14)"/>
        <circle cx="256" cy="256" r="84" fill="rgba(255,255,255,0.18)"/>
        <polygon points="232,202 332,256 232,310" fill="#ffffff"/>
        <text x="256" y="410" text-anchor="middle" font-size="36" fill="#ffffff" font-family="Tahoma, Arial, sans-serif">معامله‌گر ال‌بانک</text>
      </svg>
    `.trim();

    return 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svg);
  }

  function makeBackgroundLoopWavBlob({
    sampleRate = 22050,
    seconds = 30,
    gain = 0.00003,
    freq = 220
  } = {}) {
    const totalSamples = Math.floor(sampleRate * seconds);
    const numChannels = 1;
    const bitsPerSample = 16;
    const blockAlign = numChannels * bitsPerSample / 8;
    const byteRate = sampleRate * blockAlign;
    const dataSize = totalSamples * blockAlign;
    const buffer = new ArrayBuffer(44 + dataSize);
    const view = new DataView(buffer);

    function writeString(offset, str) {
      for (let i = 0; i < str.length; i++) {
        view.setUint8(offset + i, str.charCodeAt(i));
      }
    }

    writeString(0, 'RIFF');
    view.setUint32(4, 36 + dataSize, true);
    writeString(8, 'WAVE');
    writeString(12, 'fmt ');
    view.setUint32(16, 16, true);
    view.setUint16(20, 1, true);
    view.setUint16(22, numChannels, true);
    view.setUint32(24, sampleRate, true);
    view.setUint32(28, byteRate, true);
    view.setUint16(32, blockAlign, true);
    view.setUint16(34, bitsPerSample, true);
    writeString(36, 'data');
    view.setUint32(40, dataSize, true);

    let offset = 44;
    const fadeSamples = Math.floor(sampleRate * 0.04);

    for (let i = 0; i < totalSamples; i++) {
      const t = i / sampleRate;
      let sample = Math.sin(2 * Math.PI * freq * t);

      let envelope = 1;
      if (i < fadeSamples) envelope = i / fadeSamples;
      if (i > totalSamples - fadeSamples) {
        envelope = Math.min(envelope, (totalSamples - i) / fadeSamples);
      }

      sample *= envelope * gain;

      if (sample > 1) sample = 1;
      if (sample < -1) sample = -1;

      view.setInt16(offset, sample * 0x7FFF, true);
      offset += 2;
    }

    return new Blob([buffer], { type: 'audio/wav' });
  }

  function setBackgroundMediaState(stateName) {
    if (!('mediaSession' in navigator)) return;
    try {
      navigator.mediaSession.playbackState = stateName;
    } catch (_) {}
  }

  function setupBackgroundMediaSession() {
    if (!('mediaSession' in navigator)) return;

    try {
      navigator.mediaSession.metadata = new MediaMetadata({
        title: 'معامله‌گر ال‌بانک',
        artist: 'LBankTrader Userscript',
        album: 'Background Playback',
        artwork: [
          { src: makeBackgroundArtworkDataUrl(), sizes: '512x512', type: 'image/svg+xml' }
        ]
      });

      navigator.mediaSession.setActionHandler('play', async () => {
        try {
          await startBackgroundAudio();
        } catch (err) {
          console.error('[معامله‌گر ال‌بانک: اجرا در پس‌زمینه] media play failed', err);
        }
      });

      navigator.mediaSession.setActionHandler('pause', () => {
        stopBackgroundAudio();
      });

      try {
        navigator.mediaSession.setActionHandler('stop', () => {
          stopBackgroundAudio();
        });
      } catch (_) {}

      try {
        navigator.mediaSession.setActionHandler('seekbackward', null);
        navigator.mediaSession.setActionHandler('seekforward', null);
        navigator.mediaSession.setActionHandler('seekto', null);
        navigator.mediaSession.setActionHandler('previoustrack', null);
        navigator.mediaSession.setActionHandler('nexttrack', null);
      } catch (_) {}
    } catch (err) {
      console.warn('[معامله‌گر ال‌بانک: اجرا در پس‌زمینه] mediaSession setup failed', err);
    }
  }

  function ensureBackgroundAudio() {
    if (bgAudio) return bgAudio;

    const blob = makeBackgroundLoopWavBlob({
      sampleRate: 22050,
      seconds: 30,
      gain: 0.00003,
      freq: 220
    });

    bgAudioUrl = URL.createObjectURL(blob);

    bgAudio = document.createElement('audio');
    bgAudio.src = bgAudioUrl;
    bgAudio.preload = 'auto';
    bgAudio.loop = true;
    bgAudio.muted = false;
    bgAudio.volume = 1.0;
    bgAudio.playsInline = true;
    bgAudio.style.display = 'none';

    document.documentElement.appendChild(bgAudio);

    bgAudio.addEventListener('playing', () => {
      bgAudioPlaying = true;
      setBackgroundMediaState('playing');
      refreshUi();
      bgAudioLog('audio started');
    });

    bgAudio.addEventListener('pause', () => {
      bgAudioPlaying = false;
      setBackgroundMediaState('paused');
      refreshUi();
      bgAudioLog('audio paused');
    });

    bgAudio.addEventListener('ended', async () => {
      bgAudioLog('audio ended');
      if (!bgAudioWanted) return;

      try {
        bgAudio.currentTime = 0;
        await bgAudio.play();
      } catch (err) {
        console.error('[معامله‌گر ال‌بانک: اجرا در پس‌زمینه] replay failed', err);
        log('پخش پس‌زمینه دوباره شروع نشد.', 'warn');
      }
    });

    bgAudio.addEventListener('error', () => {
      const err = bgAudio && bgAudio.error ? bgAudio.error : null;
      const code = err ? err.code : 'unknown';
      bgAudioPlaying = false;
      setBackgroundMediaState('paused');
      refreshUi();
      log(`خطای پخش پس‌زمینه (${code})`, 'err');
      bgAudioLog('audio error', err);
    });

    setupBackgroundMediaSession();
    return bgAudio;
  }

  async function startBackgroundAudio() {
    bgAudioWanted = true;
    refreshUi();

    const a = ensureBackgroundAudio();

    try {
      await a.play();
      log('اجرای پس‌زمینه روشن شد.', 'ok');
      return true;
    } catch (err) {
      bgAudioWanted = false;
      bgAudioPlaying = false;
      setBackgroundMediaState('paused');
      refreshUi();
      log('پخش پس‌زمینه شروع نشد؛ دوباره بزن.', 'warn');
      console.error('[معامله‌گر ال‌بانک: اجرا در پس‌زمینه] play() failed', err);
      return false;
    }
  }

  function stopBackgroundAudio() {
    bgAudioWanted = false;

    if (!bgAudio) {
      bgAudioPlaying = false;
      setBackgroundMediaState('paused');
      refreshUi();
      log('اجرای پس‌زمینه خاموش شد.', 'info');
      return;
    }

    bgAudio.pause();

    try {
      bgAudio.currentTime = 0;
    } catch (_) {}

    bgAudioPlaying = false;
    setBackgroundMediaState('paused');
    refreshUi();
    log('اجرای پس‌زمینه خاموش شد.', 'info');
  }

  async function toggleBackgroundAudio() {
    if (bgAudioPlaying || bgAudioWanted) {
      stopBackgroundAudio();
      return;
    }
    await startBackgroundAudio();
  }

  function cleanupBackgroundAudio() {
    try {
      if (bgAudio) {
        bgAudio.pause();
        bgAudio.remove();
      }
      if (bgAudioUrl) {
        URL.revokeObjectURL(bgAudioUrl);
      }
    } catch (_) {}

    bgAudio = null;
    bgAudioUrl = null;
    bgAudioPlaying = false;
    bgAudioWanted = false;
    setBackgroundMediaState('paused');
  }

  function getSignalAlarmVolume() {
    const value = Number(config.signalAlarmVolume);
    if (Number.isFinite(value)) {
      return clamp(value, 0, 1);
    }
    return DEFAULT_CONFIG.signalAlarmVolume;
  }

  function ensureSignalAudioContext() {
    const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
    if (!AudioContextCtor) return null;

    if (signalAudioContext && signalAudioContext.state !== 'closed') {
      return signalAudioContext;
    }

    try {
      signalAudioContext = new AudioContextCtor({ latencyHint: 'interactive' });
    } catch (_) {
      signalAudioContext = new AudioContextCtor();
    }

    return signalAudioContext;
  }

  async function unlockSignalAudioContext() {
    const ctx = ensureSignalAudioContext();
    if (!ctx) {
      throw new Error('مرورگر از Web Audio API پشتیبانی نمی‌کند.');
    }

    if (ctx.state === 'suspended') {
      await ctx.resume();
    }

    return ctx;
  }

  function scheduleSignalTone(ctx, destination, startAt, duration, {
    type = 'sine',
    frequency = 432,
    endFrequency = null,
    gain = 0.18,
    attack = 0.08,
    release = 0.12,
  } = {}) {
    const start = Math.max(ctx.currentTime, startAt);
    const end = start + Math.max(0.02, duration);
    const safeAttack = clamp(attack, 0.002, Math.max(0.006, duration * 0.45));
    const safeRelease = clamp(release, 0.01, Math.max(0.03, duration * 0.75));
    const sustainStart = Math.min(end, start + safeAttack);
    const releaseStart = Math.max(sustainStart, end - safeRelease);

    const osc = ctx.createOscillator();
    osc.type = type;

    const amp = ctx.createGain();
    amp.gain.setValueAtTime(0.0001, start);
    amp.gain.linearRampToValueAtTime(Math.max(0.0001, gain), sustainStart);
    amp.gain.setValueAtTime(Math.max(0.0001, gain), releaseStart);
    amp.gain.exponentialRampToValueAtTime(0.0001, end);

    osc.frequency.setValueAtTime(Math.max(40, frequency), start);
    if (endFrequency && endFrequency > 0 && endFrequency !== frequency) {
      osc.frequency.exponentialRampToValueAtTime(Math.max(40, endFrequency), end);
    }

    osc.connect(amp);
    amp.connect(destination);
    osc.start(start);
    osc.stop(end + 0.02);

    return end;
  }

  function scheduleHappyBuyAlarm(ctx, destination, startAt) {
    let cursor = startAt;
    const notes = [660, 880, 1175, 1320];

    notes.forEach((frequency, index) => {
      const duration = index === notes.length - 1 ? 0.26 : 0.13;
      const wave = index >= 2 ? 'triangle' : 'sine';
      scheduleSignalTone(ctx, destination, cursor, duration, {
        type: wave,
        frequency,
        endFrequency: frequency * (index === notes.length - 1 ? 1.025 : 1.01),
        gain: index === notes.length - 1 ? 0.17 : 0.13,
        attack: 0.008,
        release: 0.11,
      });
      scheduleSignalTone(ctx, destination, cursor + 0.01, duration * 0.85, {
        type: 'sine',
        frequency: frequency / 2,
        endFrequency: (frequency / 2) * 1.01,
        gain: index === notes.length - 1 ? 0.06 : 0.045,
        attack: 0.012,
        release: 0.12,
      });
      cursor += index === notes.length - 1 ? 0.16 : 0.105;
    });

    return cursor + 0.10;
  }

  function scheduleExcitedBuyAlarm(ctx, destination, startAt) {
    let cursor = startAt;
    const bursts = [
      { frequency: 784, gain: 0.12, duration: 0.07 },
      { frequency: 988, gain: 0.135, duration: 0.07 },
      { frequency: 1175, gain: 0.145, duration: 0.07 },
      { frequency: 1568, gain: 0.16, duration: 0.08 },
      { frequency: 1760, gain: 0.19, duration: 0.20 },
    ];

    bursts.forEach((step, index) => {
      scheduleSignalTone(ctx, destination, cursor, step.duration, {
        type: index >= 3 ? 'triangle' : 'square',
        frequency: step.frequency,
        endFrequency: step.frequency * (index === bursts.length - 1 ? 1.035 : 1.012),
        gain: step.gain,
        attack: 0.004,
        release: index === bursts.length - 1 ? 0.14 : 0.06,
      });
      scheduleSignalTone(ctx, destination, cursor + 0.008, step.duration * 0.8, {
        type: 'sine',
        frequency: step.frequency / 2,
        endFrequency: (step.frequency / 2) * 1.01,
        gain: Math.max(0.028, step.gain * 0.32),
        attack: 0.006,
        release: 0.08,
      });
      cursor += index === bursts.length - 1 ? 0.14 : 0.075;
    });

    return cursor + 0.08;
  }

  function scheduleSellAlarm(ctx, destination, startAt) {
    let cursor = startAt;
    const notes = [
      { from: 620, to: 460, duration: 0.16, gain: 0.17 },
      { from: 480, to: 330, duration: 0.18, gain: 0.16 },
      { from: 360, to: 240, duration: 0.22, gain: 0.15 },
    ];

    notes.forEach((step, index) => {
      scheduleSignalTone(ctx, destination, cursor, step.duration, {
        type: index === 0 ? 'sawtooth' : 'square',
        frequency: step.from,
        endFrequency: step.to,
        gain: step.gain,
        attack: 0.006,
        release: 0.12,
      });
      scheduleSignalTone(ctx, destination, cursor + 0.012, Math.max(0.08, step.duration - 0.03), {
        type: 'triangle',
        frequency: Math.max(80, step.from * 0.52),
        endFrequency: Math.max(60, step.to * 0.56),
        gain: 0.045,
        attack: 0.01,
        release: 0.10,
      });
      cursor += 0.12;
    });

    return cursor + 0.12;
  }

  function playSignalAlarmSequence(sequence = [], options = {}) {
    const normalizedSequence = Array.isArray(sequence)
      ? sequence.filter(Boolean)
      : [];

    if (!config.signalAlarmEnabled || !normalizedSequence.length) {
      return Promise.resolve(false);
    }

    const requestedVolume = Number.isFinite(Number(options.volume))
      ? clamp(Number(options.volume), 0, 1)
      : getSignalAlarmVolume();

    signalAlarmQueue = signalAlarmQueue
      .catch(() => {})
      .then(async () => {
        const ctx = await unlockSignalAudioContext();
        const master = ctx.createGain();
        master.gain.value = requestedVolume;
        master.connect(ctx.destination);

        let cursor = ctx.currentTime + 0.02;

        for (const item of normalizedSequence) {
          if (item === 'sell') {
            cursor = scheduleSellAlarm(ctx, master, cursor);
          } else if (item === 'excited_buy') {
            cursor = scheduleExcitedBuyAlarm(ctx, master, cursor);
          } else {
            cursor = scheduleHappyBuyAlarm(ctx, master, cursor);
          }
          cursor += 0.05;
        }

        const waitMs = Math.max(120, Math.ceil((cursor - ctx.currentTime) * 1000));

        try {
          await sleep(waitMs);
        } finally {
          try {
            master.disconnect();
          } catch (_) {}
        }

        return true;
      });

    return signalAlarmQueue;
  }

  async function handleSignalAlarmButtonClick() {
    if (!config.signalAlarmEnabled) {
      await enableSignalAlarm();
      return;
    }

    await openSignalAlarmSettingsPopup();
  }

  async function enableSignalAlarm() {
    await unlockSignalAudioContext();

    config.signalAlarmEnabled = true;
    saveConfig();
    log('آلارم هنگام خرید/فروش روشن شد.', 'ok');

    try {
      await playSignalAlarmSequence(['buy']);
    } catch (err) {
      console.error('[معامله‌گر ال‌بانک] signal alarm preview failed', err);
    }
  }

  function buildSignalAlarmSettingsBody() {
    const volumePercent = Math.round(getSignalAlarmVolume() * 100);

    return `
      <div class="lbk-summary">
        آلارم هنگام خرید/فروش روشن است. با تغییر حجم، صدای خرید و فروش بعدی با همین مقدار پخش می‌شود.
      </div>

      <label for="lbk-signal-alarm-volume">حجم آلارم</label>
      <input id="lbk-signal-alarm-volume" type="range" min="0" max="100" step="1" value="${volumePercent}" />
      <div id="lbk-signal-alarm-volume-text" class="lbk-note" style="display:block;">حجم فعلی: ${formatFaNumber(volumePercent)}٪</div>

      <div class="lbk-inline-actions">
        <button type="button" id="lbk-signal-alarm-preview">پخش نمونه</button>
        <button type="button" id="lbk-signal-alarm-disable">خاموش کردن آلارم</button>
      </div>
    `;
  }

  async function openSignalAlarmSettingsPopup() {
    const initialVolumePercent = Math.round(getSignalAlarmVolume() * 100);

    await showPopupModal({
      title: 'تنظیم آلارم هنگام خرید/فروش',
      bodyHtml: buildSignalAlarmSettingsBody(),
      submitText: 'ذخیره',
      cancelText: 'بستن',
      onMount: ({ overlay, close }) => {
        const volumeInput = overlay.querySelector('#lbk-signal-alarm-volume');
        const volumeText = overlay.querySelector('#lbk-signal-alarm-volume-text');
        const previewBtn = overlay.querySelector('#lbk-signal-alarm-preview');
        const disableBtn = overlay.querySelector('#lbk-signal-alarm-disable');
        let liveVolumePercent = initialVolumePercent;

        const syncVolumeText = () => {
          if (volumeText) {
            volumeText.textContent = `حجم فعلی: ${formatFaNumber(liveVolumePercent)}٪`;
          }
        };

        if (volumeInput) {
          volumeInput.addEventListener('input', () => {
            liveVolumePercent = clamp(Number(volumeInput.value) || 0, 0, 100);
            syncVolumeText();
          });
        }

        if (previewBtn) {
          previewBtn.addEventListener('click', async () => {
            previewBtn.disabled = true;
            try {
              await playSignalAlarmSequence(['sell', 'buy'], {
                volume: liveVolumePercent / 100,
              });
            } catch (err) {
              alert(String(err?.message || err || 'پخش نمونه انجام نشد.'));
            } finally {
              previewBtn.disabled = false;
            }
          });
        }

        if (disableBtn) {
          disableBtn.addEventListener('click', () => {
            config.signalAlarmEnabled = false;
            saveConfig();
            log('آلارم هنگام خرید/فروش غیرفعال شد.', 'info');
            close(true);
          });
        }

        syncVolumeText();
      },
      onSubmit: async ({ overlay }) => {
        const volumeInput = overlay.querySelector('#lbk-signal-alarm-volume');
        const volumePercent = clamp(Number(volumeInput?.value) || 0, 0, 100);

        config.signalAlarmVolume = volumePercent / 100;
        saveConfig();
        log(`حجم آلارم هنگام خرید/فروش روی ${formatFaNumber(volumePercent)}٪ ذخیره شد.`, 'info');
        return true;
      },
    });
  }

  async function playNewSignalAlarm(newSellSignals = [], newBuySignals = []) {
    if (!config.signalAlarmEnabled) return false;

    const safeSellSignals = Array.isArray(newSellSignals) ? newSellSignals : [];
    const safeBuySignals = Array.isArray(newBuySignals) ? newBuySignals : [];

    const hasSell = safeSellSignals.length > 0;
    const hasBuy = safeBuySignals.length > 0;
    if (!hasSell && !hasBuy) return false;

    const hasExcitedBuy = safeBuySignals.some(signal => String(signal?.signalFlavor || '') === 'pump_buy');
    const sequence = [];

    if (hasSell) sequence.push('sell');
    if (hasBuy) sequence.push(hasExcitedBuy ? 'excited_buy' : 'buy');

    if (!sequence.length) return false;

    try {
      await playSignalAlarmSequence(sequence);
      return true;
    } catch (err) {
      console.error('[معامله‌گر ال‌بانک] signal alarm playback failed', err);
      return false;
    }
  }

  function renderBalances() {
    const rows = state.lastBalanceSummary || [];
    if (!rows.length) {
      ui.balancesEl.textContent = 'هنوز موجودی‌ای خوانده نشده یا همه صفر هستند.';
      return;
    }

    const text = rows
      .slice(0, 12)
      .map(row => `• ${row.currency}: ${row.balance}`)
      .join('\n');

    ui.balancesEl.textContent = text;
  }

  function renderCurrentList() {
    const out = [];

    const radarTs = state.latestRadar?.timestampText || 'ندارد';
    const strategyTs = state.latestStrategy?.timestampText || 'ندارد';

    out.push(`رادار مرجع: ${radarTs}`);
    out.push(`استراتژی مرجع: ${strategyTs}`);
    out.push('');

    const buys = state.currentPriorityList || [];
    if (buys.length) {
      out.push('خریدهای فعلی به‌ترتیب اولویت:');
      buys.slice(0, 12).forEach((item, idx) => {
        const pos = getExistingPositionText(item.base, item.quote);
        const flags = `${item.star ? '⭐' : ''}${item.red ? '🔴' : ''}` || '—';
        const streak = item.streak > 1 ? `x${item.streak}` : 'x1';
        const q = quoteLabelFa(item.quote);
        out.push(`${idx + 1}) ${item.base} (${q}) | ${flags} | ${streak}${pos ? ` | ${pos}` : ''}`);
      });
    } else {
      out.push('خرید فعالی در آخرین پیام استراتژی پیدا نشد.');
    }

    out.push('');
    const sells = state.currentSellList || [];
    if (sells.length) {
      out.push('فروش‌های موجود در آخرین پیام استراتژی:');
      sells.slice(0, 12).forEach((item, idx) => {
        const q = quoteLabelFa(item.quote);
        out.push(`${idx + 1}) ${item.base} (${q})`);
      });
    } else {
      out.push('فروش فعالی در آخرین پیام استراتژی پیدا نشد.');
    }

    const ignored = state.currentIgnoredBuys || [];
    if (ignored.length) {
      out.push('');
      out.push('خریدهای نادیده‌گرفته‌شده:');
      ignored.slice(0, 8).forEach((item, idx) => {
        out.push(`${idx + 1}) ${item.base} | علت: ${item.reason}`);
      });
    }

    ui.currentListEl.textContent = out.join('\n');
  }

  function renderPositions() {
    const openPositions = listOpenPositions()
      .sort((a, b) => (a.openedAt || 0) - (b.openedAt || 0));

    if (!openPositions.length) {
      ui.positionsEl.textContent = 'هیچ پوزیشن بازی هنوز ثبت نشده است.';
      return;
    }

    const lines = [];
    const now = Date.now();
    const totals = {};

    openPositions.forEach((p, idx) => {
      const quoteLabel = quoteLabelFa(p.quote);
      const avgText = num(p.avgEntryPrice) > 0 ? formatPrice(p.avgEntryPrice, p.quote) : 'نامشخص';
      const lastPriceText = num(p.lastKnownPrice) > 0 ? formatPrice(p.lastKnownPrice, p.quote) : '—';
      const costKnown = num(p.entryCostQuote) > 0;
      const pnlKnown = costKnown && num(p.lastPnlAt) > 0;
      const pnlPercentText = pnlKnown ? formatSignedPercent(p.lastPnlPercent) : 'نامشخص';
      const forceText = forceSellStatusText(p, now);

      if (pnlKnown) {
        if (!totals[p.quote]) {
          totals[p.quote] = { cost: 0, pnl: 0 };
        }
        totals[p.quote].cost += Math.max(0, num(p.entryCostQuote));
        totals[p.quote].pnl += num(p.lastPnlValue);
      }

      lines.push(
        `${idx + 1}) ${p.base} (${quoteLabel}) | حساب: ${accountLabel(p.accountId || 'primary')}` +
        ` | مقدار: ${formatAmount(num(p.qty))}` +
        ` | میانگین ورود: ${avgText}` +
        ` | قیمت فعلی: ${lastPriceText}` +
        ` | سود/زیان: ${pnlPercentText}` +
        ` | فروش اجباری: ${forceText}`
      );
    });

    const summaryParts = Object.entries(totals)
      .filter(([, bucket]) => num(bucket?.cost) > 0)
      .map(([quote, bucket]) => {
        const totalPercent = (num(bucket.pnl) / num(bucket.cost)) * 100;
        return { quote, text: formatSignedPercent(totalPercent) };
      });

    let summaryText = 'مجموع سود/زیان: نامشخص';
    if (summaryParts.length === 1) {
      summaryText = `مجموع سود/زیان: ${summaryParts[0].text}`;
    } else if (summaryParts.length > 1) {
      summaryText = summaryParts
        .map(item => `مجموع سود/زیان ${quoteLabelFa(item.quote)}: ${item.text}`)
        .join(' | ');
    }

    ui.positionsEl.textContent = `${summaryText}\n\n${lines.join('\n')}`;
  }

  function forceSellStatusText(position, now = Date.now()) {
    if (!position || !(num(position.openedAt) > 0) || position.forceSellSkipReason) return 'غیرفعال';

    const reason = forceSellReasonLabel(position, now);
    if (!reason) return 'غیرفعال';

    if (reason === 'حد ضرر PnL') {
      return `فعال (${reason}: ${formatSignedPercent(num(position.lastPnlPercent))})`;
    }

    return `فعال (${reason})`;
  }

  function getExistingPositionText(base, quote) {
    const matches = findOpenPositionsForMarket(base, quote);
    if (!matches.length) return '';

    const now = Date.now();
    if (matches.length > 1) {
      return `در پوزیشن در ${formatFaNumber(matches.length)} حساب`;
    }

    const p = matches[0];
    if (!p.openedAt) return `در پوزیشن | ${accountLabel(p.accountId || 'primary')} | فروش اجباری: غیرفعال`;

    const reason = forceSellReasonLabel(p, now);
    if (reason === 'حد ضرر PnL') {
      return `در پوزیشن | ${accountLabel(p.accountId || 'primary')} | فروش اجباری: فعال (${reason}: ${formatSignedPercent(num(p.lastPnlPercent))})`;
    }
    if (reason === 'مهلت نگه‌داری') {
      return `در پوزیشن | ${accountLabel(p.accountId || 'primary')} | فروش اجباری: فعال (مهلت نگه‌داری)`;
    }

    const remain = (p.openedAt + HOLD_MAX_MS) - now;
    const stopLossBaseTime = forceSellMinAgeBaseTime(p);
    const stopLossRemain = stopLossBaseTime > 0 ? (stopLossBaseTime + FORCE_SELL_MIN_AGE_MS) - now : FORCE_SELL_MIN_AGE_MS;

    return `در پوزیشن | ${accountLabel(p.accountId || 'primary')} | مانده تا فروش اجباری ۴ساعته: ${formatRemaining(remain)} | مانده تا فعال‌شدن حد ضرر: ${formatRemaining(stopLossRemain)}`;
  }

  function log(msg, level = 'info') {
    const line = document.createElement('div');
    line.className = level;
    line.textContent = `[${timeNowFa()}] ${msg}`;
    ui.logEl.prepend(line);

    while (ui.logEl.children.length > 140) {
      ui.logEl.removeChild(ui.logEl.lastChild);
    }

    try {
      if (typeof GM_notification === 'function' && (level === 'err' || level === 'warn')) {
        GM_notification({
          title: 'معامله‌گر ال‌بانک',
          text: msg,
          timeout: 3500
        });
      }
    } catch (_) {}

    console[level === 'err' ? 'error' : 'log'](`[معامله‌گر ال‌بانک] ${msg}`);
  }

  function logError(err, where) {
    const message = err && err.message ? err.message : String(err);
    log(`${where}: ${message}`, 'err');
  }

  /******************************************************************
   * ذخیره‌سازی
   ******************************************************************/
  function loadJSON(key, fallback) {
    try {
      const raw = GM_getValue(key, '');
      if (!raw) return deepClone(fallback);
      const parsed = typeof raw === 'object' ? raw : JSON.parse(raw);
      return mergeDeep(deepClone(fallback), parsed);
    } catch (_) {
      return deepClone(fallback);
    }
  }

  function saveJSON(key, value) {
    GM_setValue(key, JSON.stringify(value));
  }

  function saveConfig() {
    saveJSON(STORAGE_CONFIG, config);
    refreshUi();
  }

  function saveState() {
    state.lastSavedAtMs = Date.now();
    state.lastSavedAtIso = new Date(state.lastSavedAtMs).toISOString();
    saveJSON(STORAGE_STATE, state);
    refreshUi();
  }

  function deepClone(obj) {
    return JSON.parse(JSON.stringify(obj));
  }

  function mergeDeep(base, patch) {
    if (!patch || typeof patch !== 'object' || Array.isArray(patch)) return base;
    const out = Array.isArray(base) ? [...base] : { ...base };

    for (const [k, v] of Object.entries(patch)) {
      if (
        v &&
        typeof v === 'object' &&
        !Array.isArray(v) &&
        base &&
        typeof base[k] === 'object' &&
        base[k] !== null &&
        !Array.isArray(base[k])
      ) {
        out[k] = mergeDeep(base[k], v);
      } else {
        out[k] = v;
      }
    }
    return out;
  }



function normalizeLbankSignatureMethod(value) {
  const raw = String(value || '').trim().toLowerCase().replace(/[_\s-]+/g, '');
  if (raw === 'rsa' || raw === 'rsasha256' || raw === 'sha256withrsa') return LBANK_SIGNATURE_RSA;
  if (raw === 'hmac' || raw === 'hmacsha256' || raw === 'hmachashsha256') return LBANK_SIGNATURE_HMAC;
  return LBANK_SIGNATURE_HMAC;
}

function lbankSignatureMethodLabel(method) {
  return normalizeLbankSignatureMethod(method) === LBANK_SIGNATURE_RSA ? 'RSA' : 'HmacSHA256';
}

function lbankSecretLabel(method) {
  return normalizeLbankSignatureMethod(method) === LBANK_SIGNATURE_RSA ? 'RSA Private Key' : 'Secret Key';
}

function parseLbankSignatureMethodChoice(value, fallback = LBANK_SIGNATURE_HMAC) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return normalizeLbankSignatureMethod(fallback);
  if (raw === '1' || raw === 'h' || raw === 'hmac' || raw === 'hmacsha256' || raw.includes('hmac')) return LBANK_SIGNATURE_HMAC;
  if (raw === '2' || raw === 'r' || raw === 'rsa' || raw.includes('rsa')) return LBANK_SIGNATURE_RSA;
  return null;
}

function normalizeLbankPrivateKeyMaterial(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  return raw
    .replace(/-----BEGIN [^-]+-----/g, '')
    .replace(/-----END [^-]+-----/g, '')
    .replace(/\s+/g, '');
}

function base64ToArrayBuffer(base64) {
  const normalized = String(base64 || '').replace(/-/g, '+').replace(/_/g, '/');
  const pad = normalized.length % 4 ? '='.repeat(4 - (normalized.length % 4)) : '';
  const bin = atob(normalized + pad);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes.buffer;
}

function normalizeSubTokenEntry(raw = {}, index = 0) {
  const now = Date.now();
  return {
    id: String(raw.id || `sub_${now.toString(36)}_${index}_${Math.random().toString(36).slice(2, 7)}`),
    token: String(raw.token || raw.apiKey || '').trim(),
    secretKey: String(raw.secretKey || raw.secret || raw.privateKey || '').trim(),
    signatureMethod: normalizeLbankSignatureMethod(raw.signatureMethod || raw.signMethod || raw.verificationMethod),
    active: raw.active !== false,
    allowPumpBuys: !!raw.allowPumpBuys,
    buyOnlyUsdtMarket: true,
    backupWhenPrimaryLowUsdt: !!raw.backupWhenPrimaryLowUsdt,
    createdAt: Math.max(0, num(raw.createdAt)) || now,
    updatedAt: Math.max(0, num(raw.updatedAt)) || now,
  };
}

  function normalizeConfigCollections() {
    config.signatureMethod = normalizeLbankSignatureMethod(config.signatureMethod);
    if (String(config.targetChatName || '').trim().toLowerCase() !== '1bankbot') {
      config.targetChatName = '1bankbot';
    }
    if (!Array.isArray(config.subTokens)) {
      config.subTokens = [];
    }
    config.subTokens = config.subTokens.map((item, index) => normalizeSubTokenEntry(item, index));
  }

  function getSubTokens(includeInactive = true) {
    normalizeConfigCollections();
    const all = Array.isArray(config.subTokens) ? config.subTokens : [];
    return includeInactive ? all : all.filter(item => item.active !== false);
  }


function hasAnyConfiguredAccountToken() {
  if (String(config.token || '').trim() && String(config.secretKey || '').trim()) return true;
  return getSubTokens(false).some(item => String(item.token || '').trim() && String(item.secretKey || '').trim());
}

  function getSubTokenById(id, options = {}) {
    const items = getSubTokens(options.includeInactive !== false);
    return items.find(item => item.id === String(id || '')) || null;
  }


function accountLabel(accountOrId = 'primary') {
  const id = typeof accountOrId === 'object'
    ? String(accountOrId?.id || 'primary')
    : String(accountOrId || 'primary');

  if (id === 'primary') return 'API اصلی';

  const idx = getSubTokens(true).findIndex(item => item.id === id);
  return idx >= 0 ? `API فرعی ${formatFaNumber(idx + 1)}` : 'API فرعی';
}


function getAccountContext(accountId = 'primary', options = {}) {
  const id = String(accountId || 'primary');
  if (id === 'primary') {
    return {
      id: 'primary',
      kind: 'primary',
      token: String(config.token || '').trim(),
      secretKey: String(config.secretKey || '').trim(),
      signatureMethod: normalizeLbankSignatureMethod(config.signatureMethod),
      active: true,
      allowPumpBuys: !!config.allowPumpBuys,
      sellLowGainHoldingsForPumpBuys: !!config.sellLowGainHoldingsForPumpBuys,
      buyOnlyUsdtMarket: true,
      backupWhenPrimaryLowUsdt: false,
    };
  }

  const sub = getSubTokenById(id, { includeInactive: options.includeInactive !== false });
  if (!sub) {
    return {
      id,
      kind: 'sub',
      token: '',
      secretKey: '',
      signatureMethod: LBANK_SIGNATURE_HMAC,
      active: false,
      allowPumpBuys: false,
      sellLowGainHoldingsForPumpBuys: false,
      buyOnlyUsdtMarket: true,
      backupWhenPrimaryLowUsdt: false,
    };
  }

  return {
    id: sub.id,
    kind: 'sub',
    token: String(sub.token || '').trim(),
    secretKey: String(sub.secretKey || '').trim(),
    signatureMethod: normalizeLbankSignatureMethod(sub.signatureMethod),
    active: sub.active !== false,
    allowPumpBuys: !!sub.allowPumpBuys,
    sellLowGainHoldingsForPumpBuys: false,
    buyOnlyUsdtMarket: true,
    backupWhenPrimaryLowUsdt: !!sub.backupWhenPrimaryLowUsdt,
  };
}

  function getActiveSecondaryAccounts() {
    return getSubTokens(false)
      .filter(item => String(item.token || '').trim())
      .map(item => getAccountContext(item.id, { includeInactive: false }))
      .filter(Boolean);
  }


function maskTokenForDisplay(token) {
  const t = String(token || '').trim();
  if (!t) return 'ثبت نشده';
  if (t.length <= 10) return `${t.slice(0, 2)}…${t.slice(-2)}`;
  return `${t.slice(0, 6)}…${t.slice(-4)}`;
}


function describeSubTokenFlags(entry) {
  const flags = [];
  flags.push(`روش امضا: ${lbankSignatureMethodLabel(entry?.signatureMethod)}`);
  if (entry.allowPumpBuys) flags.push('خرید پامپ');
  if (entry.backupWhenPrimaryLowUsdt) flags.push('پشتیبان کمبود موجودی دلاری');
  flags.push('بازار دلاری');
  return flags;
}

  function hasAnyActiveSubTokenAllowingPumpBuys() {
    return getActiveSecondaryAccounts().some(item => item.allowPumpBuys);
  }

  function hasAnyActiveSubTokenUsdtFallbackForSignal(signalPatch = {}) {
    const isPump = String(signalPatch?.signalFlavor || '') === 'pump_buy';
    return getActiveSecondaryAccounts().some(item => {
      if (isPump && !item.allowPumpBuys) return false;
      return item.buyOnlyUsdtMarket || item.backupWhenPrimaryLowUsdt;
    });
  }


function hasAnyActiveSubTokenLegacyFallbackForSignal(signalPatch = {}) {
  return false;
}


function supportsSignalOnAccount(account, signal, { forBackup = false } = {}) {
  if (!account || !account.active || !account.token || !account.secretKey) return false;
  if (!isUsdLikeQuoteCode(normalizeQuoteCode(signal?.quote))) return false;
  if (isPumpBuySignal(signal) && !account.allowPumpBuys) return false;
  if (forBackup && !account.backupWhenPrimaryLowUsdt) return false;
  return true;
}

  function normalizeStateEntryAccountId(entry) {
    if (!entry || typeof entry !== 'object') return 'primary';
    return String(entry.accountId || 'primary');
  }

  function migrateLegacyAccountScopedState() {
    const migratedPositions = {};
    for (const [key, value] of Object.entries(state.positions || {})) {
      const entry = value && typeof value === 'object' ? { ...value } : {};
      const accountId = normalizeStateEntryAccountId(entry);
      const [legacyBase = '', legacyQuote = ''] = String(key || '').split('|');
      const base = String(entry.base || legacyBase || '').toUpperCase();
      const quote = String(entry.quote || legacyQuote || '').toLowerCase();
      if (!base || !quote) continue;
      entry.base = base;
      entry.quote = quote;
      entry.accountId = accountId;
      migratedPositions[posKey(base, quote, accountId)] = entry;
    }
    state.positions = migratedPositions;

    const migratedRetryBuys = {};
    for (const [key, value] of Object.entries(state.retryBuys || {})) {
      const entry = value && typeof value === 'object' ? { ...value } : {};
      const accountId = normalizeStateEntryAccountId(entry);
      const [legacyBase = '', legacyQuote = ''] = String(key || '').split('|');
      const base = String(entry.base || legacyBase || '').toUpperCase();
      const quote = String(entry.quote || legacyQuote || '').toLowerCase();
      if (!base || !quote) continue;
      entry.base = base;
      entry.quote = quote;
      entry.accountId = accountId;
      migratedRetryBuys[retryBuyKey(base, quote, accountId)] = entry;
    }
    state.retryBuys = migratedRetryBuys;

    for (const [orderId, pending] of Object.entries(state.pending || {})) {
      if (!pending || typeof pending !== 'object') continue;
      state.pending[orderId] = {
        ...pending,
        base: String(pending.base || '').toUpperCase(),
        quote: String(pending.quote || '').toLowerCase(),
        accountId: normalizeStateEntryAccountId(pending),
      };
    }
  }

  function countAccountReferences(accountId) {
    const id = String(accountId || '');
    let positions = 0;
    let pending = 0;
    let retryBuys = 0;

    for (const position of Object.values(state.positions || {})) {
      if (normalizeStateEntryAccountId(position) !== id) continue;
      if (Math.abs(num(position?.qty)) > 0) positions += 1;
    }

    for (const row of Object.values(state.pending || {})) {
      if (normalizeStateEntryAccountId(row) !== id) continue;
      pending += 1;
    }

    for (const row of Object.values(state.retryBuys || {})) {
      if (normalizeStateEntryAccountId(row) !== id) continue;
      retryBuys += 1;
    }

    return {
      positions,
      pending,
      retryBuys,
      total: positions + pending + retryBuys,
    };
  }


function buildSubTokenFormBody(entry = null) {
  const isEdit = !!entry;
  const apiKeyValue = entry ? String(entry.token || '') : '';
  const secretValue = entry ? String(entry.secretKey || '') : '';
  const methodValue = normalizeLbankSignatureMethod(entry?.signatureMethod);
  const activeChecked = !entry || entry.active !== false ? 'checked' : '';
  const pumpChecked = entry?.allowPumpBuys ? 'checked' : '';
  const backupUsdtChecked = entry?.backupWhenPrimaryLowUsdt ? 'checked' : '';

  return `
    <div class="lbk-summary">
      ${isEdit ? 'ویرایش API فرعی ال‌بانک' : 'اضافه کردن API فرعی ال‌بانک'}
    </div>

    <label for="lbk-subtoken-signature-method">روش امضای API</label>
    <select id="lbk-subtoken-signature-method">
      <option value="${LBANK_SIGNATURE_HMAC}" ${methodValue === LBANK_SIGNATURE_HMAC ? 'selected' : ''}>HmacSHA256</option>
      <option value="${LBANK_SIGNATURE_RSA}" ${methodValue === LBANK_SIGNATURE_RSA ? 'selected' : ''}>RSA</option>
    </select>

    <label for="lbk-subtoken-token">LBank API Key</label>
    <input id="lbk-subtoken-token" type="text" dir="ltr" autocomplete="off" value="${escapeHtml(apiKeyValue)}" placeholder="API Key" />

    <label id="lbk-subtoken-secret-label" for="lbk-subtoken-secret">${methodValue === LBANK_SIGNATURE_RSA ? 'RSA Private Key' : 'Secret Key'}</label>
    <textarea id="lbk-subtoken-secret" autocomplete="off" rows="5" placeholder="${methodValue === LBANK_SIGNATURE_RSA ? 'RSA Private Key' : 'Secret Key'}">${escapeHtml(secretValue)}</textarea>

    <label class="lbk-inline-option">
      <input id="lbk-subtoken-active" type="checkbox" ${activeChecked} />
      فعال باشد
    </label>

    <label class="lbk-inline-option">
      <input id="lbk-subtoken-pump" type="checkbox" ${pumpChecked} />
      اجازه خرید هنگام پامپ با این API
    </label>

    <label class="lbk-inline-option">
      <input id="lbk-subtoken-backup-usdt" type="checkbox" ${backupUsdtChecked} />
      اگر موجودی دلاری حساب اصلی کم بود، این API به‌عنوان پشتیبان استفاده شود
    </label>

    <div class="lbk-note" style="display:block;">
      برای LBank باید روش امضا را برای هر API جداگانه انتخاب کنی. در روش HmacSHA256 مقدار Secret Key را وارد کن؛ در روش RSA کلید خصوصی RSA همان API را وارد کن.
    </div>
  `;
}

function bindSubTokenFormCheckboxes(overlay) {
  const methodSelect = overlay.querySelector('#lbk-subtoken-signature-method');
  const secretLabel = overlay.querySelector('#lbk-subtoken-secret-label');
  const secretInput = overlay.querySelector('#lbk-subtoken-secret');

  const syncSecretLabel = () => {
    const method = normalizeLbankSignatureMethod(methodSelect?.value);
    const label = lbankSecretLabel(method);
    if (secretLabel) secretLabel.textContent = label;
    if (secretInput) secretInput.placeholder = label;
  };

  methodSelect?.addEventListener('change', syncSecretLabel);
  syncSecretLabel();
}


async function openSubTokenFormPopup(editId = '') {
  const entry = editId ? getSubTokenById(editId, { includeInactive: true }) : null;
  if (editId && !entry) {
    log('API فرعی پیدا نشد.', 'warn');
    return;
  }

  await showPopupModal({
    title: entry ? 'ویرایش API فرعی ال‌بانک' : 'اضافه کردن API فرعی ال‌بانک',
    bodyHtml: buildSubTokenFormBody(entry),
    submitText: 'ذخیره و بررسی اتصال',
    cancelText: 'بستن',
    onMount: ({ overlay }) => bindSubTokenFormCheckboxes(overlay),
    onSubmit: async ({ overlay, submitBtn }) => {
      const signatureMethod = normalizeLbankSignatureMethod(overlay.querySelector('#lbk-subtoken-signature-method')?.value);
      const token = String(overlay.querySelector('#lbk-subtoken-token')?.value || '').trim();
      const secretKey = String(overlay.querySelector('#lbk-subtoken-secret')?.value || '').trim();
      if (!token || !secretKey) {
        alert('API Key و Secret Key ال‌بانک را کامل وارد کن.');
        return false;
      }

      const patch = {
        id: entry?.id,
        token,
        secretKey,
        signatureMethod,
        active: !!overlay.querySelector('#lbk-subtoken-active')?.checked,
        allowPumpBuys: !!overlay.querySelector('#lbk-subtoken-pump')?.checked,
        buyOnlyUsdtMarket: true,
        backupWhenPrimaryLowUsdt: !!overlay.querySelector('#lbk-subtoken-backup-usdt')?.checked,
        createdAt: entry?.createdAt || Date.now(),
        updatedAt: Date.now(),
      };

      submitBtn.disabled = true;
      submitBtn.textContent = 'در حال بررسی اتصال...';

      try {
        await getWallets({ token, secretKey, signatureMethod, accountId: entry?.id || 'new_subtoken', skipSummary: true });
      } catch (err) {
        alert(`اتصال API فرعی ال‌بانک موفق نبود:\n${String(err?.message || err)}`);
        submitBtn.disabled = false;
        submitBtn.textContent = 'ذخیره و بررسی اتصال';
        return false;
      }

      const normalized = normalizeSubTokenEntry(patch, config.subTokens.length);
      if (entry) {
        const idx = config.subTokens.findIndex(item => item.id === entry.id);
        if (idx >= 0) config.subTokens[idx] = normalized;
      } else {
        config.subTokens.push(normalized);
      }

      saveConfig();
      refreshUi();
      log(entry ? 'API فرعی ال‌بانک به‌روزرسانی شد.' : 'API فرعی ال‌بانک اضافه شد.', 'ok');
      return true;
    },
  });
}

  function buildSubTokenListHtml() {
    const items = getSubTokens(true);

    if (!items.length) {
      return '<div class="lbk-note">هنوز API فرعی‌ای ثبت نشده است.</div>';
    }

    return items.map(item => {
      const flags = describeSubTokenFlags(item);
      const refs = countAccountReferences(item.id);
      return `
        <div class="lbk-sell-item">
          <div style="display:flex; justify-content:space-between; gap:10px; align-items:flex-start; flex-wrap:wrap;">
            <div>
              <div style="font-weight:700;">${escapeHtml(accountLabel(item.id))} | ${item.active ? 'فعال' : 'غیرفعال'}</div>
              <div style="margin-top:4px;">${escapeHtml(maskTokenForDisplay(item.token))}</div>
              <small>${flags.length ? escapeHtml(flags.join(' | ')) : 'بدون گزینهٔ ویژه'}</small>
              <small>ارجاعات فعلی: پوزیشن ${formatFaNumber(refs.positions)} | سفارش ${formatFaNumber(refs.pending)} | retry ${formatFaNumber(refs.retryBuys)}</small>
            </div>
            <div style="display:flex; gap:6px; flex-wrap:wrap;">
              <button type="button" data-lbk-subtoken-action="edit" data-lbk-subtoken-id="${escapeHtml(item.id)}">ویرایش</button>
              <button type="button" data-lbk-subtoken-action="toggle" data-lbk-subtoken-id="${escapeHtml(item.id)}">${item.active ? 'غیرفعال' : 'فعال'}</button>
              <button type="button" data-lbk-subtoken-action="delete" data-lbk-subtoken-id="${escapeHtml(item.id)}">حذف</button>
            </div>
          </div>
        </div>
      `;
    }).join('');
  }

  function bindSubTokenListActions(overlay) {
    const wrap = overlay.querySelector('#lbk-subtoken-list-wrap');
    if (!wrap) return;

    const refresh = () => {
      wrap.innerHTML = buildSubTokenListHtml();
      bindSubTokenListActions(overlay);
    };

    wrap.querySelectorAll('[data-lbk-subtoken-action]').forEach(button => {
      button.addEventListener('click', async () => {
        const action = String(button.getAttribute('data-lbk-subtoken-action') || '');
        const id = String(button.getAttribute('data-lbk-subtoken-id') || '');
        const item = getSubTokenById(id, { includeInactive: true });
        if (!item) {
          alert('API فرعی پیدا نشد.');
          refresh();
          return;
        }

        if (action === 'edit') {
          await openSubTokenFormPopup(id);
          refresh();
          return;
        }

        if (action === 'toggle') {
          config.subTokens = getSubTokens(true).map(entry => entry.id === id ? {
            ...entry,
            active: !(entry.active !== false),
            updatedAt: Date.now(),
          } : entry);
          saveConfig();
          log(`${accountLabel(id)} ${item.active ? 'غیرفعال' : 'فعال'} شد.`, 'info');
          refresh();
          return;
        }

        if (action === 'delete') {
          const refs = countAccountReferences(id);
          if (refs.total > 0) {
            alert('این API فرعی هنوز در پوزیشن‌ها، سفارش‌های درحال‌پیگیری یا retry استفاده شده است و فعلاً حذف نمی‌شود. ابتدا آن‌ها را ببند یا این توکن را فقط غیرفعال کن.');
            refresh();
            return;
          }

          if (!confirm(`آیا ${accountLabel(id)} حذف شود؟`)) {
            return;
          }

          config.subTokens = getSubTokens(true).filter(entry => entry.id !== id);
          saveConfig();
          log(`${accountLabel(id)} حذف شد.`, 'warn');
          refresh();
        }
      });
    });
  }

  async function openSubTokenListPopup() {
    await showPopupModal({
      title: 'لیست توکن های فرعی',
      bodyHtml: '<div id="lbk-subtoken-list-wrap"></div>',
      submitText: 'بستن',
      cancelText: 'بستن',
      onMount: ({ overlay, cancelBtn }) => {
        cancelBtn.style.display = 'none';
        const wrap = overlay.querySelector('#lbk-subtoken-list-wrap');
        if (wrap) {
          wrap.innerHTML = buildSubTokenListHtml();
          bindSubTokenListActions(overlay);
        }
      },
      onSubmit: () => true,
    });
  }

  /******************************************************************
   * راه‌اندازی / فرمان‌ها
   ******************************************************************/

async function setupFlow() {
  const currentMethod = normalizeLbankSignatureMethod(config.signatureMethod);

  const result = await showPopupModal({
    title: 'تنظیم API اصلی ال‌بانک',
    bodyHtml: `
      <div class="lbk-summary">
        ابتدا روش امضای API را انتخاب کن و سپس اطلاعات همان روش را وارد کن.
      </div>

      <label for="lbk-main-signature-method">روش امضای API</label>
      <select id="lbk-main-signature-method">
        <option value="${LBANK_SIGNATURE_HMAC}" ${currentMethod === LBANK_SIGNATURE_HMAC ? 'selected' : ''}>HmacSHA256</option>
        <option value="${LBANK_SIGNATURE_RSA}" ${currentMethod === LBANK_SIGNATURE_RSA ? 'selected' : ''}>RSA</option>
      </select>

      <label for="lbk-main-api-key">LBank API Key</label>
      <input id="lbk-main-api-key" type="text" dir="ltr" autocomplete="off" value="${escapeHtml(config.token || '')}" placeholder="API Key" />

      <label id="lbk-main-secret-label" for="lbk-main-secret">${currentMethod === LBANK_SIGNATURE_RSA ? 'RSA Private Key' : 'Secret Key'}</label>
      <textarea id="lbk-main-secret" autocomplete="off" placeholder="${currentMethod === LBANK_SIGNATURE_RSA ? 'RSA Private Key' : 'Secret Key'}">${escapeHtml(config.secretKey || '')}</textarea>

      <label class="lbk-inline-option">
        <input id="lbk-main-pump" type="checkbox" ${config.allowPumpBuys ? 'checked' : ''} />
        خریدهای هنگام پامپ فعال شوند
      </label>

      <label class="lbk-inline-option">
        <input id="lbk-main-sell-low-gain" type="checkbox" ${config.sellLowGainHoldingsForPumpBuys ? 'checked' : ''} />
        اگر خرید هنگام پامپ آمد و موجودی دلاری API اصلی کافی نبود، پوزیشن‌های با سود کمتر از ۱٪ برای تأمین خرید قابل فروش باشند
      </label>
    `,
    submitText: 'ذخیره و بررسی اتصال',
    cancelText: 'بستن',
    onMount: ({ overlay }) => {
      const methodSelect = overlay.querySelector('#lbk-main-signature-method');
      const secretLabel = overlay.querySelector('#lbk-main-secret-label');
      const secretInput = overlay.querySelector('#lbk-main-secret');
      const syncSecretLabel = () => {
        const method = normalizeLbankSignatureMethod(methodSelect?.value);
        const label = lbankSecretLabel(method);
        if (secretLabel) secretLabel.textContent = label;
        if (secretInput) secretInput.placeholder = label;
      };
      methodSelect?.addEventListener('change', syncSecretLabel);
      syncSecretLabel();
    },
    onSubmit: async ({ overlay, submitBtn }) => {
      const signatureMethod = normalizeLbankSignatureMethod(overlay.querySelector('#lbk-main-signature-method')?.value);
      const token = String(overlay.querySelector('#lbk-main-api-key')?.value || '').trim();
      const secretKey = String(overlay.querySelector('#lbk-main-secret')?.value || '').trim();
      const allowPumpBuys = !!overlay.querySelector('#lbk-main-pump')?.checked;
      const sellLowGainHoldingsForPumpBuys = !!overlay.querySelector('#lbk-main-sell-low-gain')?.checked;

      if (!token) throw new Error('API Key ال‌بانک را وارد کن.');
      if (!secretKey) throw new Error(`${lbankSecretLabel(signatureMethod)} ال‌بانک را وارد کن.`);

      const previous = {
        token: config.token,
        secretKey: config.secretKey,
        signatureMethod: config.signatureMethod,
        allowPumpBuys: config.allowPumpBuys,
        sellLowGainHoldingsForPumpBuys: config.sellLowGainHoldingsForPumpBuys,
      };

      config = {
        ...config,
        token,
        secretKey,
        signatureMethod,
        enableUsdtMarkets: true,
        buyTetherCoinsOnUsdtMarkets: false,
        allowPumpBuys,
        sellLowGainHoldingsForPumpBuys,
      };
      saveConfig();

      submitBtn.textContent = 'در حال بررسی اتصال...';
      try {
        log('در حال بررسی صحت API اصلی ال‌بانک...', 'info');
        const wallets = await getWallets({ accountId: 'primary' });
        setBalanceSummary(wallets);
        log(`API اصلی ال‌بانک معتبر است. روش امضا: ${lbankSignatureMethodLabel(signatureMethod)}`, 'ok');
        renderExactBalances(wallets);
        refreshUi();
        return true;
      } catch (err) {
        config = { ...config, ...previous };
        saveConfig();
        refreshUi();
        throw new Error(`بررسی اتصال شکست خورد:\n${String(err?.message || err)}`);
      }
    },
  });

  if (result) refreshUi();
}

  function toggleArmed() {
    if (!config.token && !config.armed) {
      log('اول API اصلی ال‌بانک را ثبت کن.', 'warn');
      return;
    }
    config.armed = !config.armed;
    saveConfig();
    log(`اجرای واقعی ${config.armed ? 'روشن' : 'خاموش'} شد.`, config.armed ? 'warn' : 'info');
  }

  function toggleDryRun() {
    config.dryRun = !config.dryRun;
    saveConfig();
    log(`حالت آزمایشی ${config.dryRun ? 'روشن' : 'خاموش'} شد.`, config.dryRun ? 'warn' : 'ok');
  }

  function resetLocalPositions() {
    const ok = confirm('همه پوزیشن‌ها و سفارش‌های محلی پاک شوند؟');
    if (!ok) return;

    state.positions = {};
    state.pending = {};
    state.retryBuys = {};
    saveState();
    log('پوزیشن‌ها و سفارش‌های محلی ریست شدند.', 'warn');
  }

  async function showBalances() {
    try {
      if (!config.token) {
        log('APIای ثبت نشده است.', 'warn');
        return;
      }
      const wallets = await getWallets();
      setBalanceSummary(wallets);
      renderExactBalances(wallets);
      await refreshOpenPositionsPnl(true);
      saveState();
    } catch (err) {
      logError(err, 'showBalances');
    }
  }

  function setBalanceSummary(wallets) {
    const nonZero = (wallets || [])
      .filter(w => num(w.activeBalance ?? w.balance) > 0)
      .sort((a, b) => {
        const ar = num(a.usdtBalance ?? 0);
        const br = num(b.usdtBalance ?? 0);
        return br - ar;
      });

    state.lastBalanceSummary = nonZero.map(w => ({
      currency: String(w.currency || '').toUpperCase(),
      balance: String(w.activeBalance ?? w.balance ?? '0'),
    }));
  }

  function renderExactBalances(wallets) {
    const nonZero = (wallets || [])
      .filter(w => num(w.activeBalance ?? w.balance) > 0)
      .sort((a, b) => {
        const ar = num(a.usdtBalance ?? 0);
        const br = num(b.usdtBalance ?? 0);
        return br - ar;
      });

    if (!nonZero.length) {
      log('همه موجودی‌های اسپات صفر هستند.', 'info');
      return;
    }

    log('موجودی‌های غیرصفر:', 'ok');
    for (const w of nonZero) {
      const exact = String(w.activeBalance ?? w.balance ?? '0');
      log(`• ${String(w.currency).toUpperCase()}: ${exact}`, 'info');
    }
  }


function normalizeUsdQuoteCode(value, fallback = 'usdt') {
  let quote = String(value || fallback || 'usdt')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');

  if (!quote) quote = String(fallback || 'usdt').toLowerCase().replace(/[^a-z0-9]/g, '') || 'usdt';
  if (!isUsdLikeQuoteCode(quote)) return 'usdt';
  return quote;
}

function isUsdLikeQuoteCode(value) {
  const quote = String(value || '').trim().toLowerCase().replace(/[^a-z0-9]/g, '');
  return !!quote && quote.includes('usd');
}

function usdLikeQuotePriority(quote) {
  const q = normalizeUsdQuoteCode(quote);
  const priorities = ['usdt', 'usdc', 'usde', 'usdd', 'usdp', 'fdusd', 'tusd', 'busd', 'usds', 'usdk', 'usdx', 'usd'];
  const idx = priorities.indexOf(q);
  return idx >= 0 ? idx : priorities.length;
}

function quoteLabelFa(quote) {
  return normalizeUsdQuoteCode(quote).toUpperCase();
}


function quoteUnitFa(quote) {
  return quoteLabelFa(quote);
}


function quoteSuffixToken(quote) {
  return quoteLabelFa(quote);
}

  function walletCurrencyUpper(wallet) {
    return String(wallet?.currency || '').toUpperCase();
  }


function walletUsdtValue(wallet) {
  return num(wallet.usdtValue || wallet.estimatedValue || wallet.value || wallet.balanceValue || 0);
}

  function makeManualSignalKey(side, base, quote) {
    return hashString(`manual|${side}|${base}|${quote}|${Date.now()}|${Math.random()}`);
  }

  function showPopupModal({
    title = '',
    bodyHtml = '',
    submitText = 'تأیید',
    cancelText = 'انصراف',
    onSubmit = null,
    onMount = null,
    onCancel = null,
    closeOnOverlay = true,
  } = {}) {
    return new Promise(resolve => {
      const overlay = document.createElement('div');
      overlay.className = 'lbk-modal-overlay';
      overlay.innerHTML = `
        <div class="lbk-modal" role="dialog" aria-modal="true">
          <form>
            <div class="lbk-modal-header">${title}</div>
            <div class="lbk-modal-body">${bodyHtml}</div>
            <div class="lbk-modal-actions">
              <button type="submit" data-lbk-submit>${submitText}</button>
              <button type="button" data-lbk-cancel>${cancelText}</button>
            </div>
          </form>
        </div>
      `;

      const form = overlay.querySelector('form');
      const modal = overlay.querySelector('.lbk-modal');
      const cancelBtn = overlay.querySelector('[data-lbk-cancel]');
      const submitBtn = overlay.querySelector('[data-lbk-submit]');

      function close(value = null) {
        overlay.remove();
        resolve(value);
      }

      async function handleCancel(source = 'button') {
        if (typeof onCancel === 'function') {
          try {
            const result = await onCancel({ overlay, modal, form, close, submitBtn, cancelBtn, source });
            if (result === false) return;
            if (result !== undefined && result !== true) {
              close(result);
              return;
            }
          } catch (err) {
            const message = String(err?.message || err || 'خطای نامشخص');
            alert(message);
            return;
          }
        }

        close(null);
      }

      cancelBtn.addEventListener('click', () => {
        void handleCancel('button');
      });

      if (closeOnOverlay) {
        overlay.addEventListener('click', (event) => {
          if (event.target === overlay) {
            void handleCancel('overlay');
          }
        });
      }

      form.addEventListener('submit', async (event) => {
        event.preventDefault();

        if (!onSubmit) {
          close(true);
          return;
        }

        const oldSubmitText = submitBtn.textContent;
        submitBtn.disabled = true;
        cancelBtn.disabled = true;
        submitBtn.textContent = 'در حال پردازش...';

        try {
          const result = await onSubmit({ overlay, modal, form, close, submitBtn, cancelBtn });

          if (result && typeof result === 'object' && result.keepOpen) {
            submitBtn.textContent = result.submitText || oldSubmitText;
            if (!result.keepSubmitDisabled) submitBtn.disabled = false;
            if (!result.keepCancelDisabled) cancelBtn.disabled = false;
            return;
          }

          if (result !== false) {
            close(result === undefined ? true : result);
            return;
          }
        } catch (err) {
          const message = String(err?.message || err || 'خطای نامشخص');
          alert(message);
        }

        submitBtn.textContent = oldSubmitText;
        submitBtn.disabled = false;
        cancelBtn.disabled = false;
      });

      document.documentElement.appendChild(overlay);

      const firstFocus = overlay.querySelector('input, button, textarea, select');
      if (firstFocus) {
        window.setTimeout(() => {
          try {
            firstFocus.focus();
            if (typeof firstFocus.select === 'function') firstFocus.select();
          } catch (_) {}
        }, 0);
      }

      if (typeof onMount === 'function') {
        try {
          onMount({ overlay, modal, form, close, submitBtn, cancelBtn });
        } catch (err) {
          console.error('[معامله‌گر ال‌بانک] modal onMount failed', err);
        }
      }
    });
  }

  async function showInfoPopup(title, message, submitText = 'باشه') {
    await showPopupModal({
      title,
      bodyHtml: `<p>${message}</p>`,
      submitText,
      cancelText: 'بستن',
      onSubmit: () => true,
    });
  }

  function showBusyOverlay(title = 'لطفاً صبر کن', message = 'در حال پردازش...') {
    const overlay = document.createElement('div');
    overlay.className = 'lbk-modal-overlay';
    overlay.innerHTML = `
      <div class="lbk-modal" role="dialog" aria-modal="true">
        <div class="lbk-modal-header">${escapeHtml(title)}</div>
        <div class="lbk-modal-body">
          <div class="lbk-manual-buy-live-box is-active" style="display:block; margin-top:0;">
            <div class="lbk-manual-buy-live-row">
              <span class="lbk-loading-spinner" aria-hidden="true"></span>
              <div>
                <div style="font-weight:700;">در حال انجام...</div>
                <div data-lbk-busy-message class="lbk-note" style="display:block; margin-top:4px;">${escapeHtml(message)}</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    `;
    document.documentElement.appendChild(overlay);

    return {
      overlay,
      setMessage(nextMessage) {
        const el = overlay.querySelector('[data-lbk-busy-message]');
        if (el) el.textContent = String(nextMessage || 'در حال پردازش...');
      },
      close() {
        overlay.remove();
      },
    };
  }


function parseManualMarketInput(value) {
  const parsed = splitSymbolAndQuote(value, 'usdt');
  return parsed && parsed.base ? { base: parsed.base, quote: parsed.quote, quoteExplicit: !!parsed.quoteExplicit } : null;
}

  function shouldPlaceRealOrder(options = {}) {
    if (options.allowWhenDryRunOff) {
      return !config.dryRun;
    }
    return !!config.armed && !config.dryRun;
  }

  function createManualBuySession(base, quote) {
    const startedAt = Date.now();
    return {
      manualSessionId: makeManualSignalKey('manual-session', base, quote),
      manualSessionStartedAt: startedAt,
      manualSessionExpiresAt: startedAt + MANUAL_BUY_SESSION_TTL_MS,
      manualSessionCancelledAt: 0,
      manualSessionStopReason: '',
    };
  }

  function normalizeManualBuySessionMeta(source = {}) {
    return {
      manualSessionId: String(source?.manualSessionId || ''),
      manualSessionStartedAt: Math.max(0, num(source?.manualSessionStartedAt)),
      manualSessionExpiresAt: Math.max(0, num(source?.manualSessionExpiresAt)),
      manualSessionCancelledAt: Math.max(0, num(source?.manualSessionCancelledAt)),
      manualSessionStopReason: String(source?.manualSessionStopReason || ''),
    };
  }

  function evaluateManualBuySessionGuard(source = {}, now = Date.now()) {
    const meta = normalizeManualBuySessionMeta(source);

    if (!meta.manualSessionId) {
      return { enabled: false, ok: true, reason: '', meta };
    }

    if (meta.manualSessionCancelledAt > 0) {
      return {
        enabled: true,
        ok: false,
        reason: meta.manualSessionStopReason || 'خرید دستی توسط کاربر متوقف شد.',
        meta,
      };
    }

    if (meta.manualSessionExpiresAt > 0 && now >= meta.manualSessionExpiresAt) {
      return {
        enabled: true,
        ok: false,
        reason: meta.manualSessionStopReason || 'مهلت ۱۰ دقیقه‌ای خرید دستی تمام شد.',
        meta,
      };
    }

    return { enabled: true, ok: true, reason: '', meta };
  }

  function retryEntryStatusLabel(status) {
    switch (String(status || '')) {
      case 'filled':
        return 'خرید دستی تکمیل شد.';
      case 'dry_run':
        return 'خرید دستی در حالت شبیه‌سازی انجام شد.';
      case 'cancelled':
        return 'خرید دستی توسط کاربر متوقف شد.';
      case 'expired':
        return 'مهلت ۱۰ دقیقه‌ای خرید دستی تمام شد.';
      case 'partial':
        return 'بخشی از سفارش انجام شد و ادامه‌ای ثبت نشد.';
      case 'failed':
        return 'ثبت خرید دستی ناموفق بود.';
      case 'ignored':
        return 'خرید دستی قابل‌اجرا نبود.';
      case 'needs_retry':
        return 'سفارش بدون پرشدن کامل تمام شد و ادامه‌ای انجام نشد.';
      case 'validation_retry':
        return 'ثبت سفارش به retry نیاز داشت و ادامه‌ای انجام نشد.';
      default:
        return 'خرید دستی دیگر فعال نیست.';
    }
  }

  function renderManualBuyPopupProgress(overlay, patch = {}) {
    if (!overlay || !overlay.isConnected) return;

    const box = overlay.querySelector('#lbk-manual-buy-live-box');
    const titleEl = overlay.querySelector('#lbk-manual-buy-live-title');
    const statusEl = overlay.querySelector('#lbk-manual-buy-live-status');

    if (!box || !titleEl || !statusEl) return;

    box.classList.toggle('is-active', !!patch.active);
    box.classList.toggle('is-finished', !!patch.finished);
    box.classList.toggle('is-error', !!patch.error);

    if (patch.title !== undefined) {
      titleEl.textContent = String(patch.title || '');
    }
    if (patch.message !== undefined) {
      statusEl.textContent = String(patch.message || '');
    }
  }

  function setManualBuyPopupBusyControls(overlay, busy = true) {
    if (!overlay) return;
    const controls = overlay.querySelectorAll('input[name="lbk-buy-mode"], #lbk-buy-allocation-input');
    controls.forEach(control => {
      control.disabled = !!busy;
    });
  }

  function cleanupManualBuyPopupController(sessionId) {
    const key = String(sessionId || '');
    if (!key) return null;

    const controller = activeManualBuyPopupControllers.get(key);
    if (!controller) return null;

    if (controller.intervalId) {
      window.clearInterval(controller.intervalId);
    }
    if (controller.closeTimerId) {
      window.clearTimeout(controller.closeTimerId);
    }

    activeManualBuyPopupControllers.delete(key);
    return controller;
  }

  function manualBuySessionState(base, quote, sessionId, defaultMeta = {}, accountId = 'primary') {
    const upperBase = normalizeLbankBaseToken(base);
    const normalizedQuote = normalizeQuoteCode(quote);
    const safeSessionId = String(sessionId || '');
    const normalizedAccountId = String(accountId || 'primary');

    const pending = Object.values(state.pending || {}).find(p =>
      p &&
      p.side === 'buy' &&
      canonicalBaseEquals(p.base, upperBase) &&
      p.quote === normalizedQuote &&
      String(p.accountId || 'primary') === normalizedAccountId &&
      (!safeSessionId || String(p.manualSessionId || '') === safeSessionId)
    );

    const retryEntry = getRetryBuy(upperBase, normalizedQuote, normalizedAccountId);
    const matchingRetry = retryEntry && (!safeSessionId || String(retryEntry.manualSessionId || '') === safeSessionId)
      ? retryEntry
      : null;

    const guardSource = matchingRetry || pending || defaultMeta || {};
    const guard = evaluateManualBuySessionGuard(guardSource);

    if (guard.enabled && !guard.ok) {
      return {
        active: false,
        finished: true,
        error: true,
        title: 'خرید دستی متوقف شد',
        message: guard.reason,
      };
    }

    if (pending) {
      return {
        active: true,
        finished: false,
        error: false,
        title: 'خرید دستی در حال انجام است...',
        message: 'سفارش فعلی هنوز در حال پیگیری است.',
      };
    }

    if (matchingRetry) {
      const status = String(matchingRetry.status || '');

      if (matchingRetry.autoSweepRemainingBalance && !matchingRetry.autoSweepComplete) {
        return {
          active: true,
          finished: false,
          error: false,
          title: 'خرید دستی در حال انجام است...',
          message: ['pending', 'attempting'].includes(status)
            ? 'سفارش فعلی در حال ثبت/پیگیری است.'
            : 'اول زمان ۱۰ دقیقه‌ای بررسی می‌شود؛ اگر هنوز داخل مهلت باشد، باقی‌مانده دوباره بررسی و ادامه داده می‌شود.',
        };
      }

      if (['pending', 'attempting'].includes(status)) {
        return {
          active: true,
          finished: false,
          error: false,
          title: 'خرید دستی در حال انجام است...',
          message: 'سفارش فعلی در حال ثبت/پیگیری است.',
        };
      }

      return {
        active: false,
        finished: true,
        error: ['failed', 'validation_retry', 'needs_retry'].includes(status),
        title: 'خرید دستی تمام شد',
        message: String(
          matchingRetry.autoSweepStopReason ||
          matchingRetry.manualSessionStopReason ||
          matchingRetry.lastError ||
          retryEntryStatusLabel(status)
        ),
      };
    }

    return {
      active: false,
      finished: true,
      error: false,
      title: 'خرید دستی تمام شد',
      message: 'خرید دستی دیگر فعال نیست.',
    };
  }

  function monitorManualBuyPopup(controller) {
    const safeSessionId = String(controller?.sessionId || '');
    if (!safeSessionId) return;

    cleanupManualBuyPopupController(safeSessionId);

    const nextController = {
      ...controller,
      intervalId: 0,
      closeTimerId: 0,
      closed: false,
    };

    function safeClose(value = true) {
      if (nextController.closed) return;
      nextController.closed = true;
      cleanupManualBuyPopupController(safeSessionId);
      try {
        nextController.close(value);
      } catch (_) {}
    }

    function tick() {
      if (!nextController.overlay?.isConnected) {
        cleanupManualBuyPopupController(safeSessionId);
        return;
      }

      const sessionState = manualBuySessionState(
        nextController.base,
        nextController.quote,
        safeSessionId,
        nextController.defaultMeta,
        nextController.accountId || 'primary'
      );

      renderManualBuyPopupProgress(nextController.overlay, {
        active: true,
        finished: sessionState.finished,
        error: sessionState.error,
        title: sessionState.title,
        message: sessionState.message,
      });

      if (!sessionState.active) {
        nextController.submitBtn.disabled = true;
        nextController.cancelBtn.disabled = false;
        nextController.cancelBtn.textContent = 'بستن';

        if (!nextController.closeTimerId) {
          nextController.closeTimerId = window.setTimeout(() => {
            safeClose({
              status: sessionState.error ? 'error' : 'done',
              sessionState,
            });
          }, 900);
        }
        return;
      }

      nextController.submitBtn.disabled = true;
      nextController.cancelBtn.disabled = false;
      nextController.cancelBtn.textContent = 'انصراف';
    }

    nextController.intervalId = window.setInterval(tick, 1000);
    activeManualBuyPopupControllers.set(safeSessionId, nextController);
    tick();
  }

  function markManualBuySessionStopped(base, quote, sessionId, reason, options = {}) {
    const upperBase = normalizeLbankBaseToken(base);
    const normalizedQuote = normalizeQuoteCode(quote);
    const safeSessionId = String(sessionId || '');
    const safeReason = String(reason || 'خرید دستی متوقف شد.');
    const stoppedAt = Date.now();
    const accountId = String(options.accountId || 'primary');
    const retryEntry = getRetryBuy(upperBase, normalizedQuote, accountId);

    if (retryEntry && (!safeSessionId || String(retryEntry.manualSessionId || '') === safeSessionId)) {
      upsertRetryBuy(retryEntry, {
        manualSessionCancelledAt: options.markCancelled ? stoppedAt : num(retryEntry.manualSessionCancelledAt),
        manualSessionStopReason: safeReason,
        autoSweepComplete: true,
        autoSweepStopReason: safeReason,
        status: options.status || (num(retryEntry.filledAmount) > 0 ? 'filled' : 'cancelled'),
      });
    }

    for (const pending of Object.values(state.pending || {})) {
      if (!pending || pending.side !== 'buy') continue;
      if (!canonicalBaseEquals(pending.base, upperBase) || normalizeQuoteCode(pending.quote) !== normalizedQuote) continue;
      if (String(pending.accountId || 'primary') !== accountId) continue;
      if (safeSessionId && String(pending.manualSessionId || '') !== safeSessionId) continue;

      if (options.markCancelled) {
        pending.manualSessionCancelledAt = stoppedAt;
      }
      pending.manualSessionStopReason = safeReason;
    }
  }

  function getManualBuySelectableAccounts() {
    const accounts = [];

    if (String(config.token || '').trim()) {
      accounts.push({
        id: 'primary',
        kind: 'primary',
        label: accountLabel('primary'),
        token: String(config.token || '').trim(),
        active: true,
      });
    }

    for (const item of getSubTokens(true)) {
      const token = String(item?.token || '').trim();
      if (!token) continue;

      accounts.push({
        id: String(item.id || ''),
        kind: 'sub',
        label: accountLabel(item.id),
        token,
        active: item.active !== false,
      });
    }

    return accounts;
  }

  function buildSelectableAccountOptionsHtml(accounts, selectedId = 'primary') {
    const list = Array.isArray(accounts) ? accounts : [];
    if (!list.length) {
      return '<option value="primary">اصلی (توکن ثبت نشده)</option>';
    }

    return list.map((account) => {
      const id = String(account?.id || 'primary');
      const label = escapeHtml(account?.label || accountLabel(id));
      const suffix = account?.kind === 'sub' && account?.active === false ? ' - غیرفعال' : '';
      const selected = id === String(selectedId || 'primary') ? ' selected' : '';
      return `<option value="${escapeHtml(id)}"${selected}>${label}${suffix}</option>`;
    }).join('');
  }

  async function askManualBuyRequestPopup() {
    const accounts = getManualBuySelectableAccounts();
    if (!accounts.length) return null;

    return showPopupModal({
      title: 'خرید دستی',
      bodyHtml: `
        <label for="lbk-manual-buy-symbol">کوین / بازار</label>
        <input id="lbk-manual-buy-symbol" type="text" value="" placeholder="مثلاً BTC یا BTCUSDT یا BTC-USDC" autocomplete="off" />
        <label for="lbk-manual-buy-account">حساب درخواست</label>
        <select id="lbk-manual-buy-account">${buildSelectableAccountOptionsHtml(accounts, 'primary')}</select>
        <div class="lbk-note">اگر فقط نام کوین را بنویسی، ربات بین جفت‌های دلاری مجاز همان کوین، جفتی را انتخاب می‌کند که موجودی آزاد بیشتری در حساب انتخابی دارد.</div>
      `,
      submitText: 'مرحله بعد',
      cancelText: 'انصراف',
      onSubmit: ({ overlay }) => {
        const raw = String(overlay.querySelector('#lbk-manual-buy-symbol')?.value || '').trim();
        const quoteHint = 'usdt';
        const accountId = String(overlay.querySelector('#lbk-manual-buy-account')?.value || 'primary');
        const parsed = parseLivePriceMarketInput(raw, quoteHint);
        const selectedAccount = accounts.find(item => String(item.id || 'primary') === accountId) || null;

        if (!selectedAccount) {
          throw new Error('حساب درخواست را انتخاب کن.');
        }
        if (!parsed) {
          throw new Error('بازار نامعتبر است. مثال درست: BTC، BTCUSDT یا BTC-USDC');
        }

        return {
          selectedAccount,
          raw,
          base: parsed.base,
          quote: parsed.quote,
        };
      },
    });
  }

  async function askManualSellRequestPopup() {
    const accounts = getManualBuySelectableAccounts();
    if (!accounts.length) return null;

    return showPopupModal({
      title: 'فروش دستی',
      bodyHtml: `
        <div class="lbk-summary">حساب فروش را انتخاب کن.</div>
        <label for="lbk-manual-sell-account">حساب درخواست</label>
        <select id="lbk-manual-sell-account">${buildSelectableAccountOptionsHtml(accounts, 'primary')}</select>
        <div class="lbk-note">اول حساب فروش را انتخاب کن؛ مرحله بعد دارایی‌های قابل‌معامله همان حساب در جفت‌های دلاری مجاز نمایش داده می‌شود.</div>
      `,
      submitText: 'نمایش دارایی‌ها',
      cancelText: 'انصراف',
      onSubmit: ({ overlay }) => {
        const quote = 'usdt';
        const accountId = String(overlay.querySelector('#lbk-manual-sell-account')?.value || 'primary');
        const selectedAccount = accounts.find(item => String(item.id || 'primary') === accountId) || null;

        if (!selectedAccount) {
          throw new Error('حساب درخواست را انتخاب کن.');
        }

        return {
          selectedAccount,
          quote,
        };
      },
    });
  }

  async function askManualBuyAccountPopup() {
    const accounts = getManualBuySelectableAccounts();
    if (!accounts.length) return null;
    if (accounts.length === 1) return accounts[0];

    return showPopupModal({
      title: 'انتخاب حساب خرید دستی',
      bodyHtml: `
        <div class="lbk-summary">
          خرید دستی را روی کدام حساب انجام می‌دهی؟
        </div>

        <div class="lbk-radio-list">
          ${accounts.map((account, index) => `
            <label class="lbk-sell-item">
              <span class="lbk-inline-option" style="margin-bottom:0;">
                <input type="radio" name="lbk-manual-buy-account" value="${escapeHtml(account.id)}" ${index === 0 ? 'checked' : ''} />
                <span>${escapeHtml(account.label)}${account.kind === 'sub' && !account.active ? ' (غیرفعال)' : ''}</span>
              </span>
              <small>${escapeHtml(maskTokenForDisplay(account.token))}</small>
            </label>
          `).join('')}
        </div>

        <div class="lbk-note">در خرید دستی می‌توانی بین API اصلی و توکن‌های فرعی انتخاب کنی. غیرفعال‌بودن API فرعی فقط روی مسیرهای خودکار اثر دارد.</div>
      `,
      submitText: 'مرحله بعد',
      cancelText: 'انصراف',
      onSubmit: ({ overlay }) => {
        const selectedId = String(overlay.querySelector('input[name="lbk-manual-buy-account"]:checked')?.value || '');
        const selected = accounts.find(item => item.id === selectedId) || null;
        if (!selected) {
          throw new Error('حساب خرید دستی را انتخاب کن.');
        }
        return selected;
      },
    });
  }

  async function askManualMarketPopup() {
    return showPopupModal({
      title: 'خرید دستی',
      bodyHtml: `
        <label for="lbk-manual-market-input">کوین / بازار</label>
        <input id="lbk-manual-market-input" type="text" placeholder="مثال: BTC یا BTCUSDT یا BTC-USDC" autocomplete="off" />
        <div class="lbk-note">اگر فقط نام کوین را بنویسی، ربات بین جفت‌های دلاری مجاز همان کوین انتخاب می‌کند.</div>
      `,
      submitText: 'مرحله بعد',
      cancelText: 'انصراف',
      onSubmit: ({ overlay }) => {
        const input = overlay.querySelector('#lbk-manual-market-input');
        const quoteHint = 'usdt';
        const raw = String(input?.value || '').trim();
        const parsed = parseLivePriceMarketInput(raw, quoteHint);
        if (!parsed) {
          throw new Error('بازار نامعتبر است. مثال درست: BTC، BTCUSDT یا BTC-USDC');
        }
        return {
          raw,
          base: parsed.base,
          quote: parsed.quote,
        };
      },
    });
  }

  async function askManualBuyAmountPopup(context, handlers = {}) {
    const {
      base,
      quote,
      availableBalance,
      minValue,
      price,
      accountLabelText = '',
    } = context;

    const quoteLabel = quoteLabelFa(quote);
    const balanceText = formatQuoteValue(availableBalance, quote);
    const minText = formatQuoteValue(minValue, quote);
    const priceText = price > 0 ? formatPrice(price, quote) : 'نامشخص';

    return showPopupModal({
      title: `خرید دستی ${base} (${quoteLabel})`,
      bodyHtml: `
        <div class="lbk-summary">
          حساب انتخابی: <b>${escapeHtml(accountLabelText || 'API اصلی')}</b><br />
          بازار: <b>${base}${quoteSuffixToken(quote)}</b><br />
          موجودی قابل استفاده ${quoteLabel}: <b>${balanceText}</b><br />
          حداقل سفارش این بازار: <b>${minText}</b><br />
          قیمت تقریبی فعلی: <b>${priceText}</b>
        </div>

        <div class="lbk-radio-list">
          <label class="lbk-inline-option">
            <input type="radio" name="lbk-buy-mode" value="manual" checked />
            <span>مقدار دلخواه</span>
          </label>
          <label class="lbk-inline-option">
            <input type="radio" name="lbk-buy-mode" value="all" />
            <span>خرید با تمام موجودی</span>
          </label>
        </div>

        <label for="lbk-buy-allocation-input">مقدار خرید برحسب ${quoteUnitFa(quote)}</label>
        <input id="lbk-buy-allocation-input" type="number" min="0" step="0.01" placeholder="مثال: 0.70 یا 20" />
        <div class="lbk-note">اگر «خرید با تمام موجودی» را انتخاب کنی، همین موجودی آزاد ${quoteLabel} برای خرید استفاده می‌شود.</div>

        <div id="lbk-manual-buy-live-box" class="lbk-manual-buy-live-box" aria-live="polite">
          <div class="lbk-manual-buy-live-row">
            <span class="lbk-loading-spinner" aria-hidden="true"></span>
            <div>
              <div id="lbk-manual-buy-live-title" style="font-weight:700;">خرید دستی در حال انجام است...</div>
              <div id="lbk-manual-buy-live-status" class="lbk-note" style="margin-top:4px;">تا وقتی خرید دستی فعال است، این پنجره باز می‌ماند.</div>
            </div>
          </div>
        </div>
      `,
      submitText: 'اجرای خرید',
      cancelText: 'انصراف',
      closeOnOverlay: false,
      onMount: ({ overlay, modal, form, close, submitBtn, cancelBtn }) => {
        const radios = Array.from(overlay.querySelectorAll('input[name="lbk-buy-mode"]'));
        const amountInput = overlay.querySelector('#lbk-buy-allocation-input');

        function syncMode() {
          const mode = radios.find(r => r.checked)?.value || 'manual';
          amountInput.disabled = mode === 'all';
          if (mode === 'all') {
            amountInput.value = '';
          }
        }

        radios.forEach(r => r.addEventListener('change', syncMode));
        syncMode();

        if (typeof handlers.onMount === 'function') {
          handlers.onMount({ overlay, modal, form, close, submitBtn, cancelBtn });
        }
      },
      onCancel: ({ overlay, modal, form, close, submitBtn, cancelBtn, source }) => {
        if (typeof handlers.onCancel === 'function') {
          return handlers.onCancel({ overlay, modal, form, close, submitBtn, cancelBtn, source });
        }
        return true;
      },
      onSubmit: ({ overlay, modal, form, close, submitBtn, cancelBtn }) => {
        const mode = overlay.querySelector('input[name="lbk-buy-mode"]:checked')?.value || 'manual';
        let allocation = 0;

        if (mode === 'all') {
          allocation = availableBalance;
        } else {
          allocation = num(overlay.querySelector('#lbk-buy-allocation-input')?.value);
        }

        if (!(allocation > 0)) {
          throw new Error('مقدار خرید باید بیشتر از صفر باشد.');
        }
        if (allocation > availableBalance) {
          throw new Error('مقدار خرید از موجودی آزاد بیشتر است.');
        }
        if (allocation < minValue) {
          throw new Error(`حداقل سفارش این بازار ${minText} است.`);
        }

        const result = {
          allocation,
          useFullBalance: mode === 'all',
        };

        if (typeof handlers.onSubmit === 'function') {
          return handlers.onSubmit({ overlay, modal, form, close, submitBtn, cancelBtn, result });
        }

        return result;
      },
    });
  }


async function resolveManualSellQuote(base, preferredQuote = 'usdt', options = {}) {
  const info = await getAllowedUsdPairInfo(base, { ...options, quote: preferredQuote });
  if (info.exact) return normalizeUsdQuoteCode(preferredQuote);
  const first = Array.isArray(info.sameBase) && info.sameBase.length ? info.sameBase[0] : null;
  return first ? normalizeUsdQuoteCode(first.quote) : 'usdt';
}


async function buildManualSellCandidates(wallets, preferredQuote = 'usdt', options = {}) {
  const map = walletMap(wallets);
  const out = [];
  for (const [currency, wallet] of Object.entries(map)) {
    const base = normalizeLbankBaseToken(currency);
    if (!base || isUsdLikeQuoteCode(base)) continue;

    const qty = walletAvailable(map, currency);
    if (!(qty > 0)) continue;

    let allowedInfo = null;
    try {
      allowedInfo = await getAllowedUsdPairInfo(base, options);
    } catch (_) {
      continue;
    }

    const allowedPairs = Array.isArray(allowedInfo?.sameBase) ? allowedInfo.sameBase : [];
    for (const allowed of allowedPairs) {
      const quote = normalizeUsdQuoteCode(allowed.quote);
      let stats = null;
      try {
        stats = await getMarketStats(base, quote, options);
      } catch (_) {
        continue;
      }

      const price = num(stats?.bestBuy || stats?.latest || stats?.mark || stats?.dayClose);
      if (!(price > 0)) continue;

      const pairRules = await getLbankPairRules(base, quote, options);
      const minQty = Math.max(0, num(pairRules?.minQty));
      const minVisibleValue = safeMinUsdtOrderValue(price, pairRules) + SELL_KEEP_MAX_USDT_VALUE;
      const quoteValue = qty * price;
      if (minQty > 0 && qty < minQty) continue;
      if (quoteValue < minVisibleValue) continue;

      out.push({
        base,
        quote,
        qty,
        balance: qty,
        usdtValue: quoteValue,
        quoteValue,
        price,
        wallet,
      });
    }
  }

  out.sort((a, b) => num(b.quoteValue ?? b.usdtValue) - num(a.quoteValue ?? a.usdtValue) || a.base.localeCompare(b.base) || usdLikeQuotePriority(a.quote) - usdLikeQuotePriority(b.quote));
  return out;
}

  async function askManualSellPopup(candidates, context = {}) {
    const accountText = context?.accountLabelText || 'اصلی';
    const preferredQuote = normalizeQuoteCode(context?.preferredQuote || 'usdt');
    const itemsHtml = candidates.map((item, idx) => `
      <label class="lbk-sell-item">
        <span class="lbk-inline-option" style="margin-bottom:0;">
          <input type="checkbox" data-lbk-sell-item value="${idx}" />
          <span><b>${item.base}</b> → ${quoteLabelFa(item.quote)}</span>
        </span>
        <small>
          موجودی: ${formatAmount(item.balance)}<br />
          ارزش تقریبی: ${formatQuoteValue(item.quoteValue ?? item.usdtValue, item.quote)}
        </small>
      </label>
    `).join('');

    return showPopupModal({
      title: 'فروش دستی',
      bodyHtml: `
        <div class="lbk-summary">
          حساب درخواست: <b>${escapeHtml(accountText)}</b><br />
          بازار: <b>دلاری مجاز</b><br />
          دارایی‌های قابل‌معامله در بازار دلاری نشان داده می‌شوند؛ موجودی خود USDT کنار گذاشته شده است.<br />
          شرط نمایش: کف امن ${formatFaNumber(MIN_USDT_ORDER)} USDT + حداقل مقدار واقعی همان جفت در LBank + ریزمانده ${formatFaNumber(SELL_KEEP_MAX_USDT_VALUE)} USDT
        </div>

        <label class="lbk-inline-option">
          <input id="lbk-sell-select-all" type="checkbox" />
          <span>انتخاب همه</span>
        </label>

        <div class="lbk-sell-list">${itemsHtml}</div>
        <div class="lbk-note">فروش دستی بر اساس موجودی واقعی کیف پول انجام می‌شود، نه صرفاً پوزیشن محلی. در فروش دستی، کمتر از ${formatFaNumber(SELL_KEEP_MAX_USDT_VALUE)} USDT از ارزش هر دارایی در کیف پول نگه داشته می‌شود.</div>
      `,
      submitText: 'اجرای فروش',
      cancelText: 'انصراف',
      onMount: ({ overlay }) => {
        const selectAll = overlay.querySelector('#lbk-sell-select-all');
        const items = Array.from(overlay.querySelectorAll('[data-lbk-sell-item]'));

        function syncAllFlag() {
          if (!items.length) {
            selectAll.checked = false;
            selectAll.indeterminate = false;
            return;
          }

          const checkedCount = items.filter(i => i.checked).length;
          selectAll.checked = checkedCount === items.length;
          selectAll.indeterminate = checkedCount > 0 && checkedCount < items.length;
        }

        selectAll.addEventListener('change', () => {
          items.forEach(item => {
            item.checked = selectAll.checked;
          });
          syncAllFlag();
        });

        items.forEach(item => item.addEventListener('change', syncAllFlag));
        syncAllFlag();
      },
      onSubmit: ({ overlay }) => {
        const indices = Array.from(overlay.querySelectorAll('[data-lbk-sell-item]:checked'))
          .map(node => Number(node.value))
          .filter(idx => Number.isInteger(idx) && idx >= 0 && idx < candidates.length);

        if (!indices.length) {
          throw new Error('حداقل یک دارایی را برای فروش انتخاب کن.');
        }

        return indices.map(idx => candidates[idx]);
      },
    });
  }


  function buildAccountOptionsHtml(includeInactive = true) {
    const accounts = [
      { id: 'primary', label: 'اصلی', token: config.token, active: true },
      ...getSubTokens(includeInactive),
    ].filter(account => String(account.token || '').trim());

    if (!accounts.length) {
      return '<option value="primary">اصلی (توکن ثبت نشده)</option>';
    }

    return accounts.map(account => {
      const id = escapeHtml(account.id || 'primary');
      const label = escapeHtml(account.label || accountLabel(account.id || 'primary'));
      const suffix = account.active === false ? ' - غیرفعال' : '';
      return `<option value="${id}">${label}${suffix}</option>`;
    }).join('');
  }


function parseLivePriceMarketInput(rawValue, quoteHint = 'usdt') {
  const parsed = splitSymbolAndQuote(rawValue, quoteHint);
  return parsed && parsed.base ? { base: parsed.base, quote: parsed.quote, quoteExplicit: !!parsed.quoteExplicit } : null;
}


function normalizeQuoteCode(value) {
  return normalizeUsdQuoteCode(value);
}

  function pickMarketPrice(stats, quote) {
    const keys = ['bestSell', 'latest', 'mark', 'dayClose', 'bestBuy'];
    for (const key of keys) {
      const value = num(stats?.[key]);
      if (value > 0) return value;
    }
    return 0;
  }

  function pickMarketDayChangePct(stats) {
    const keys = ['dayChange', 'change24h', 'changePct', 'percentChange24h'];
    for (const key of keys) {
      const value = num(stats?.[key]);
      if (Number.isFinite(value)) return value;
    }
    return null;
  }

  function formatLivePriceValue(price, quote) {
    const safePrice = num(price);
    if (!(safePrice > 0)) return '—';
    const decimals = safePrice >= 1 ? 6 : 10;
    return `${trimZeroes(safePrice.toFixed(decimals))} ${quoteLabelFa(quote)}`;
  }

  function formatLivePriceDayChangePct(changePct) {
    const value = num(changePct);
    if (!Number.isFinite(value)) return '';

    const rounded = Math.abs(Math.round(value * 100) / 100);
    const sign = value > 0 ? '+' : value < 0 ? '-' : '';
    return ` ٪${formatFaNumber(rounded)}${sign}`;
  }

  function extractPriceFromFailedOrderResponse(response, quote) {
    const text = `${response?.price || ''} ${response?.averagePrice || ''} ${response?.message || ''} ${response?.error || ''}`;
    const matches = toAsciiDigits(text).match(/\d[\d,]*(?:\.\d+)?/g) || [];
    const values = matches.map(num).filter(v => v > 0);

    if (!values.length) return 0;

    if (quote === 'usdt') {
      const usdtValues = values.filter(v => v >= 1000);
      return usdtValues.length ? Math.max(...usdtValues) : 0;
    }

    return values.find(v => v > 0 && v < 1_000_000) || values[0] || 0;
  }


function buildRejectedBuyProbePayload(base, quote, availableQuoteBalance, price) {
  return null;
}


async function cancelLivePriceProbeOrder(order, accountId) {
  return false;
}


function normalizeLbankBaseToken(base) {
  return String(base || '').trim().replace(/[^A-Za-z0-9]/g, '').toUpperCase();
}


function compactLbankBaseAlias(base) {
  return normalizeLbankBaseToken(base);
}


function canonicalBaseEquals(left, right) {
  return normalizeLbankBaseToken(left) === normalizeLbankBaseToken(right);
}

  function formatLivePriceStableOutput({
    base = '',
    quote = 'usdt',
    priceText = '—',
    changePct = null,
    updatedText = '—',
  } = {}) {
    const market = base ? `${String(base).toUpperCase()}/${quoteLabelFa(normalizeQuoteCode(quote))}` : '—';
    const changeText = priceText && priceText !== '—' ? formatLivePriceDayChangePct(changePct) : '';

    return [
      `بازار: ${market}`,
      `قیمت لحظه‌ای: ${priceText || '—'}${changeText}`,
      `آخرین بروزرسانی: ${updatedText || '—'}`,
    ].join('\n');
  }

  function formatLivePriceProbeResult(result) {
    const quote = normalizeQuoteCode(result?.quote);
    const priceText = formatLivePriceValue(result?.price, quote);
    const updatedText = new Date(num(result?.updatedAt) || Date.now()).toLocaleTimeString('fa-IR', { hour12: false });

    return formatLivePriceStableOutput({
      base: result?.base || '',
      quote,
      priceText,
      changePct: result?.changePct,
      updatedText,
    });
  }

  async function refreshLivePricePopup(overlay, submitBtn = null) {
    const input = overlay.querySelector('#lbk-live-price-symbol');
    const accountSelect = overlay.querySelector('#lbk-live-price-account');
    const resultEl = overlay.querySelector('#lbk-live-price-result');

    if (!input || !resultEl) return;
    updateLivePriceBudgetElement(overlay);

    const rawValue = String(input.value || '').trim();
    if (!rawValue) {
      resultEl.className = 'lbk-live-price-result';
      resultEl.textContent = formatLivePriceStableOutput();
      return;
    }

    const parsed = parseLivePriceMarketInput(rawValue, 'usdt');
    if (!parsed) {
      resultEl.className = 'lbk-live-price-result is-error';
      resultEl.textContent = formatLivePriceStableOutput({
        priceText: 'نماد نامعتبر',
        updatedText: new Date().toLocaleTimeString('fa-IR', { hour12: false }),
      });
      resultEl.title = 'نماد را مثل BTCUSDT، BTC-USDC یا BTC وارد کنید.';
      return;
    }


    const orderBudgetStatus = getOrderRequestBudgetStatus();
    const statsBudgetStatus = getMarketStatsRequestBudgetStatus();
    updateLivePriceBudgetElement(overlay, orderBudgetStatus);
    if (!orderBudgetStatus.canSpend) {
      stopLivePricePopupTimer(overlay);
      throw makeOrderRequestBudgetError(orderBudgetStatus);
    }
    if (!statsBudgetStatus.canSpend) {
      stopLivePricePopupTimer(overlay);
      throw makeMarketStatsRequestBudgetError(statsBudgetStatus);
    }

    resultEl.classList.add('is-loading');

    if (submitBtn) submitBtn.disabled = true;

    try {
      const result = await resolveLivePriceFromExperimentalBuy(parsed.base, parsed.quote, accountSelect?.value || 'primary');
      updateLivePriceBudgetElement(overlay, result.budgetStatus || getOrderRequestBudgetStatus());
      resultEl.className = `lbk-live-price-result ${result.status === 'warning' ? 'is-error' : 'is-ok'}`;
      resultEl.title = result.status === 'warning' ? String(result.probeMessage || '') : '';
      resultEl.textContent = formatLivePriceProbeResult(result);
    } catch (err) {
      if (err?.code === 'LocalOrderRequestBudget' || err?.code === 'LocalMarketStatsRequestBudget' || err?.status === 429) {
        stopLivePricePopupTimer(overlay);
        if (err?.budgetStatus) updateLivePriceBudgetElement(overlay, err.budgetStatus);
      }
      resultEl.className = 'lbk-live-price-result is-error';
      resultEl.title = String(err?.message || err || 'خطای نامشخص');
      resultEl.textContent = formatLivePriceStableOutput({
        base: parsed.base,
        quote: parsed.quote,
        priceText: 'خطا در رفرش',
        updatedText: new Date().toLocaleTimeString('fa-IR', { hour12: false }),
      });
    } finally {
      if (submitBtn) submitBtn.disabled = false;
      resultEl.classList.remove('is-loading');
    }
  }

  function stopLivePricePopupTimer(overlay = null) {
    const timer = num(overlay?.dataset?.nbxLivePriceTimer || activeLivePricePopupTimer);
    if (timer > 0) {
      window.clearInterval(timer);
    }
    activeLivePricePopupTimer = 0;
    if (overlay?.dataset) {
      overlay.dataset.nbxLivePriceTimer = '0';
    }
  }

  async function openLivePricePopup() {
    await showPopupModal({
      title: 'قیمت لحظه‌ای',
      bodyHtml: `
        <p class="lbk-note">نماد را وارد کنید. پنجره هر ۱۰ ثانیه خودش رفرش می‌شود. این بخش چیزی در گزارش پنل ذخیره نمی‌کند و نزدیک سقف درخواست‌ها خودکار مکث می‌کند.</p>
        <label>کوین / بازار</label>
        <input id="lbk-live-price-symbol" type="text" value="" placeholder="مثلاً BTCUSDT یا BTC-USDC یا BTC" autocomplete="off" />
        <label>حساب برای درخواست آزمایشی</label>
        <select id="lbk-live-price-account">${buildAccountOptionsHtml(true)}</select>
        <div id="lbk-live-price-result" class="lbk-live-price-result">${formatLivePriceStableOutput()}</div>
      `,
      submitText: 'رفرش',
      cancelText: 'بستن',
      closeOnOverlay: true,
      onMount: ({ overlay, submitBtn }) => {
        stopLivePricePopupTimer();
        const run = () => {
          void refreshLivePricePopup(overlay, submitBtn);
        };

        run();

        const timer = window.setInterval(run, LIVE_PRICE_REFRESH_MS);
        activeLivePricePopupTimer = timer;
        overlay.dataset.nbxLivePriceTimer = String(timer);

        const input = overlay.querySelector('#lbk-live-price-symbol');
        const account = overlay.querySelector('#lbk-live-price-account');

        for (const el of [account]) {
          if (!el) continue;
          el.addEventListener('change', run);
        }
        if (input) {
          input.addEventListener('change', run);
          input.addEventListener('keydown', (event) => {
            if (event.key === 'Enter') {
              event.preventDefault();
              run();
            }
          });
        }
      },
      onSubmit: async ({ overlay, submitBtn }) => {
        await refreshLivePricePopup(overlay, submitBtn);
        return { keepOpen: true, keepSubmitDisabled: false, keepCancelDisabled: false };
      },
      onCancel: ({ overlay }) => {
        stopLivePricePopupTimer(overlay);
        return true;
      },
    });
  }

  async function openManualBuyPopup() {
    try {
      const selectableAccounts = getManualBuySelectableAccounts();
      if (!selectableAccounts.length) {
        log('برای خرید دستی باید حداقل یک توکن ال‌بانک (اصلی یا فرعی) ثبت شده باشد.', 'warn');
        return;
      }

      const requestInfo = await askManualBuyRequestPopup();
      if (!requestInfo) return;

      const selectedAccount = requestInfo.selectedAccount;
      const accountId = String(selectedAccount.id || 'primary');
      const accountText = accountLabel(accountId);
      const base = requestInfo.base;
      let quote = normalizeUsdQuoteCode(requestInfo.quote || 'usdt');
      const quoteWasExplicit = !!requestInfo.quoteExplicit;
      const busy = showBusyOverlay('آماده‌سازی خرید دستی', `در حال خواندن موجودی ${accountText}...`);
      let availableBalance = 0;
      let minValue = minOrderValueForQuote(quote);
      let price = 0;
      let pairRules = null;
      let unsupportedPairMessage = '';

      try {
        const wallets = await getWallets({ accountId, includeInactive: true });
        if (accountId === 'primary') {
          setBalanceSummary(wallets);
        }

        const wm = walletMap(wallets);

        busy.setMessage(`در حال بررسی جفت‌های دلاری مجاز برای ${base}...`);
        try {
          const resolvedPair = await resolveBestUsdAllowedPairForWallet(base, wm, {
            accountId,
            includeInactive: true,
            preferredQuote: quote,
            preferExplicit: false,
          });

          if (!resolvedPair.ok) {
            unsupportedPairMessage = resolvedPair.reason || `برای ${base} جفت دلاری مجاز پیدا نشد.`;
          } else {
            quote = resolvedPair.quote;
            availableBalance = resolvedPair.availableBalance;
            const candidateText = allowedPairCsv((resolvedPair.candidates || []).map(item => item.pair), 20);
            log(`جفت خرید دستی ${base}: ${displayLbankPairSymbol(resolvedPair.symbol)} انتخاب شد. موجودی‌های قابل انتخاب: ${candidateText}`, 'info');
          }
        } catch (err) {
          logError(err, `allowedPairs manual buy ${base}/${quote}`);
        }

        if (!unsupportedPairMessage) {
          busy.setMessage(`در حال خواندن قیمت تقریبی ${base}/${quoteLabelFa(quote)}...`);
          try {
            const stats = await getMarketStats(base, quote, { accountId, includeInactive: true });
            price = Math.max(0, num(stats?.bestSell || stats?.latest || stats?.mark || stats?.dayClose));
            pairRules = await getLbankPairRules(base, quote, { accountId, includeInactive: true });
            minValue = safeMinUsdtOrderValue(price, pairRules);
          } catch (err) {
            logError(err, `getMarketStats manual buy ${base}/${quote}`);
          }
        }
      } finally {
        busy.close();
      }

      if (unsupportedPairMessage) {
        log(unsupportedPairMessage, 'warn');
        await showInfoPopup('جفت معاملاتی قابل استفاده نیست', `${escapeHtml(unsupportedPairMessage)}`);
        return;
      }

      if (availableBalance < minValue) {
        const thresholdText = formatQuoteValue(minValue, quote);
        const balanceText = formatQuoteValue(availableBalance, quote);

        log(`برای خرید دستی ${base}/${quote} در ${accountText} موجودی ${quoteLabelFa(quote)} کافی نیست.`, 'warn');
        await showInfoPopup('خرید دستی', `حساب انتخابی: ${accountText}<br />موجودی آزاد ${quoteLabelFa(quote)} فعلی ${balanceText} است و حداقل لازم ${thresholdText} است.`);
        return;
      }

      await askManualBuyAmountPopup({
        base,
        quote,
        availableBalance,
        minValue,
        price,
        accountLabelText: accountText,
      }, {
        onCancel: ({ overlay }) => {
          const sessionId = String(overlay.dataset.nbxManualBuySessionId || '');
          if (!sessionId) return true;

          markManualBuySessionStopped(
            base,
            quote,
            sessionId,
            'کاربر از پنجرهٔ خرید دستی انصراف زد.',
            { accountId, markCancelled: true, status: 'cancelled' }
          );
          cleanupManualBuyPopupController(sessionId);
          saveState();
          return true;
        },
        onSubmit: ({ overlay, close, submitBtn, cancelBtn, result }) => {
          const allocation = Math.max(0, num(result?.allocation));
          const useFullBalance = !!result?.useFullBalance;
          const quoteLabel = quoteLabelFa(quote);
          const allocationText = formatQuoteValue(allocation, quote);
          const autoSweepSequenceId = useFullBalance ? makeManualSignalKey('buy-all', base, quote) : '';
          const manualSession = createManualBuySession(base, quote);

          overlay.dataset.nbxManualBuySessionId = manualSession.manualSessionId;
          overlay.dataset.nbxManualBuyBase = base;
          overlay.dataset.nbxManualBuyQuote = quote;

          renderManualBuyPopupProgress(overlay, {
            active: true,
            finished: false,
            error: false,
            title: 'خرید دستی در حال انجام است...',
            message: 'در حال ثبت سفارش خرید دستی...',
          });
          setManualBuyPopupBusyControls(overlay, true);

          const signal = {
            side: 'buy',
            base,
            quote,
            rawLine: `manual-buy|${accountId}|${base}${quoteSuffixToken(quote)}|${allocation}|${useFullBalance ? 'all' : 'manual'}`,
            signalKey: makeManualSignalKey('buy', base, quote),
            score: 0,
          };

          upsertRetryBuy(signal, {
            accountId,
            desiredAllocation: allocation,
            status: 'queued',
            lastError: '',
            allowWhenDryRunOff: true,
            autoSweepRemainingBalance: useFullBalance,
            autoSweepSequenceId,
            autoSweepIteration: 0,
            autoSweepComplete: false,
            autoSweepStopReason: '',
            ...manualSession,
          });
          saveState();

          monitorManualBuyPopup({
            sessionId: manualSession.manualSessionId,
            accountId,
            base,
            quote,
            overlay,
            close,
            submitBtn,
            cancelBtn,
            defaultMeta: manualSession,
          });

          const manualBuyIsReal = shouldPlaceRealOrder({ allowWhenDryRunOff: true });

          log(
            `درخواست خرید دستی ${base} (${quoteLabel}) | حساب: ${accountText} | بودجه: ${allocationText}` +
            `${useFullBalance ? ' (تمام موجودی آزاد)' : ''} ثبت شد.` +
            `${manualBuyIsReal ? ' در حالت واقعی.' : ' در حالت شبیه‌سازی/غیرفعال.'}`,
            manualBuyIsReal ? 'warn' : 'info'
          );

          void (async () => {
            try {
              await executeBuy(signal, allocation, {
                accountId,
                reasonLabel: 'manual_buy',
                allowWhenDryRunOff: true,
                autoSweepRemainingBalance: useFullBalance,
                autoSweepSequenceId,
                autoSweepIteration: 0,
                ...manualSession,
              });
              saveState();
            } catch (err) {
              markManualBuySessionStopped(
                base,
                quote,
                manualSession.manualSessionId,
                String(err?.message || err || 'خطای نامشخص'),
                { accountId, status: 'failed' }
              );
              saveState();
              logError(err, 'openManualBuyPopup/startManualBuy');
            }
          })();

          return {
            keepOpen: true,
            keepSubmitDisabled: true,
            keepCancelDisabled: false,
            submitText: 'در حال انجام...',
          };
        },
      });
    } catch (err) {
      logError(err, 'openManualBuyPopup');
    }
  }

  async function openManualSellPopup() {

    try {
      const requestInfo = await askManualSellRequestPopup();
      if (!requestInfo) return;

      const accountId = String(requestInfo.selectedAccount?.id || 'primary');
      const accountText = accountLabel(accountId);
      const preferredQuote = normalizeQuoteCode(requestInfo.quote || 'usdt');

      const busy = showBusyOverlay('آماده‌سازی فروش دستی', `در حال خواندن موجودی ${accountText}...`);
      let candidates = [];
      try {
        const wallets = await getWallets({ accountId, includeInactive: true });
        if (accountId === 'primary') {
          setBalanceSummary(wallets);
        }

        busy.setMessage('در حال آماده‌سازی لیست دارایی‌های قابل فروش...');
        candidates = await buildManualSellCandidates(wallets, preferredQuote, { accountId });
      } finally {
        busy.close();
      }

      if (!candidates.length) {
        log(`هیچ دارایی مناسبی برای فروش دستی در ${accountText} پیدا نشد.`, 'warn');
        await showInfoPopup('فروش دستی', `حساب انتخابی: ${escapeHtml(accountText)}<br />هیچ دارایی قابل‌معامله در بازار دلاری با ارزش کافی برای فروش در موجودی این حساب پیدا نشد.`);
        return;
      }

      const selected = await askManualSellPopup(candidates, { accountLabelText: accountText, preferredQuote });
      if (!selected?.length) return;

      const manualSellIsReal = shouldPlaceRealOrder({ allowWhenDryRunOff: true });

      log(
        `فروش دستی در ${accountText} برای ${selected.map(item => `${item.base}(${quoteLabelFa(item.quote)})`).join('، ')} شروع شد.` +
        `${manualSellIsReal ? ' سفارش‌ها واقعی ارسال می‌شوند.' : ' فعلاً در حالت شبیه‌سازی/غیرفعال اجرا می‌شود.'}`,
        manualSellIsReal ? 'warn' : 'info'
      );

      for (let i = 0; i < selected.length; i++) {
        const item = selected[i];
        await executeSell(item.base, item.quote, item.balance, false, item.balance, {
          accountId,
          keepReserve: true,
          reasonLabel: 'manual_sell',
          allowWhenDryRunOff: true,
        });

        if (i < selected.length - 1) {
          await waitBetweenTradeActions(`بین فروش دستی ${item.base} و فروش دستی بعدی`);
        }
      }

      saveState();
    } catch (err) {
      logError(err, 'openManualSellPopup');
    }
  }


  /******************************************************************
   * زمان / نرمال‌سازی
   ******************************************************************/
  function timeNowFa() {
    return new Intl.DateTimeFormat('fa-IR', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    }).format(new Date());
  }

  function formatDateTimeFa(ms) {
    try {
      return new Intl.DateTimeFormat('fa-IR', {
        timeZone: 'Asia/Tehran',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
      }).format(new Date(ms));
    } catch (_) {
      return new Date(ms).toLocaleString();
    }
  }

  function formatFaNumber(n) {
    try {
      const safe = Number(n);
      if (!Number.isFinite(safe)) return String(n);
      const ascii = safe.toLocaleString('en-US', {
        useGrouping: true,
        maximumFractionDigits: 20,
      });
      return ascii.replace(/\d/g, d => '۰۱۲۳۴۵۶۷۸۹'[Number(d)]);
    } catch (_) {
      return String(n);
    }
  }

  function formatRemaining(ms) {
    if (!Number.isFinite(ms)) return 'نامشخص';
    if (ms <= 0) return 'رسیده';
    const totalMin = Math.ceil(ms / 60000);
    const h = Math.floor(totalMin / 60);
    const m = totalMin % 60;
    if (h <= 0) return `${formatFaNumber(m)} دقیقه`;
    return `${formatFaNumber(h)} ساعت و ${formatFaNumber(m)} دقیقه`;
  }


function feeRateForQuote(quote) {
  return FEE_RATE_USDT;
}

  function feePercentText(quote) {
    return formatLtrText('0.2%');
  }

  function feeValueForQuote(value, quote) {
    return Math.max(0, num(value)) * feeRateForQuote(quote);
  }


function formatQuoteValue(value, quote) {
  const safe = num(value);
  const decimals = Math.abs(safe) >= 1 ? 4 : 6;
  return `${trimZeroes(safe.toFixed(decimals))} ${quoteUnitFa(quote)}`;
}

  function formatSignedQuoteValue(value, quote) {
    const safe = num(value);
    const absValue = Math.abs(safe);
    const sign = safe > 0 ? '+' : safe < 0 ? '-' : '';
    const numberText = trimZeroes(absValue.toFixed(absValue >= 1 ? 4 : 6));
    return `${formatLtrText(`${sign}${numberText}`)} ${quoteUnitFa(quote)}`;
  }


function dailyPnlDisplayUnitFa(quote) {
  return quoteUnitFa(quote);
}


function convertDailyPnlDisplayValue(value, quote) {
  return num(value);
}


function formatDailyPnlSignedValue(value, quote) {
  const v = num(value);
  const sign = v > 0 ? '+' : '';
  return `${sign}${trimZeroes(v.toFixed(4))} ${quoteUnitFa(quote)}`;
}

  function calcSummaryPercentFromValues(values) {
    const safeValues = Array.isArray(values)
      ? values.map(item => num(item)).filter(value => Number.isFinite(value))
      : [];

    const positiveSum = safeValues
      .filter(value => value > 0)
      .reduce((sum, value) => sum + value, 0);

    const negativeMagnitude = safeValues
      .filter(value => value < 0)
      .reduce((sum, value) => sum + Math.abs(value), 0);

    if (!(positiveSum > 0)) {
      return safeValues.reduce((sum, value) => sum + value, 0);
    }

    return positiveSum - negativeMagnitude;
  }

  function formatLtrText(value) {
    return `⁦${String(value ?? '')}⁩`;
  }

  function calcPnlPercent(pnlValue, costValue) {
    const cost = Math.max(0, num(costValue));
    if (!(cost > 0)) return 0;
    return (num(pnlValue) / cost) * 100;
  }

  function formatSignedPercent(value) {
    const safe = num(value);
    const abs = Math.abs(safe).toFixed(2);
    if (safe > 0) return formatLtrText(`+${abs}%`);
    if (safe < 0) return formatLtrText(`-${abs}%`);
    return formatLtrText(`${abs}%`);
  }

  function forceSellMinAgeBaseTime(position) {
    return Math.max(0, num(position?.lastBuyAt) || num(position?.openedAt));
  }

  function hasReachedMaxHold(position, now = Date.now()) {
    return !!position && (num(position?.openedAt) > 0) && ((now - num(position.openedAt)) >= HOLD_MAX_MS);
  }

  function hasReachedPnlForceSell(position, now = Date.now()) {
    if (!position) return false;

    const baseTime = forceSellMinAgeBaseTime(position);
    if (!(baseTime > 0)) return false;
    if ((now - baseTime) < FORCE_SELL_MIN_AGE_MS) return false;
    if (!(num(position.lastPnlAt) > 0)) return false;

    return num(position.lastPnlPercent) <= FORCE_SELL_PNL_THRESHOLD_PERCENT;
  }

  function forceSellReasonLabel(position, now = Date.now()) {
    if (hasReachedPnlForceSell(position, now)) return 'حد ضرر PnL';
    if (hasReachedMaxHold(position, now)) return 'مهلت نگه‌داری';
    return '';
  }

  function tehranDayKey() {
    return new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Tehran',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    }).format(new Date());
  }

  function ensureDayReset() {
    const dk = tehranDayKey();
    if (state.dayKey !== dk) {
      state.dayKey = dk;
      state.processedSignals = {};
      state.radarMeta = {};
      state.currentPriorityList = [];
      state.currentSellList = [];
      state.currentIgnoredBuys = [];
      state.latestRadar = deepClone(DEFAULT_STATE.latestRadar);
      state.latestStrategy = deepClone(DEFAULT_STATE.latestStrategy);
      saveState();
      log(`روز جدید تهران شروع شد: ${dk}. کش روزانه سیگنال‌ها ریست شد.`, 'info');
    }
  }

  function toAsciiDigits(s) {
    const fa = '۰۱۲۳۴۵۶۷۸۹';
    const ar = '٠١٢٣٤٥٦٧٨٩';
    return String(s ?? '')
      .split('')
      .map(ch => {
        const fi = fa.indexOf(ch);
        if (fi >= 0) return String(fi);
        const ai = ar.indexOf(ch);
        if (ai >= 0) return String(ai);
        return ch;
      })
      .join('');
  }

  function cleanText(s) {
    return toAsciiDigits(String(s ?? ''))
      .replace(/[\`*~]/g, '')
      .replace(/[‎‏؜⁦⁧⁨⁩]/g, '')
      .replace(/ /g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function looksLikeSignalFeedText(text) {
    const raw = String(text || '');
    const compact = cleanText(raw).replace(/\s+/g, '');
    const hasShortTs = /(?:\d{2}_\d{2}::\d{2}_\d{2}|\d{4}::\d{4})(?:AM|PM)/i.test(compact);
    const hasIsoTs = /\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}/.test(cleanText(raw));
    const hasMarkers = /[⭐🔴🟢🟡🔻]/.test(raw);
    const hasMarket = /\bmarket\b/i.test(raw);
    return (hasShortTs || hasIsoTs) && (hasMarkers || hasMarket);
  }

  function num(v) {
    if (typeof v === 'number') return Number.isFinite(v) ? v : 0;
    const cleaned = toAsciiDigits(String(v ?? ''))
      .replace(/,/g, '')
      .replace(/[^\d.\-]/g, '');
    const n = Number(cleaned);
    return Number.isFinite(n) ? n : 0;
  }

  function clamp(value, min, max) {
    return Math.min(max, Math.max(min, value));
  }

  function formatAmount(v) {
    return trimZeroes((Math.floor(v * 1e8) / 1e8).toFixed(8));
  }


function formatPrice(v, quote) {
  const n = num(v);
  if (!Number.isFinite(n) || n <= 0) return '—';
  return `${trimZeroes(n.toFixed(n >= 1 ? 6 : 10))} ${quoteLabelFa(quote)}`;
}

  function trimZeroes(s) {
    return String(s).replace(/(\.\d*?[1-9])0+$/,'$1').replace(/\.0+$/, '').replace(/\.$/, '');
  }

  function hashString(str) {
    let h = 2166136261;
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      h += (h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24);
    }
    return (h >>> 0).toString(36);
  }



function isOrderBudgetedPath(path) {
  return /^\/v2\/(?:supplement\/create_order(?:_test)?)\.do(?:\?|$)/.test(String(path || ''));
}

  function pruneOrderRequestBudget(now = Date.now()) {
    const cutoff = now - ORDER_REQUEST_WINDOW_MS;
    orderRequestBudget.timestamps = orderRequestBudget.timestamps
      .map(ts => num(ts))
      .filter(ts => ts > cutoff && ts <= now + 1000)
      .sort((a, b) => a - b);

    if (orderRequestBudget.cooldownUntilMs <= now) {
      orderRequestBudget.cooldownUntilMs = 0;
      orderRequestBudget.lastBackOffSeconds = 0;
    }
  }

  function getOrderRequestBudgetStatus(now = Date.now()) {
    pruneOrderRequestBudget(now);

    const used = orderRequestBudget.timestamps.length;
    const oldest = used ? orderRequestBudget.timestamps[0] : 0;
    const windowRetryMs = used >= ORDER_REQUEST_SAFE_LIMIT
      ? Math.max(0, oldest + ORDER_REQUEST_WINDOW_MS - now)
      : 0;
    const cooldownRetryMs = Math.max(0, num(orderRequestBudget.cooldownUntilMs) - now);
    const retryAfterMs = Math.max(windowRetryMs, cooldownRetryMs);

    return {
      used,
      remaining: Math.max(0, ORDER_REQUEST_SAFE_LIMIT - used),
      safeLimit: ORDER_REQUEST_SAFE_LIMIT,
      docLimit: ORDER_REQUEST_DOC_LIMIT,
      reserve: ORDER_REQUEST_RESERVE,
      windowMs: ORDER_REQUEST_WINDOW_MS,
      canSpend: retryAfterMs <= 0 && used < ORDER_REQUEST_SAFE_LIMIT,
      retryAfterMs,
      cooldownUntilMs: num(orderRequestBudget.cooldownUntilMs),
      lastBackOffSeconds: num(orderRequestBudget.lastBackOffSeconds),
    };
  }

  function makeOrderRequestBudgetError(status = getOrderRequestBudgetStatus()) {
    const retryText = status.retryAfterMs > 0 ? ` حدود ${formatRemaining(status.retryAfterMs)} دیگر` : ' کمی بعد';
    const err = new Error(
      `بودجه محلی درخواست سفارش پر شده است؛ برای جلوگیری از خطای 429، رفرش خودکار قیمت لحظه‌ای موقتاً متوقف شد. دوباره${retryText} رفرش کن.`
    );
    err.code = 'LocalOrderRequestBudget';
    err.status = 429;
    err.retryAfterMs = status.retryAfterMs;
    err.budgetStatus = status;
    return err;
  }

  function spendOrderRequestBudget(path, options = {}) {
    if (!isOrderBudgetedPath(path) || options.skipOrderBudget === true) {
      return getOrderRequestBudgetStatus();
    }

    const now = Date.now();
    const status = getOrderRequestBudgetStatus(now);
    if (!status.canSpend) {
      throw makeOrderRequestBudgetError(status);
    }

    orderRequestBudget.timestamps.push(now);
    return getOrderRequestBudgetStatus(now);
  }

  function applyOrderRequestBudgetBackoff(path, err) {
    if (!isOrderBudgetedPath(path) || num(err?.backOff) <= 0) return;

    const backOffMs = Math.ceil(num(err.backOff) * 1000);
    const until = Date.now() + backOffMs;
    orderRequestBudget.cooldownUntilMs = Math.max(num(orderRequestBudget.cooldownUntilMs), until);
    orderRequestBudget.lastBackOffSeconds = Math.max(num(orderRequestBudget.lastBackOffSeconds), num(err.backOff));
  }

  function clearOrderRequestBudget() {
    orderRequestBudget.timestamps = [];
    orderRequestBudget.cooldownUntilMs = 0;
    orderRequestBudget.lastBackOffSeconds = 0;
  }

  function renderLivePriceBudgetStatus(status = getOrderRequestBudgetStatus()) {
    const retry = status.canSpend
      ? ''
      : ` | توقف تا حدود ${formatRemaining(status.retryAfterMs)}`;
    return [
      `بودجه درخواست سفارش: ${formatFaNumber(status.used)}/${formatFaNumber(status.safeLimit)} در ۱۰ ثانیه`,
      `رزرو ایمنی تا سقف مستند ال‌بانک: ${formatFaNumber(status.reserve)} درخواست`,
      retry,
    ].filter(Boolean).join('\n');
  }

  function updateLivePriceBudgetElement(overlay, status = getOrderRequestBudgetStatus()) {
    const budgetEl = overlay?.querySelector?.('#lbk-live-price-budget');
    if (!budgetEl) return;
    budgetEl.className = `lbk-live-price-budget${status.canSpend ? '' : ' is-paused'}`;
    budgetEl.textContent = renderLivePriceBudgetStatus(status);
  }


function isMarketStatsBudgetedPath(path) {
  return /^\/v2\/(?:supplement\/ticker\/(?:bookTicker|price)|ticker\/24hr|etfTicker\/24hr)\.do(?:\?|$)/.test(String(path || ''));
}

  function pruneMarketStatsRequestBudget(now = Date.now()) {
    const cutoff = now - MARKET_STATS_REQUEST_WINDOW_MS;
    marketStatsRequestBudget.timestamps = marketStatsRequestBudget.timestamps
      .map(ts => num(ts))
      .filter(ts => ts > cutoff && ts <= now + 1000)
      .sort((a, b) => a - b);

    if (marketStatsRequestBudget.cooldownUntilMs <= now) {
      marketStatsRequestBudget.cooldownUntilMs = 0;
      marketStatsRequestBudget.lastBackOffSeconds = 0;
    }
  }

  function getMarketStatsRequestBudgetStatus(now = Date.now()) {
    pruneMarketStatsRequestBudget(now);

    const used = marketStatsRequestBudget.timestamps.length;
    const oldest = used ? marketStatsRequestBudget.timestamps[0] : 0;
    const windowRetryMs = used >= MARKET_STATS_REQUEST_SAFE_LIMIT
      ? Math.max(0, oldest + MARKET_STATS_REQUEST_WINDOW_MS - now)
      : 0;
    const cooldownRetryMs = Math.max(0, num(marketStatsRequestBudget.cooldownUntilMs) - now);
    const retryAfterMs = Math.max(windowRetryMs, cooldownRetryMs);

    return {
      used,
      remaining: Math.max(0, MARKET_STATS_REQUEST_SAFE_LIMIT - used),
      safeLimit: MARKET_STATS_REQUEST_SAFE_LIMIT,
      docLimit: MARKET_STATS_REQUEST_DOC_LIMIT,
      reserve: MARKET_STATS_REQUEST_RESERVE,
      windowMs: MARKET_STATS_REQUEST_WINDOW_MS,
      canSpend: retryAfterMs <= 0 && used < MARKET_STATS_REQUEST_SAFE_LIMIT,
      retryAfterMs,
      cooldownUntilMs: num(marketStatsRequestBudget.cooldownUntilMs),
      lastBackOffSeconds: num(marketStatsRequestBudget.lastBackOffSeconds),
    };
  }

  function makeMarketStatsRequestBudgetError(status = getMarketStatsRequestBudgetStatus()) {
    const retryText = status.retryAfterMs > 0 ? ` حدود ${formatRemaining(status.retryAfterMs)} دیگر` : ' کمی بعد';
    const err = new Error(
      `بودجه محلی market/stats پر شده است؛ برای جلوگیری از خطای 429، رفرش قیمت لحظه‌ای موقتاً متوقف شد. دوباره${retryText} رفرش کن.`
    );
    err.code = 'LocalMarketStatsRequestBudget';
    err.status = 429;
    err.retryAfterMs = status.retryAfterMs;
    err.budgetStatus = status;
    return err;
  }

  function spendMarketStatsRequestBudget(path, options = {}) {
    if (!isMarketStatsBudgetedPath(path) || options.skipMarketStatsBudget === true) {
      return getMarketStatsRequestBudgetStatus();
    }

    const now = Date.now();
    const status = getMarketStatsRequestBudgetStatus(now);
    if (!status.canSpend) {
      throw makeMarketStatsRequestBudgetError(status);
    }

    marketStatsRequestBudget.timestamps.push(now);
    return getMarketStatsRequestBudgetStatus(now);
  }

  function applyMarketStatsRequestBudgetBackoff(path, err) {
    if (!isMarketStatsBudgetedPath(path) || num(err?.backOff) <= 0) return;

    const backOffMs = Math.ceil(num(err.backOff) * 1000);
    const until = Date.now() + backOffMs;
    marketStatsRequestBudget.cooldownUntilMs = Math.max(num(marketStatsRequestBudget.cooldownUntilMs), until);
    marketStatsRequestBudget.lastBackOffSeconds = Math.max(num(marketStatsRequestBudget.lastBackOffSeconds), num(err.backOff));
  }

  function clearMarketStatsRequestBudget() {
    marketStatsRequestBudget.timestamps = [];
    marketStatsRequestBudget.cooldownUntilMs = 0;
    marketStatsRequestBudget.lastBackOffSeconds = 0;
  }

  function renderMarketStatsBudgetStatus(status = getMarketStatsRequestBudgetStatus()) {
    const retry = status.canSpend
      ? ''
      : ` | توقف تا حدود ${formatRemaining(status.retryAfterMs)}`;
    return [
      `بودجه market/stats: ${formatFaNumber(status.used)}/${formatFaNumber(status.safeLimit)} در ۱۰ ثانیه`,
      `رزرو ایمنی تا سقف مستند ال‌بانک: ${formatFaNumber(status.reserve)} درخواست`,
      retry,
    ].filter(Boolean).join('\n');
  }

  /******************************************************************
   * API ال‌بانک
   ******************************************************************/

let lbankTimeOffsetMs = 0;
let lbankTimeSyncedAtMs = 0;

function lbankSymbol(base, quote = 'usdt') {
  const cleanBase = normalizeLbankBaseToken(base).toLowerCase();
  const cleanQuote = normalizeUsdQuoteCode(quote);
  return `${cleanBase}_${cleanQuote}`;
}

function makeLbankEchostr() {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let out = '';
  const bytes = new Uint8Array(36);
  if (window.crypto && window.crypto.getRandomValues) {
    window.crypto.getRandomValues(bytes);
    for (const b of bytes) out += alphabet[b % alphabet.length];
    return out;
  }
  for (let i = 0; i < 36; i++) out += alphabet[Math.floor(Math.random() * alphabet.length)];
  return out;
}

function lbankSuccess(response) {
  if (!response || typeof response !== 'object') return false;
  const result = response.result;
  const okResult = result === true || result === 'true' || result === undefined;
  const code = response.error_code ?? response.code;
  const okCode = code === undefined || code === 0 || code === '0';
  return okResult && okCode;
}

async function syncLbankTimeIfNeeded(force = false) {
  const now = Date.now();
  if (!force && lbankTimeSyncedAtMs && now - lbankTimeSyncedAtMs < 5 * 60 * 1000) return;
  try {
    const resp = await httpRequest('GET', '/v2/timestamp.do', null, { auth: false, skipBudget: true, raw: true });
    const serverTime = num(resp?.data || resp?.ts || resp?.timestamp || resp);
    if (serverTime > 0) {
      lbankTimeOffsetMs = serverTime - Date.now();
      lbankTimeSyncedAtMs = Date.now();
    }
  } catch (err) {
    lbankTimeSyncedAtMs = Date.now();
  }
}

function md5Upper(input) {
  function cmn(q, a, b, x, s, t) {
    a = add32(add32(a, q), add32(x, t));
    return add32((a << s) | (a >>> (32 - s)), b);
  }
  function ff(a, b, c, d, x, s, t) { return cmn((b & c) | ((~b) & d), a, b, x, s, t); }
  function gg(a, b, c, d, x, s, t) { return cmn((b & d) | (c & (~d)), a, b, x, s, t); }
  function hh(a, b, c, d, x, s, t) { return cmn(b ^ c ^ d, a, b, x, s, t); }
  function ii(a, b, c, d, x, s, t) { return cmn(c ^ (b | (~d)), a, b, x, s, t); }
  function md5cycle(x, k) {
    let [a, b, c, d] = x;
    a = ff(a, b, c, d, k[0], 7, -680876936);
    d = ff(d, a, b, c, k[1], 12, -389564586);
    c = ff(c, d, a, b, k[2], 17, 606105819);
    b = ff(b, c, d, a, k[3], 22, -1044525330);
    a = ff(a, b, c, d, k[4], 7, -176418897);
    d = ff(d, a, b, c, k[5], 12, 1200080426);
    c = ff(c, d, a, b, k[6], 17, -1473231341);
    b = ff(b, c, d, a, k[7], 22, -45705983);
    a = ff(a, b, c, d, k[8], 7, 1770035416);
    d = ff(d, a, b, c, k[9], 12, -1958414417);
    c = ff(c, d, a, b, k[10], 17, -42063);
    b = ff(b, c, d, a, k[11], 22, -1990404162);
    a = ff(a, b, c, d, k[12], 7, 1804603682);
    d = ff(d, a, b, c, k[13], 12, -40341101);
    c = ff(c, d, a, b, k[14], 17, -1502002290);
    b = ff(b, c, d, a, k[15], 22, 1236535329);
    a = gg(a, b, c, d, k[1], 5, -165796510);
    d = gg(d, a, b, c, k[6], 9, -1069501632);
    c = gg(c, d, a, b, k[11], 14, 643717713);
    b = gg(b, c, d, a, k[0], 20, -373897302);
    a = gg(a, b, c, d, k[5], 5, -701558691);
    d = gg(d, a, b, c, k[10], 9, 38016083);
    c = gg(c, d, a, b, k[15], 14, -660478335);
    b = gg(b, c, d, a, k[4], 20, -405537848);
    a = gg(a, b, c, d, k[9], 5, 568446438);
    d = gg(d, a, b, c, k[14], 9, -1019803690);
    c = gg(c, d, a, b, k[3], 14, -187363961);
    b = gg(b, c, d, a, k[8], 20, 1163531501);
    a = gg(a, b, c, d, k[13], 5, -1444681467);
    d = gg(d, a, b, c, k[2], 9, -51403784);
    c = gg(c, d, a, b, k[7], 14, 1735328473);
    b = gg(b, c, d, a, k[12], 20, -1926607734);
    a = hh(a, b, c, d, k[5], 4, -378558);
    d = hh(d, a, b, c, k[8], 11, -2022574463);
    c = hh(c, d, a, b, k[11], 16, 1839030562);
    b = hh(b, c, d, a, k[14], 23, -35309556);
    a = hh(a, b, c, d, k[1], 4, -1530992060);
    d = hh(d, a, b, c, k[4], 11, 1272893353);
    c = hh(c, d, a, b, k[7], 16, -155497632);
    b = hh(b, c, d, a, k[10], 23, -1094730640);
    a = hh(a, b, c, d, k[13], 4, 681279174);
    d = hh(d, a, b, c, k[0], 11, -358537222);
    c = hh(c, d, a, b, k[3], 16, -722521979);
    b = hh(b, c, d, a, k[6], 23, 76029189);
    a = hh(a, b, c, d, k[9], 4, -640364487);
    d = hh(d, a, b, c, k[12], 11, -421815835);
    c = hh(c, d, a, b, k[15], 16, 530742520);
    b = hh(b, c, d, a, k[2], 23, -995338651);
    a = ii(a, b, c, d, k[0], 6, -198630844);
    d = ii(d, a, b, c, k[7], 10, 1126891415);
    c = ii(c, d, a, b, k[14], 15, -1416354905);
    b = ii(b, c, d, a, k[5], 21, -57434055);
    a = ii(a, b, c, d, k[12], 6, 1700485571);
    d = ii(d, a, b, c, k[3], 10, -1894986606);
    c = ii(c, d, a, b, k[10], 15, -1051523);
    b = ii(b, c, d, a, k[1], 21, -2054922799);
    a = ii(a, b, c, d, k[8], 6, 1873313359);
    d = ii(d, a, b, c, k[15], 10, -30611744);
    c = ii(c, d, a, b, k[6], 15, -1560198380);
    b = ii(b, c, d, a, k[13], 21, 1309151649);
    a = ii(a, b, c, d, k[4], 6, -145523070);
    d = ii(d, a, b, c, k[11], 10, -1120210379);
    c = ii(c, d, a, b, k[2], 15, 718787259);
    b = ii(b, c, d, a, k[9], 21, -343485551);
    x[0] = add32(a, x[0]);
    x[1] = add32(b, x[1]);
    x[2] = add32(c, x[2]);
    x[3] = add32(d, x[3]);
  }
  function md5blk(s) {
    const md5blks = [];
    for (let i = 0; i < 64; i += 4) {
      md5blks[i >> 2] = s.charCodeAt(i) + (s.charCodeAt(i + 1) << 8) + (s.charCodeAt(i + 2) << 16) + (s.charCodeAt(i + 3) << 24);
    }
    return md5blks;
  }
  function md51(str) {
    const utf8 = unescape(encodeURIComponent(str));
    let n = utf8.length;
    const state = [1732584193, -271733879, -1732584194, 271733878];
    let i;
    for (i = 64; i <= n; i += 64) md5cycle(state, md5blk(utf8.substring(i - 64, i)));
    let tail = Array(16).fill(0);
    const rest = utf8.substring(i - 64);
    for (i = 0; i < rest.length; i++) tail[i >> 2] |= rest.charCodeAt(i) << ((i % 4) << 3);
    tail[i >> 2] |= 0x80 << ((i % 4) << 3);
    if (i > 55) {
      md5cycle(state, tail);
      tail = Array(16).fill(0);
    }
    tail[14] = n * 8;
    md5cycle(state, tail);
    return state;
  }
  function rhex(n) {
    let s = '';
    for (let j = 0; j < 4; j++) s += ((n >> (j * 8 + 4)) & 0x0F).toString(16) + ((n >> (j * 8)) & 0x0F).toString(16);
    return s;
  }
  function hex(x) { return x.map(rhex).join(''); }
  function add32(a, b) { return (a + b) & 0xFFFFFFFF; }
  return hex(md51(String(input))).toUpperCase();
}

async function hmacSha256Hex(message, secret) {
  if (!window.crypto?.subtle) throw new Error('مرورگر از WebCrypto برای امضای HMAC پشتیبانی نمی‌کند.');
  const enc = new TextEncoder();
  const key = await window.crypto.subtle.importKey('raw', enc.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const sig = await window.crypto.subtle.sign('HMAC', key, enc.encode(message));
  return Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, '0')).join('');
}

async function rsaSha256Base64(message, privateKeyValue) {
  if (!window.crypto?.subtle) throw new Error('مرورگر از WebCrypto برای امضای RSA پشتیبانی نمی‌کند.');
  const cleanKey = normalizeLbankPrivateKeyMaterial(privateKeyValue);
  if (!cleanKey) throw new Error('RSA Private Key خالی است.');

  let keyBuffer;
  try {
    keyBuffer = base64ToArrayBuffer(cleanKey);
  } catch (_) {
    throw new Error('RSA Private Key باید به‌صورت PEM یا Base64 معتبر باشد.');
  }

  let cryptoKey;
  try {
    cryptoKey = await window.crypto.subtle.importKey(
      'pkcs8',
      keyBuffer,
      { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' },
      false,
      ['sign']
    );
  } catch (_) {
    throw new Error('RSA Private Key باید فرمت PKCS#8 باشد. کلیدهای BEGIN RSA PRIVATE KEY معمولاً باید به PKCS#8 تبدیل شوند.');
  }

  const enc = new TextEncoder();
  const sig = await window.crypto.subtle.sign({ name: 'RSASSA-PKCS1-v1_5' }, cryptoKey, enc.encode(message));
  let bin = '';
  const bytes = new Uint8Array(sig);
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin);
}

function buildLbankSignSource(params = {}) {
  return Object.keys(params)
    .filter(key => key !== 'sign' && params[key] !== undefined && params[key] !== null)
    .sort()
    .map(key => `${key}=${params[key]}`)
    .join('&');
}

async function signLbankParams(params, secretKey, signatureMethod = LBANK_SIGNATURE_HMAC) {
  const source = buildLbankSignSource(params);
  const prepared = md5Upper(source);
  const method = normalizeLbankSignatureMethod(signatureMethod);
  if (method === LBANK_SIGNATURE_RSA) {
    return rsaSha256Base64(prepared, secretKey);
  }
  return hmacSha256Hex(prepared, secretKey);
}


function httpRequest(method, path, data = null, options = {}) {
  const isAuth = options.auth !== false;
  const account = getAccountContext(options.accountId || 'primary', { includeInactive: true });
  const apiKey = String(options.token || options.apiKey || account.token || '').trim();
  const secretKey = String(options.secretKey || account.secretKey || '').trim();
  const signatureMethod = normalizeLbankSignatureMethod(options.signatureMethod || account.signatureMethod);

  return new Promise(async (resolve, reject) => {
    const upperMethod = String(method || 'GET').toUpperCase();
    const headers = Object.assign({
      'Accept': 'application/json, text/plain, */*',
    }, options.headers || {});

    let body = null;
    let url = API_BASE + path;
    let params = Object.assign({}, data || {});

    try {
      if (isAuth) {
        if (!apiKey || !secretKey) throw new Error(`API Key یا ${lbankSecretLabel(signatureMethod)} ال‌بانک برای این حساب ثبت نشده است.`);
        await syncLbankTimeIfNeeded(false);
        const authParams = {
          ...params,
          api_key: apiKey,
          timestamp: String(Date.now() + lbankTimeOffsetMs),
          signature_method: signatureMethod,
          echostr: makeLbankEchostr(),
        };
        const sign = await signLbankParams(authParams, secretKey, signatureMethod);
        // v20 fix: timestamp, echostr, signature_method go in BOTH POST body AND headers.
        // CCXT lbank2 puts all auth params in the body (body = urlencode(all_params_including_auth))
        // and duplicates them in headers. LBank's server reads the signature source from the POST body,
        // so if these are header-only the server can't reconstruct the sorted string → signature mismatch
        // reported as 10008 ("currency pair nonsupport"). Matching CCXT exactly fixes this.
        headers['timestamp'] = authParams.timestamp;
        headers['signature_method'] = authParams.signature_method;
        headers['echostr'] = authParams.echostr;
        params = {
          ...params,
          api_key: apiKey,
          timestamp: authParams.timestamp,
          signature_method: signatureMethod,
          echostr: authParams.echostr,
          sign,
        };
      }

      if (upperMethod === 'GET') {
        const qs = buildQueryString(params);
        if (qs) url += (url.includes('?') ? '&' : '?') + qs;
      } else {
        headers['Content-Type'] = 'application/x-www-form-urlencoded';
        body = buildQueryString(params);
      }
    } catch (err) {
      reject(err);
      return;
    }

    GM_xmlhttpRequest({
      method: upperMethod,
      url,
      headers,
      data: body,
      timeout: options.timeout || 30000,
      onload: (res) => {
        let json = null;
        try {
          json = res.responseText ? JSON.parse(res.responseText) : null;
        } catch (_) {}

        if (res.status < 200 || res.status >= 300) {
          reject(new Error(`HTTP ${res.status}: ${res.responseText || ''}`));
          return;
        }

        if (options.raw) {
          resolve(json !== null ? json : res.responseText);
          return;
        }

        if (json && !lbankSuccess(json)) {
          const code = json.error_code ?? json.code ?? '';
          const msg = json.msg || json.message || json.error || res.responseText || 'LBank API error';
          reject(new Error(code ? `${msg} (${code})` : msg));
          return;
        }

        resolve(json !== null ? json : res.responseText);
      },
      onerror: err => reject(new Error(`خطای شبکه LBank: ${err?.error || err?.message || err}`)),
      ontimeout: () => reject(new Error('درخواست LBank timeout شد.')),
    });
  });
}


function apiGet(path, options = {}) {
  return httpRequest('GET', path, null, { ...options, auth: false });
}


function normalizeLbankPairSymbol(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/-/g, '_')
    .replace(/[^a-z0-9_]/g, '');
}

function displayLbankPairSymbol(value) {
  const normalized = normalizeLbankPairSymbol(value);
  if (!normalized) return '';
  const parts = normalized.split('_');
  if (parts.length >= 2) {
    return `${parts[0].toUpperCase()}/${parts[1].toUpperCase()}`;
  }
  return normalized.toUpperCase();
}

function compactLbankPairKey(value) {
  return String(value || '').trim().toLowerCase().replace(/[^a-z0-9]/g, '');
}

function requestedLbankPairKeys(base, quote) {
  const cleanBase = normalizeLbankBaseToken(base).toLowerCase();
  const cleanQuote = normalizeUsdQuoteCode(quote);
  const canonical = lbankSymbol(cleanBase, cleanQuote);
  return new Set([
    normalizeLbankPairSymbol(canonical),
    compactLbankPairKey(canonical),
    compactLbankPairKey(`${cleanBase}/${cleanQuote}`),
    compactLbankPairKey(`${cleanBase}${cleanQuote}`),
  ].filter(Boolean));
}

function pairMatchesBaseQuote(pair, base, quote) {
  const keys = requestedLbankPairKeys(base, quote);
  const normalized = normalizeLbankPairSymbol(pair);
  const compact = compactLbankPairKey(pair);
  if (keys.has(normalized) || keys.has(compact)) return true;
  const parts = normalized.includes('_') ? normalized.split('_') : [];
  if (parts.length >= 2) {
    const itemBase = parts[0];
    const itemQuote = normalizeUsdQuoteCode(parts.slice(1).join('_'));
    return itemBase === normalizeLbankBaseToken(base).toLowerCase() && itemQuote === normalizeUsdQuoteCode(quote);
  }
  return false;
}

async function getLbankAllowedPairs(options = {}) {
  const now = Date.now();
  if (lbankAllowedPairsCache.cachedAtMs && now - lbankAllowedPairsCache.cachedAtMs < LBANK_ALLOWED_PAIRS_CACHE_MS && lbankAllowedPairsCache.pairs.length) {
    return lbankAllowedPairsCache.pairs.slice();
  }

  const res = await apiGet('/v2/currencyPairs.do', {
    ...options,
    skipMarketStatsBudget: true,
  });

  const rawPairs = Array.isArray(res?.data) ? res.data : [];
  const pairs = [...new Set(rawPairs
    .map(normalizeLbankPairSymbol)
    .filter(Boolean))]
    .sort();

  lbankAllowedPairsCache.pairs = pairs;
  lbankAllowedPairsCache.set = new Set(pairs);
  lbankAllowedPairsCache.cachedAtMs = Date.now();
  return pairs.slice();
}

function splitLbankAllowedPair(pair) {
  const normalized = normalizeLbankPairSymbol(pair);
  const idx = normalized.lastIndexOf('_');
  if (idx <= 0 || idx >= normalized.length - 1) return null;
  const base = normalized.slice(0, idx);
  const quote = normalized.slice(idx + 1);
  if (!base || !isUsdLikeQuoteCode(quote)) return null;
  return { pair: normalized, base, quote: normalizeUsdQuoteCode(quote) };
}

function getUsdLikeAllowedPairs(pairs) {
  return (Array.isArray(pairs) ? pairs : [])
    .map(splitLbankAllowedPair)
    .filter(Boolean)
    .sort((a, b) => a.base.localeCompare(b.base) || usdLikeQuotePriority(a.quote) - usdLikeQuotePriority(b.quote) || a.quote.localeCompare(b.quote));
}

function getCachedAllowedPairSet() {
  return lbankAllowedPairsCache.set instanceof Set ? lbankAllowedPairsCache.set : new Set(lbankAllowedPairsCache.pairs || []);
}

async function getAllowedUsdPairInfo(base, options = {}) {
  const baseLower = normalizeLbankBaseToken(base).toLowerCase();
  const requestedQuote = options.quote ? normalizeUsdQuoteCode(options.quote) : '';
  const requestedSymbol = requestedQuote ? normalizeLbankPairSymbol(lbankSymbol(baseLower, requestedQuote)) : '';
  const pairs = await getLbankAllowedPairs(options);
  const usdPairs = getUsdLikeAllowedPairs(pairs);
  const sameBase = usdPairs.filter(item => item.base === baseLower);

  // Use the parsed allowed-pair rows as the source of truth. In v11 the UI resolver
  // could select MORI/USDT from sameBase, but the later preflight compared against a
  // differently-normalized cache Set and rejected the same pair. This keeps selection
  // and validation on one canonical key: base_quote in lowercase.
  const matchedExact = requestedSymbol
    ? sameBase.find(item => normalizeLbankPairSymbol(item.pair) === requestedSymbol || normalizeUsdQuoteCode(item.quote) === requestedQuote)
    : (sameBase[0] || null);
  const exact = !!matchedExact;
  const exactPair = matchedExact?.pair || '';

  const startsWithBase = sameBase.map(item => item.pair);
  const containsBase = usdPairs
    .filter(item => item.base !== baseLower && item.pair.includes(baseLower))
    .map(item => item.pair);
  const suggestions = [...startsWithBase, ...containsBase].slice(0, 40);

  return {
    symbol: requestedSymbol || exactPair || lbankSymbol(baseLower, requestedQuote || 'usdt'),
    quote: requestedQuote || (matchedExact?.quote || sameBase[0]?.quote || 'usdt'),
    exact,
    exactPair,
    sameBase,
    total: usdPairs.length,
    suggestions,
    usdPairs: usdPairs.map(item => item.pair),
  };
}

async function getAllowedUsdtPairInfo(base, options = {}) {
  return getAllowedUsdPairInfo(base, { ...options, quote: options.quote || 'usdt' });
}

function isAllowedUsdPairInfoExact(info = {}, base = '', quote = '') {
  const cleanBase = normalizeLbankBaseToken(base).toLowerCase();
  const cleanQuote = normalizeUsdQuoteCode(quote);
  const requestedSymbol = normalizeLbankPairSymbol(lbankSymbol(cleanBase, cleanQuote));
  const sameBase = Array.isArray(info.sameBase) ? info.sameBase : [];
  const suggestions = Array.isArray(info.suggestions) ? info.suggestions : [];
  const usdPairs = Array.isArray(info.usdPairs) ? info.usdPairs : [];

  if (info.exact && pairMatchesBaseQuote(info.exactPair || info.symbol || requestedSymbol, cleanBase, cleanQuote)) {
    return true;
  }

  if (normalizeUsdQuoteCode(info.quote || '') === cleanQuote && pairMatchesBaseQuote(info.symbol || requestedSymbol, cleanBase, cleanQuote)) {
    return true;
  }

  const candidates = [];
  for (const item of sameBase) {
    if (!item) continue;
    candidates.push(item.pair || lbankSymbol(item.base || cleanBase, item.quote || cleanQuote));
    if (item.base && item.quote) candidates.push(lbankSymbol(item.base, item.quote));
  }
  candidates.push(...suggestions, ...usdPairs, info.exactPair, info.symbol);

  return candidates.some(pair => pairMatchesBaseQuote(pair, cleanBase, cleanQuote));
}

function normalizeAllowedUsdPairInfo(info = {}, base = '', quote = '') {
  if (!isAllowedUsdPairInfoExact(info, base, quote)) return info;
  const cleanQuote = normalizeUsdQuoteCode(quote || info.quote || 'usdt');
  const cleanSymbol = normalizeLbankPairSymbol(lbankSymbol(base, cleanQuote));
  return {
    ...info,
    exact: true,
    exactPair: info.exactPair || cleanSymbol,
    symbol: cleanSymbol,
    quote: cleanQuote,
  };
}

function makeUnsupportedPairMessage(base, info = {}) {
  const symbolText = displayLbankPairSymbol(info.symbol || lbankSymbol(base, info.quote || 'usdt'));
  const suggestions = Array.isArray(info.suggestions) ? info.suggestions : [];
  const shown = suggestions.map(displayLbankPairSymbol).filter(Boolean).slice(0, 24);
  const totalText = info.total ? ` تعداد جفت‌های دلاری قابل‌مشاهده: ${formatFaNumber(info.total)}.` : '';
  const suggestionText = shown.length
    ? ` جفت‌های مجاز/نزدیک: ${shown.join(', ')}`
    : ' جفت دلاری نزدیکی برای این نماد پیدا نشد؛ از دکمه «جفت‌های مجاز» لیست قابل جستجو را ببین.';
  return `${symbolText} در لیست جفت‌های دلاری مجاز LBank نیست.${totalText}${suggestionText}`;
}

function allowedPairCsv(pairs, limit = 300) {
  const items = (Array.isArray(pairs) ? pairs : [])
    .map(displayLbankPairSymbol)
    .filter(Boolean)
    .slice(0, Math.max(1, limit));
  return items.join(', ');
}

function isCurrencyPairUnsupportedMessage(message) {
  return /currency pair nonsupport/i.test(String(message || '')) || /\b10008\b/.test(String(message || ''));
}

async function ensureTradableUsdtPair(base, options = {}) {
  const info = normalizeAllowedUsdPairInfo(await getAllowedUsdtPairInfo(base, options), base, options.quote || 'usdt');
  if (isAllowedUsdPairInfoExact(info, base, info.quote || options.quote || 'usdt')) return info;
  throw new Error(makeUnsupportedPairMessage(base, info));
}

async function ensureTradableUsdPair(base, quote, options = {}) {
  const info = normalizeAllowedUsdPairInfo(await getAllowedUsdPairInfo(base, { ...options, quote }), base, quote);
  if (isAllowedUsdPairInfoExact(info, base, quote)) return info;

  // Public pair lists can be misleading and can also arrive in multiple textual
  // formats. Do not block a real LBank order solely from the local cache when the
  // same base has USD-like pairs; let create_order_test/create_order return the
  // authoritative API error. This prevents false negatives such as selecting
  // BTC/USDT and then rejecting BTC/USDT locally.
  if (Array.isArray(info.sameBase) && info.sameBase.length) {
    log(`اعتبارسنجی محلی جفت ${displayLbankPairSymbol(lbankSymbol(base, quote))} قطعی نبود؛ بررسی نهایی با API ال‌بانک انجام می‌شود.`, 'warn');
    return {
      ...info,
      exact: true,
      exactPair: lbankSymbol(base, quote),
      symbol: lbankSymbol(base, quote),
      quote: normalizeUsdQuoteCode(quote),
      localValidationSoftPassed: true,
    };
  }

  throw new Error(makeUnsupportedPairMessage(base, info));
}

function chooseBestUsdAllowedPairForWallet(base, allowedInfo, walletMap, options = {}) {
  const preferredQuote = options.preferredQuote ? normalizeUsdQuoteCode(options.preferredQuote) : '';
  const preferExplicit = !!options.preferExplicit;
  const sameBase = Array.isArray(allowedInfo?.sameBase) ? allowedInfo.sameBase : [];
  if (!sameBase.length) return null;

  let candidates = sameBase.map(item => {
    const quote = normalizeUsdQuoteCode(item.quote);
    const available = walletAvailable(walletMap || {}, quote.toUpperCase());
    return {
      base: normalizeLbankBaseToken(base),
      quote,
      symbol: item.pair || lbankSymbol(base, quote),
      availableBalance: available,
      availableQuoteBalance: available,
      label: displayLbankPairSymbol(item.pair || lbankSymbol(base, quote)),
    };
  });

  if (preferExplicit && preferredQuote) {
    const exact = candidates.find(item => item.quote === preferredQuote);
    if (exact) return exact;
  }

  candidates = candidates.sort((a, b) =>
    num(b.availableBalance) - num(a.availableBalance) ||
    usdLikeQuotePriority(a.quote) - usdLikeQuotePriority(b.quote) ||
    a.quote.localeCompare(b.quote)
  );
  return candidates[0] || null;
}

async function resolveBestUsdAllowedPairForWallet(base, walletMap, options = {}) {
  const preferredQuote = options.preferredQuote ? normalizeUsdQuoteCode(options.preferredQuote) : '';
  const allowedInfo = await getAllowedUsdPairInfo(base, options);
  const selected = chooseBestUsdAllowedPairForWallet(base, allowedInfo, walletMap, {
    preferredQuote,
    preferExplicit: !!options.preferExplicit,
  });
  if (!selected) {
    return { ok: false, reason: makeUnsupportedPairMessage(base, allowedInfo), allowedInfo, candidates: [] };
  }
  return { ok: true, ...selected, allowedInfo, candidates: allowedInfo.sameBase || [] };
}

function renderAllowedPairsInto(container, pairs, filterValue = '') {
  if (!container) return;
  const q = String(filterValue || '').trim().toLowerCase().replace(/[^a-z0-9]/g, '');
  const filtered = pairs
    .filter(pair => !q || pair.replace(/_/g, '').includes(q) || pair.includes(q));

  if (!filtered.length) {
    container.innerHTML = '<div style="direction:rtl;text-align:right;color:#b76e00;">موردی پیدا نشد.</div>';
    return;
  }

  const csv = allowedPairCsv(filtered, 600);
  container.innerHTML = `<textarea readonly style="width:100%;min-height:220px;box-sizing:border-box;border:1px solid #cbd5e1;border-radius:10px;padding:10px;font:14px/1.8 Tahoma,Arial,sans-serif;direction:ltr;text-align:left;">${escapeHtml(csv)}</textarea>`;
}

async function showAllowedPairsPopup() {
  const bodyHtml = `
    <div class="lbk-summary">
      لیست جفت‌های دلاری مجاز از LBank دریافت می‌شود. خروجی به‌صورت comma-separated است؛ فقط جفت‌هایی نمایش داده می‌شوند که quote آن‌ها USD-like باشد.
    </div>
    <div id="lbk-allowed-pairs-loading" class="lbk-note" style="display:block;">در حال دریافت لیست...</div>
    <div id="lbk-allowed-pairs-content" style="display:none;">
      <div class="lbk-allowed-pairs-tools">
        <input id="lbk-allowed-pairs-search" type="text" placeholder="جستجو؛ مثلا BTC یا MORI" autocomplete="off" />
        <button id="lbk-allowed-pairs-refresh" type="button">به‌روزرسانی</button>
      </div>
      <div id="lbk-allowed-pairs-count" class="lbk-note" style="display:block;"></div>
      <div id="lbk-allowed-pairs-list" class="lbk-allowed-pairs-list"></div>
    </div>
  `;

  await showPopupModal({
    title: 'جفت‌های مجاز LBank',
    bodyHtml,
    submitText: 'بستن',
    cancelText: 'انصراف',
    onMount: ({ overlay }) => {
      const loading = overlay.querySelector('#lbk-allowed-pairs-loading');
      const content = overlay.querySelector('#lbk-allowed-pairs-content');
      const input = overlay.querySelector('#lbk-allowed-pairs-search');
      const refreshBtn = overlay.querySelector('#lbk-allowed-pairs-refresh');
      const countEl = overlay.querySelector('#lbk-allowed-pairs-count');
      const listEl = overlay.querySelector('#lbk-allowed-pairs-list');
      let usdPairs = [];

      const load = async (force = false) => {
        if (loading) {
          loading.style.display = 'block';
          loading.textContent = force ? 'در حال به‌روزرسانی...' : 'در حال دریافت لیست...';
        }
        if (refreshBtn) refreshBtn.disabled = true;
        try {
          if (force) {
            lbankAllowedPairsCache.cachedAtMs = 0;
            lbankAllowedPairsCache.pairs = [];
            lbankAllowedPairsCache.set = new Set();
          }
          const pairs = await getLbankAllowedPairs({ skipMarketStatsBudget: true });
          usdPairs = getUsdLikeAllowedPairs(pairs).map(item => item.pair);
          if (countEl) countEl.textContent = `${formatFaNumber(usdPairs.length)} جفت دلاری قابل‌مشاهده دریافت شد.`;
          if (content) content.style.display = 'block';
          if (loading) loading.style.display = 'none';
          renderAllowedPairsInto(listEl, usdPairs, input?.value || '');
        } catch (err) {
          if (loading) {
            loading.style.display = 'block';
            loading.textContent = `دریافت لیست انجام نشد: ${String(err?.message || err || '')}`;
          }
        } finally {
          if (refreshBtn) refreshBtn.disabled = false;
        }
      };

      if (input) {
        input.addEventListener('input', () => renderAllowedPairsInto(listEl, usdPairs, input.value));
      }
      if (refreshBtn) {
        refreshBtn.addEventListener('click', () => void load(true));
      }
      void load(false);
    },
    onSubmit: () => true,
  });
}


function apiPost(path, body = {}, options = {}) {
  return httpRequest('POST', path, body, { ...options, auth: options.auth !== false });
}


async function getWallets(options = {}) {
  const res = await apiPost('/v2/supplement/user_info_account.do', {}, options);
  const balances = Array.isArray(res?.data?.balances) ? res.data.balances : [];
  return balances.map(item => {
    const currency = String(item.asset || item.assetCode || '').toUpperCase();
    const free = num(item.free);
    const locked = num(item.locked || item.freeze);
    return {
      currency,
      asset: currency,
      balance: free + locked,
      activeBalance: free,
      available: free,
      locked,
      blockedBalance: locked,
      usdtValue: isUsdLikeQuoteCode(currency) ? free + locked : 0,
    };
  });
}


function firstLbankMarketRow(data, symbol = '') {
  const rows = Array.isArray(data)
    ? data
    : (data && typeof data === 'object' ? [data] : []);
  if (!rows.length) return null;

  const cleanSymbol = String(symbol || '').toLowerCase();
  return rows.find(row => String(row?.symbol || '').toLowerCase() === cleanSymbol) || rows[0] || null;
}

function firstPositiveNumber(...values) {
  for (const value of values) {
    const n = num(value);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return 0;
}

function firstFiniteNumber(...values) {
  for (const value of values) {
    const n = num(value);
    if (Number.isFinite(n)) return n;
  }
  return 0;
}

async function getMarketStats(base, quote, options = {}) {
  const cleanQuote = normalizeUsdQuoteCode(quote);
  const symbol = lbankSymbol(base, cleanQuote);
  spendMarketStatsRequestBudget('/v2/supplement/ticker/bookTicker.do', options);

  const requests = await Promise.allSettled([
    apiGet(`/v2/supplement/ticker/bookTicker.do?symbol=${encodeURIComponent(symbol)}`, options),
    apiGet(`/v2/supplement/ticker/price.do?symbol=${encodeURIComponent(symbol)}`, options),
    apiGet(`/v2/ticker/24hr.do?symbol=${encodeURIComponent(symbol)}`, options),
    apiGet(`/v2/etfTicker/24hr.do?symbol=${encodeURIComponent(symbol)}`, options),
  ]);

  const [book, priceTicker, spotTicker, etfTicker] = requests;
  const fulfilled = requests.filter(item => item.status === 'fulfilled');

  if (!fulfilled.length) {
    const err = requests.find(item => item.status === 'rejected')?.reason || new Error(`اطلاعات بازار ${symbol} دریافت نشد.`);
    applyMarketStatsRequestBudgetBackoff('/v2/supplement/ticker/bookTicker.do', err);
    throw err;
  }

  const bookData = book.status === 'fulfilled'
    ? (firstLbankMarketRow(book.value?.data, symbol) || {})
    : {};

  const priceData = priceTicker.status === 'fulfilled'
    ? (firstLbankMarketRow(priceTicker.value?.data, symbol) || {})
    : {};

  const spotRow = spotTicker.status === 'fulfilled'
    ? (firstLbankMarketRow(spotTicker.value?.data, symbol) || {})
    : {};
  const spotTickerData = spotRow?.ticker || {};

  const etfRow = etfTicker.status === 'fulfilled'
    ? (firstLbankMarketRow(etfTicker.value?.data, symbol) || {})
    : {};
  const etfTickerData = etfRow?.ticker || {};

  const latest = firstPositiveNumber(
    priceData.price,
    spotTickerData.latest,
    etfTickerData.latest,
    bookData.askPrice,
    bookData.bidPrice
  );

  clearMarketStatsRequestBudget();
  return {
    market: `${normalizeLbankBaseToken(base)}${quoteLabelFa(cleanQuote)}`,
    symbol,
    bestBuy: firstPositiveNumber(bookData.bidPrice, latest),
    bestSell: firstPositiveNumber(bookData.askPrice, latest),
    latest,
    mark: latest,
    dayClose: latest,
    dayChange: firstFiniteNumber(spotTickerData.change, etfTickerData.change),
    rawBook: bookData,
    rawTicker: spotTickerData,
    rawEtfTicker: etfTickerData,
    rawPriceTicker: priceData,
  };
}

async function getLbankPairRules(base, quote = 'usdt', options = {}) {
  const symbol = lbankSymbol(base, quote);
  const cached = lbankPairRulesCache.get(symbol);
  const now = Date.now();
  if (cached && now - num(cached.cachedAt) < LBANK_PAIR_RULES_CACHE_MS) {
    return cached.rules;
  }

  try {
    const res = await apiGet(`/v2/accuracy.do?symbol=${encodeURIComponent(symbol)}`, {
      ...options,
      skipMarketStatsBudget: true,
    });
    const row = Array.isArray(res?.data) ? (res.data.find(item => String(item?.symbol || '').toLowerCase() === symbol) || res.data[0]) : null;
    const rules = {
      symbol,
      minQty: Math.max(0, num(row?.minTranQua)),
      quantityAccuracy: Math.max(0, Math.min(18, Math.floor(num(row?.quantityAccuracy || AMOUNT_DECIMALS)))) || AMOUNT_DECIMALS,
      priceAccuracy: Math.max(0, Math.min(18, Math.floor(num(row?.priceAccuracy || 8)))) || 8,
      raw: row || null,
    };
    lbankPairRulesCache.set(symbol, { cachedAt: now, rules });
    return rules;
  } catch (err) {
    // v20: log accuracy.do failures so they are visible in console (previously silent)
    const errMsg = String(err?.message || err || '');
    log(`دریافت قوانین جفت LBank (accuracy.do) برای ${symbol} ناموفق بود — از حداقل بالاتر امنیت استفاده می‌شود. خطا: ${errMsg}`, 'warn');
    const rules = {
      symbol,
      minQty: 0,
      quantityAccuracy: AMOUNT_DECIMALS,
      priceAccuracy: 8,
      raw: null,
      error: errMsg,
      accuracyFailed: true, // v20: flag so safeMinUsdtOrderValue can apply a safer fallback
    };
    lbankPairRulesCache.set(symbol, { cachedAt: now, rules });
    return rules;
  }
}

// v20: When accuracy.do failed (accuracyFailed=true) and we have no real minQty, apply a
// conservative safety floor of 15 USDT so we never send a below-minimum order to LBank.
// LBank's real minimums for major pairs (BTC/USDT, ETH/USDT) are typically $10–$15 USDT.
const LBANK_MIN_ORDER_FALLBACK_USDT = 15;

function safeMinUsdtOrderValue(price, pairRules = null) {
  const p = Math.max(0, num(price));
  const minQty = Math.max(0, num(pairRules?.minQty));
  const pairMinValue = p > 0 && minQty > 0 ? minQty * p * LBANK_MIN_ORDER_QUOTE_BUFFER : 0;
  // If accuracy.do failed and we have no real minimum, use the conservative fallback
  const safeFallback = pairRules?.accuracyFailed ? LBANK_MIN_ORDER_FALLBACK_USDT : MIN_USDT_ORDER;
  return Math.max(safeFallback, pairMinValue);
}

function quantityDecimalsForPair(pairRules = null) {
  const decimals = Math.floor(num(pairRules?.quantityAccuracy));
  return Number.isFinite(decimals) && decimals >= 0 ? Math.min(18, decimals) : AMOUNT_DECIMALS;
}

function amountFloorByPairRules(value, pairRules = null) {
  return amountFloor(value, quantityDecimalsForPair(pairRules));
}

  function buildQueryString(params = {}) {
    const search = new URLSearchParams();

    for (const [key, value] of Object.entries(params || {})) {
      if (value === undefined || value === null || value === '') continue;
      search.set(key, String(value));
    }

    return search.toString();
  }

  async function apiGetAllPages(path, params, listKey, options = {}) {
    const maxPages = Math.max(1, Math.floor(num(options.maxPages || DAILY_REALIZED_PNL_MAX_PAGES)));
    const pageSize = Math.max(1, Math.min(100, Math.floor(num(options.pageSize || 100))));
    const items = [];
    let page = 1;
    let truncated = false;

    while (page <= maxPages) {
      const qs = buildQueryString({ ...(params || {}), page, pageSize });
      const res = await apiGet(`${path}${qs ? `?${qs}` : ''}`, options);
      const chunk = Array.isArray(res?.[listKey]) ? res[listKey] : [];
      items.push(...chunk);

      if (!res?.hasNext || !chunk.length) break;
      page += 1;
    }

    if (page > maxPages) {
      truncated = true;
    }

    return { items, truncated };
  }

  function tehranDateTimeParts(date = new Date()) {
    const partMap = {};
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Tehran',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).formatToParts(date);

    for (const part of parts) {
      if (part.type !== 'literal') {
        partMap[part.type] = part.value;
      }
    }

    return {
      year: partMap.year || '0000',
      month: partMap.month || '01',
      day: partMap.day || '01',
      hour: partMap.hour || '00',
      minute: partMap.minute || '00',
      second: partMap.second || '00',
    };
  }

  function tehranRangeFromMidnight() {
    const parts = tehranDateTimeParts(new Date());
    const fromLocalIso = `${parts.year}-${parts.month}-${parts.day}T00:00:00+03:30`;
    const fromMs = parseApiDateMs(fromLocalIso);
    let toMs = Date.now();

    if (!(fromMs > 0)) {
      throw new Error('شروع بازهٔ امروز تهران نامعتبر است.');
    }

    if (!(toMs > fromMs)) {
      toMs = fromMs + 1000;
    }

    const fromApiSeconds = Math.floor(fromMs / 1000);
    let toApiSeconds = Math.floor(toMs / 1000);

    if (!(toApiSeconds > fromApiSeconds)) {
      toApiSeconds = fromApiSeconds + 1;
      toMs = toApiSeconds * 1000;
    }

    return {
      dayKey: `${parts.year}-${parts.month}-${parts.day}`,
      fromIso: new Date(fromMs).toISOString(),
      toIso: new Date(toMs).toISOString(),
      fromMs,
      toMs,
      fromApiSeconds,
      toApiSeconds,
      fromLabel: '00:00',
      toLabel: `${parts.hour}:${parts.minute}`,
    };
  }

  function parseApiDateMs(value) {
    const ms = Date.parse(String(value || ''));
    return Number.isFinite(ms) ? ms : 0;
  }

  function formatTimeHmTehran(value) {
    const ms = typeof value === 'number' ? value : parseApiDateMs(value);
    if (!(ms > 0)) return '--:--';

    return new Intl.DateTimeFormat('en-GB', {
      timeZone: 'Asia/Tehran',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    }).format(new Date(ms));
  }

  function parseTradeMarket(marketText) {
    const cleaned = String(marketText || '').toUpperCase().replace(/[^A-Z0-9\-_]/g, '');

    if (cleaned.includes('-')) {
      const [baseToken, quoteToken] = cleaned.split('-');
      const base = String(baseToken || '').toUpperCase();
      const quoteRaw = String(quoteToken || '').toUpperCase();
      const quote = quoteRaw === 'USDT' ? 'usdt' : '';
      return { base, quote };
    }

    const parsed = splitSymbolAndQuote(cleaned);
    return { base: parsed.base || '', quote: parsed.quote || '' };
  }

  function parseGrossQtyFromTransactionDescription(description) {
    const cleaned = cleanText(description);
    const match = cleaned.match(/خرید\s*([0-9.,]+)\s*[A-Z0-9_]+/i);
    return match ? num(match[1]) : 0;
  }

  function normalizeTradeHistoryItem(raw) {
    const marketInfo = parseTradeMarket(raw?.market || '');
    const type = String(raw?.type || '').toLowerCase();
    const timeMs = parseApiDateMs(raw?.timestamp);
    const amount = Math.max(0, num(raw?.amount));

    if (!marketInfo.base || !marketInfo.quote || !type || !(timeMs > 0) || !(amount > 0)) {
      return null;
    }

    return {
      id: Math.max(0, num(raw?.id)),
      orderId: Math.max(0, num(raw?.orderId)),
      base: marketInfo.base,
      quote: marketInfo.quote,
      market: `${marketInfo.base}${quoteSuffixToken(marketInfo.quote)}`,
      type,
      timeMs,
      amount,
      total: Math.max(0, num(raw?.total)),
      fee: Math.max(0, num(raw?.fee)),
      raw,
    };
  }

  function normalizeHistoryTransactionItem(raw) {
    const currency = String(raw?.currency || '').toUpperCase();
    const timeMs = parseApiDateMs(raw?.created_at);
    const amount = num(raw?.amount);
    const tp = String(raw?.tp || '').toLowerCase();

    if (!currency || !(timeMs > 0)) return null;

    return {
      id: Math.max(0, num(raw?.id)),
      currency,
      timeMs,
      amount,
      tp,
      description: String(raw?.description || ''),
      grossAmountHint: parseGrossQtyFromTransactionDescription(raw?.description || ''),
      raw,
    };
  }

  function findClosestBuyTransaction(txList, trade, maxDiffMs = TRADE_TX_MATCH_WINDOW_MS) {
    if (!Array.isArray(txList) || !txList.length || !trade) return null;

    let bestIndex = -1;
    let bestAmountDiff = Infinity;
    let bestTimeDiff = Infinity;

    for (let i = 0; i < txList.length; i++) {
      const tx = txList[i];
      if (!tx || tx.used) continue;

      const timeDiff = Math.abs(num(tx.timeMs) - num(trade.timeMs));
      if (timeDiff > maxDiffMs) {
        if (num(tx.timeMs) > (num(trade.timeMs) + maxDiffMs)) break;
        continue;
      }

      const hintedQty = Math.max(0, num(tx.grossAmountHint));
      const amountDiff = hintedQty > 0 ? Math.abs(hintedQty - num(trade.amount)) : Infinity;

      if (amountDiff < bestAmountDiff || (amountDiff === bestAmountDiff && timeDiff < bestTimeDiff)) {
        bestIndex = i;
        bestAmountDiff = amountDiff;
        bestTimeDiff = timeDiff;
      }
    }

    if (bestIndex < 0) return null;

    txList[bestIndex].used = true;
    return txList[bestIndex];
  }

  function escapeHtml(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }



async function getTransactionsHistoryForRange(range, options = {}) {
  return [];
}



async function buildTodayRealizedPnlReport() {
  const openPositions = listOpenPositions();
  const rows = openPositions.map(p => ({
    base: p.base,
    quote: normalizeQuoteCode(p.quote || 'usdt'),
    accountId: p.accountId || 'primary',
    buyTimeMs: p.openedAt || p.lastBuyAt || 0,
    buyPrice: num(p.avgEntryPrice),
    buyGrossQuote: num(p.entryCostQuote),
    buyQty: num(p.qty),
    sellTimeMs: 0,
    sellPrice: 0,
    sellGrossQuote: 0,
    sellQty: 0,
    feeQuote: 0,
    pnlValue: num(p.lastPnlValue),
    pnlPercent: num(p.lastPnlPercent),
    timeMs: p.openedAt || p.lastBuyAt || 0,
    status: 'open_local_position',
  }));

  return {
    ok: true,
    source: 'local_positions_only',
    quote: 'mixed_usd',
    rows,
    totals: {
      totalBuyGross: rows.reduce((sum, r) => sum + num(r.buyGrossQuote), 0),
      totalSellGross: 0,
      totalFees: 0,
      totalPnl: rows.reduce((sum, r) => sum + num(r.pnlValue), 0),
    },
    note: 'گزارش تاریخی معاملات ال‌بانک بدون انتخاب symbol عمومی قابل جمع‌آوری مطمئن نیست؛ برای جلوگیری از فراخوانی endpointهای قدیمی ال‌بانک، گزارش از پوزیشن‌های محلی فعلی ساخته می‌شود.',
  };
}


function renderTodayRealizedPnlReport(report) {
  const rows = Array.isArray(report?.rows) ? report.rows : [];
  const totals = report?.totals || {};
  const htmlRows = rows.length
    ? rows.map((row, idx) => `
        <tr>
          <td>${formatFaNumber(idx + 1)}</td>
          <td>${escapeHtml(row.base)}</td>
          <td>${escapeHtml(accountLabel(row.accountId))}</td>
          <td>${formatAmount(row.buyQty)}</td>
          <td>${formatPrice(row.buyPrice, row.quote || 'usdt')}</td>
          <td>${formatDailyPnlSignedValue(row.pnlValue, row.quote || 'usdt')}</td>
          <td>${formatSignedPercent(row.pnlPercent)}</td>
        </tr>
      `).join('')
    : `<tr><td colspan="7">پوزیشن محلی فعالی برای گزارش پیدا نشد.</td></tr>`;

  return `
    <div class="lbk-summary">
      <div><b>گزارش سود/زیان LBank</b></div>
      <div>منبع: پوزیشن‌های محلی فعلی</div>
      <div>جمع تقریبی سود/زیان: <b>${formatDailyPnlSignedValue(totals.totalPnl, 'usdt')}</b></div>
      <div class="lbk-note" style="display:block;">${escapeHtml(report?.note || '')}</div>
    </div>
    <div style="overflow:auto; max-height:420px;">
      <table style="width:100%; border-collapse:collapse; direction:rtl; text-align:right; font-size:14px;">
        <thead>
          <tr>
            <th>#</th><th>ارز</th><th>حساب</th><th>مقدار</th><th>میانگین ورود</th><th>PnL</th><th>درصد</th>
          </tr>
        </thead>
        <tbody>${htmlRows}</tbody>
      </table>
    </div>
  `;
}

  async function showDailyRealizedPnlPopup() {
    try {
      if (!config.token) {
        log('برای مشاهده لیست سود/زیان ابتدا API اصلی ال‌بانک را ثبت کن.', 'warn');
        return;
      }

      log('در حال دریافت لیست سود/زیان امروز...', 'info');
      const report = await buildTodayRealizedPnlReport();
      const bodyHtml = renderTodayRealizedPnlReport(report);

      await showPopupModal({
        title: 'لیست سود/زیان LBank',
        bodyHtml,
        submitText: 'بستن',
        cancelText: 'انصراف',
        onSubmit: () => true,
      });
    } catch (err) {
      logError(err, 'showDailyRealizedPnlPopup');
    }
  }


// v19 fix: 'window' parameter removed from all order payloads.
// Root cause of persistent 10008 errors: LBank's supplement endpoint does NOT include
// the 'window' field when verifying the request signature server-side. The client was
// signing over {…params, window, api_key, timestamp, echostr, signature_method} but the
// server verified over {…params_without_window, api_key, timestamp, echostr, signature_method},
// producing a mismatch that LBank silently reports as error 10008 ("currency pair nonsupport")
// instead of the expected signature-error code. CCXT's lbank2 implementation never sends
// 'window' and works correctly — this confirms the fix.
//
// supplementStyle (default true): when true the quote-currency spend amount for buy_market
// goes into the 'amount' field per the v2 supplement endpoint specification; when false it
// goes into the legacy 'price' field. placeMarketOrder() tries supplementStyle=true first
// and falls back to supplementStyle=false if the endpoint returns 10008.
function buildMarketOrderPayload(side, base, quote, amount, priceHint, { supplementStyle = true, includeClientOrderId = true, pairRules = null } = {}) {
  const cleanBase = normalizeLbankBaseToken(base);
  const cleanQuote = normalizeUsdQuoteCode(quote);
  const symbol = lbankSymbol(cleanBase, cleanQuote);
  const payload = {
    symbol,
    type: side === 'buy' ? 'buy_market' : 'sell_market',
    // 'window' intentionally omitted — see v19 fix note above
  };

  if (includeClientOrderId) {
    payload.custom_id = makeClientOrderId(side, cleanBase, cleanQuote);
  }

  if (side === 'buy') {
    // quoteAmount = the USDT (or other quote) to spend; amount here is base-token qty
    const quoteAmount = Math.max(0, num(amount) * num(priceHint));
    const quoteTrimmed = trimZeroes(quoteAmount.toFixed(8));
    if (supplementStyle) {
      // v2 supplement spec: for buy_market the field is 'amount' = quote to spend
      payload.amount = quoteTrimmed;
    } else {
      // classic/legacy spec: for buy_market the field is 'price' = quote to spend
      payload.price = quoteTrimmed;
    }
  } else {
    const decimals = quantityDecimalsForPair(pairRules);
    payload.amount = trimZeroes(amountFloor(amount, decimals).toFixed(decimals));
  }

  return payload;
}

  function buildAmountCandidates(amount, preferredDecimals = AMOUNT_DECIMALS) {
    const candidates = [];
    const seen = new Set();
    const decimalChoices = [...new Set([
      Math.max(0, Math.min(18, Math.floor(num(preferredDecimals)))),
      8, 6, 5, 4, 3, 2, 1, 0,
    ])];

    for (const decimals of decimalChoices) {
      const floored = amountFloor(amount, decimals);
      if (!(floored > 0)) continue;
      const str = trimZeroes(floored.toFixed(decimals));
      if (!str || str === '0' || seen.has(str)) continue;
      seen.add(str);
      candidates.push({ decimals, amount: str });
    }

    return candidates;
  }


async function preflightLbankMarketOrderPayload(payload, options = {}) {
  if (options.skipOrderPreflight === true || LBANK_CREATE_ORDER_TEST_PREFLIGHT_ENABLED !== true) {
    return true;
  }

  try {
    spendOrderRequestBudget('/v2/supplement/create_order_test.do', options);
    await apiPost('/v2/supplement/create_order_test.do', payload, { ...options, skipOrderBudget: true });
    return true;
  } catch (err) {
    applyOrderRequestBudgetBackoff('/v2/supplement/create_order_test.do', err);
    const msg = String(err?.message || err || '');
    if (isCurrencyPairUnsupportedMessage(msg) || /not support market trade|market trading not supported|10023/i.test(msg)) {
      const symbol = normalizeLbankPairSymbol(payload?.symbol || '');
      const parts = symbol.includes('_') ? symbol.split('_') : ['', ''];
      const base = parts[0] || symbol;
      const quote = parts.slice(1).join('_') || 'usdt';
      let info = null;
      try {
        lbankAllowedPairsCache.cachedAtMs = 0;
        info = await getAllowedUsdPairInfo(base, { ...options, quote, skipMarketStatsBudget: true });
      } catch (_) {}
      let publicText = `${displayLbankPairSymbol(symbol)} برای ثبت سفارش API توسط LBank رد شد.`;
      if (info && !isAllowedUsdPairInfoExact(info, base, quote)) {
        publicText = makeUnsupportedPairMessage(base || symbol, info);
      }
      throw new Error(`${displayLbankPairSymbol(symbol)} در تست سفارش LBank رد شد. ${publicText} پاسخ LBank: ${msg}`);
    }
    throw err;
  }
}

function shouldRetryOrderPlacement(msg) {
  const text = String(msg || '');
  return /Order Validation Failed/i.test(text) ||
    /Market Validation Failed/i.test(text) ||
    /Invalid clientOrderId/i.test(text) ||
    /market order amount is less/i.test(text) ||
    /minimum transaction/i.test(text) ||
    /invalid quantity/i.test(text) ||
    /illegal quantity/i.test(text) ||
    /amount field must be passed/i.test(text) ||
    /price field must be passed/i.test(text) ||
    /amount is required/i.test(text) ||
    /quantity is required/i.test(text) ||
    /1001[123]|10020|10009|10002|37|44|48|194/.test(text) ||
    (/ParseError/i.test(text) && /clientOrderId/i.test(text));
}

function orderEndpointLabel(path) {
  return String(path || '').includes('/supplement/') ? 'supplement' : 'classic';
}

// v19: 10008 (isCurrencyPairUnsupportedMessage) is NO LONGER a fallback trigger here.
// It is handled inside placeMarketOrder by cycling through payload-style variants before
// deciding the pair is genuinely unsupported. Keeping this function for non-10008 cases.
function shouldFallbackOrderEndpoint(message) {
  const text = String(message || '');
  return /not support market trade|market trading not supported|10023/i.test(text) ||
    /Interface closed unavailable|10601/i.test(text);
}

async function ensureLbankApiCanTrade(options = {}) {
  if (options.skipApiPermissionCheck === true) return true;
  try {
    const res = await apiPost('/v2/supplement/api_Restrictions.do', {}, { ...options, skipOrderBudget: true, skipMarketStatsBudget: true });
    const data = res?.data || res || {};
    // v20: log the full restrictions so user can diagnose permission problems
    log(
      `مجوزهای API Key لب‌بانک: ` +
      `SpotTrading=${data.enableSpotTrading} | ` +
      `Withdraw=${data.enableWithdraw} | ` +
      `InternalTransfer=${data.enableInternalTransfer} | ` +
      `Futures=${data.enableFutures ?? data.enableFuture} | ` +
      `IPRestriction=${JSON.stringify(data.ipRestrict ?? data.ipWhiteList ?? 'N/A')}`,
      'info'
    );
    const enabled = data.enableSpotTrading === true || String(data.enableSpotTrading).toLowerCase() === 'true';
    if (!enabled) {
      throw new Error('API Key اجازه Spot Trading ندارد. در تنظیمات API ال‌بانک، Trading/Spot Trading را فعال کن و محدودیت IP را هم بررسی کن.');
    }
    return true;
  } catch (err) {
    const msg = String(err?.message || err || '');
    if (/Spot Trading|اجازه Spot Trading/i.test(msg)) throw err;
    log(`بررسی مجوز Spot Trading API انجام نشد؛ سفارش ادامه پیدا می‌کند. پاسخ: ${msg}`, 'warn');
    return true;
  }
}

// v19: placeMarketOrder now tries two payload styles for buy_market orders:
//   1. supplementStyle=true  → 'amount' field = USDT to spend  (v2 supplement spec)
//   2. supplementStyle=false → 'price'  field = USDT to spend  (classic/legacy spec)
// If both variants return 10008, pair validity is re-checked and a diagnostic message is shown.
// The 'window' parameter has been removed from all payloads (see buildMarketOrderPayload).
async function placeMarketOrder(side, base, quote, amount, priceHint, options = {}) {
  const accountId = String(options.accountId || 'primary');
  const cleanBase = normalizeLbankBaseToken(base);
  const cleanQuote = normalizeUsdQuoteCode(quote);
  await ensureTradableUsdPair(cleanBase, cleanQuote, { accountId });
  const pairRules = await getLbankPairRules(cleanBase, cleanQuote, { accountId });
  const candidates = side === 'buy'
    ? [amount]
    : buildAmountCandidates(amount, quantityDecimalsForPair(pairRules));

  let lastErr = null;

  for (const candidateAmount of candidates) {
    // Build the preflight payload in supplement style (first variant)
    const preflightPayload = buildMarketOrderPayload(side, cleanBase, cleanQuote, candidateAmount, priceHint, {
      supplementStyle: true,
      includeClientOrderId: true,
      pairRules,
    });

    try {
      if (LBANK_CREATE_ORDER_TEST_PREFLIGHT_ENABLED === true) {
        const buyHint = preflightPayload.amount !== undefined
          ? `بودجه (amount) ${preflightPayload.amount} ${cleanQuote.toUpperCase()}`
          : `بودجه (price) ${preflightPayload.price} ${cleanQuote.toUpperCase()}`;
        log(`تست سفارش LBank: ${preflightPayload.type} ${displayLbankPairSymbol(preflightPayload.symbol)} | ${side === 'buy' ? buyHint : `مقدار ${preflightPayload.amount} ${cleanBase.toUpperCase()}`}`, 'info');
      } else {
        log(`پیش‌بررسی تست سفارش LBank غیرفعال است؛ معیار نهایی پاسخ create_order خواهد بود.`, 'info');
      }
      await preflightLbankMarketOrderPayload(preflightPayload, { accountId });
      await ensureLbankApiCanTrade({ accountId });

      // For buy_market: try supplement style (amount) first, then classic style (price).
      // For sell_market: only supplement style applies (amount = base qty, same for both).
      const styleVariants = side === 'buy' ? [true, false] : [true];
      let orderPlaced = false;

      for (const supplementStyle of styleVariants) {
        if (orderPlaced) break;

        const payload = buildMarketOrderPayload(side, cleanBase, cleanQuote, candidateAmount, priceHint, {
          supplementStyle,
          includeClientOrderId: true,
          pairRules,
        });

        for (const orderPath of LBANK_ORDER_ENDPOINTS) {
          try {
            spendOrderRequestBudget(orderPath, options);
            const buyField = side === 'buy'
              ? (supplementStyle ? `amount(supp)=${payload.amount}` : `price(classic)=${payload.price}`)
              : `amount=${payload.amount}`;
            log(`ارسال سفارش واقعی LBank (${orderEndpointLabel(orderPath)}): ${payload.type} ${displayLbankPairSymbol(payload.symbol)} | symbol=${payload.symbol} | ${buyField}`, 'info');
            const res = await apiPost(orderPath, payload, { accountId });
            clearOrderRequestBudget();
            const data = res?.data || {};
            const id = String(data.order_id || data.orderId || data.id || payload.custom_id || '').trim();
            if (!id) throw new Error('شناسه سفارش LBank در پاسخ API وجود نداشت.');
            const quoteAmountPlaced = side === 'buy'
              ? num(supplementStyle ? payload.amount : payload.price)
              : 0;
            const order = {
              id,
              clientOrderId: String(data.custom_id || payload.custom_id || ''),
              amount: side === 'buy' ? candidateAmount : num(payload.amount),
              quoteAmount: quoteAmountPlaced,
              price: priceHint,
              symbol: data.symbol || payload.symbol,
              raw: res,
              orderEndpoint: orderPath,
            };
            orderPlaced = true;
            return { order, amount: order.amount };
          } catch (err) {
            lastErr = err;
            applyOrderRequestBudgetBackoff(orderPath, err);
            const errText = String(err?.message || err || '');

            if (isCurrencyPairUnsupportedMessage(errText)) {
              // 10008: this payload style didn't work; break out of endpoint loop
              // to try the next styleVariant (or surface error if no more variants)
              log(`payload رد شد با 10008 (${supplementStyle ? 'supplement-style' : 'classic-style'}) در endpoint ${orderEndpointLabel(orderPath)}: ${errText}`, 'warn');
              break; // break endpoint loop; outer styleVariants loop continues
            }

            if (orderPath !== LBANK_ORDER_ENDPOINTS[LBANK_ORDER_ENDPOINTS.length - 1] && shouldFallbackOrderEndpoint(errText)) {
              log(`endpoint ${orderEndpointLabel(orderPath)} سفارش را رد کرد؛ تلاش با endpoint جایگزین انجام می‌شود. پاسخ: ${errText}`, 'warn');
              continue; // try next endpoint in LBANK_ORDER_ENDPOINTS
            }
            throw err; // non-recoverable error; bubble up
          }
        }
      }

      // All style variants exhausted and still 10008 — diagnose the real cause
      if (!orderPlaced && isCurrencyPairUnsupportedMessage(String(lastErr?.message || lastErr))) {
        lbankAllowedPairsCache.cachedAtMs = 0;
        try {
          const info = await getAllowedUsdPairInfo(cleanBase, { accountId, quote: cleanQuote, skipMarketStatsBudget: true });
          if (!isAllowedUsdPairInfoExact(info, cleanBase, cleanQuote)) {
            lastErr = new Error(makeUnsupportedPairMessage(cleanBase, info));
          } else {
            // Pair IS in the allowed list yet both payload styles returned 10008.
            // Most likely the API key lacks trading permission, or its IP restriction
            // is blocking this request, or LBank has a transient server-side issue.
            const diagHint =
              'جفت معاملاتی در لیست مجاز LBank تأیید شد ولی سفارش رد شد. ' +
              'احتمالاً: (۱) کلید API اجازه Spot Trading ندارد — در پنل LBank بررسی کن؛ ' +
              '(۲) محدودیت IP روی کلید API تنظیم شده — IP مرورگر را در whitelist بگذار یا محدودیت را بردار؛ ' +
              '(۳) مشکل گذرا در سمت سرور LBank — چند لحظه دیگر دوباره امتحان کن.';
            log(diagHint, 'warn');
            lastErr = new Error(`${String(lastErr?.message || lastErr)} | ${diagHint}`);
          }
        } catch (_) {}
        throw lastErr;
      }

      if (!orderPlaced) throw lastErr || new Error('ثبت سفارش LBank انجام نشد.');
    } catch (err) {
      lastErr = err;
      const errMsg = String(err?.message || err || '');
      if (side !== 'sell' || !shouldRetryOrderPlacement(errMsg)) break;
    }
  }

  throw lastErr || new Error('ثبت سفارش LBank انجام نشد.');
}

async function getOrderStatus(orderId, options = {}) {
  const pending = state.pending?.[String(orderId)] || {};
  const symbol = pending.symbol || lbankSymbol(pending.base || options.base || '', pending.quote || options.quote || 'usdt');
  const payload = { symbol, orderId: String(orderId) };
  let res = null;
  let lastErr = null;

  for (const path of ['/v2/supplement/orders_info.do', '/v2/orders_info.do']) {
    try {
      res = await apiPost(path, payload, options);
      break;
    } catch (err) {
      lastErr = err;
      if (path.includes('/supplement/')) continue;
      throw err;
    }
  }

  if (!res && lastErr) throw lastErr;

  const data = Array.isArray(res?.data) ? (res.data[0] || {}) : (res?.data || {});
  const statusCode = Number(data.status);
  const executedQty = num(data.executedQty || data.deal_amount || data.accAmt);
  const cumulativeQuote = num(data.cummulativeQuoteQty || data.deal_money || data.accAmt);
  const avgPrice = executedQty > 0 && cumulativeQuote > 0 ? cumulativeQuote / executedQty : num(data.price || data.avgPrice || pending.estimatedPrice);
  const normalizedStatus = statusCode === 2
    ? 'Done'
    : (statusCode === -1 || statusCode === 3 ? 'Canceled' : 'Active');

  return {
    id: String(data.orderId || data.order_id || orderId),
    status: normalizedStatus,
    statusCode,
    matchedAmount: executedQty,
    averagePrice: avgPrice,
    amount: num(data.origQty || data.origQty || data.amount || pending.requestedAmount),
    quoteAmount: cumulativeQuote || num(data.origQuoteOrderQty),
    side: String(data.type || data.tradeType || pending.side || '').includes('sell') ? 'sell' : 'buy',
    raw: res,
  };
}

  function makeClientOrderId(side, base, quote) {
    const safeSide = String(side || '').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 1) || 'o';
    const safeBase = String(base || '').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 8) || 'asset';
    const safeQuote = String(quote || '').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 4) || 'q';
    const timePart = Date.now().toString(36);
    const randPart = Math.random().toString(36).slice(2, 8);
    return `${safeSide}${safeBase}${safeQuote}${timePart}${randPart}`.slice(0, 32);
  }

  /******************************************************************
   * اسکن زنده / اسکرول چت
   ******************************************************************/
  function scheduleScan(reason = 'live', force = false, delayMs = 650) {
    if (liveScanTimer) clearTimeout(liveScanTimer);
    liveScanTimer = window.setTimeout(() => {
      liveScanTimer = 0;
      scanVisibleChat(force).catch(err => logError(err, `scanVisibleChat:${reason}`));
    }, Math.max(0, delayMs | 0));
  }

  window.addEventListener('pagehide', () => {
    bgAudioLog('pagehide');
  });

  window.addEventListener('beforeunload', () => {
    cleanupBackgroundAudio();
  });

  function installLiveChatHooks() {
    if (liveObserver) return;

    const boot = () => {
      if (!document.body) return false;

      liveObserver = new MutationObserver(() => {
        lastMutationAt = Date.now();
        keepChatScrolledToBottom(false);
        scheduleScan('mutation', false, 700);
      });

      liveObserver.observe(document.body, {
        childList: true,
        subtree: true,
        characterData: true,
      });

      window.addEventListener('focus', () => {
        keepChatScrolledToBottom(true);
        scheduleScan('focus', true, 180);
      }, true);

      document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible') {
          keepChatScrolledToBottom(true);
          scheduleScan('visible', true, 180);
        }
      }, true);

      document.addEventListener('click', () => {
        cachedChatScroller = null;
        scheduleScan('click', false, 900);
      }, true);

      setInterval(() => {
        keepChatScrolledToBottom(false);
        scheduleScan('keepalive', false, 120);
      }, Math.max(1500, Math.min(config.scanIntervalMs, 3000)));

      return true;
    };

    if (!boot()) {
      const wait = window.setInterval(() => {
        if (boot()) clearInterval(wait);
      }, 500);
    }
  }

  function isElementVisible(el) {
    if (!el || !el.isConnected || el === ui.panel || el.closest('#lbk-trader-panel-fa')) return false;
    const rect = el.getBoundingClientRect();
    if (rect.width <= 0 || rect.height <= 0) return false;
    const style = window.getComputedStyle(el);
    return style.display !== 'none' && style.visibility !== 'hidden';
  }

  function isScrollableElement(el) {
    if (!isElementVisible(el)) return false;
    const style = window.getComputedStyle(el);
    if (!/(auto|scroll|overlay)/i.test(style.overflowY || '')) return false;
    return el.scrollHeight > el.clientHeight + 80;
  }

  function findBestChatScroller() {
    if (isScrollableElement(cachedChatScroller)) return cachedChatScroller;

    const pool = Array.from(document.querySelectorAll('main, section, div, article'))
      .filter(isScrollableElement)
      .sort((a, b) => (b.scrollHeight - b.clientHeight) - (a.scrollHeight - a.clientHeight))
      .slice(0, 24);

    let best = null;
    let bestScore = -1;

    for (const el of pool) {
      const text = cleanText(el.innerText || '');
      const extractedText = getExtractedVisibleChatText(el);
      const candidateText = extractedText || text;
      if (!candidateText) continue;

      let score = 0;
      if (currentPageLooksLikeTargetChat(candidateText)) score += 4000;
      if (/@?1bankbot/i.test(candidateText)) score += 3000;
      if (/\bmarket\b/i.test(candidateText)) score += 1500;
      if (/[⭐🔴🟢🔻]/.test(candidateText)) score += 1200;
      score += Math.min(1200, candidateText.length);
      score += Math.min(800, Math.round(el.clientHeight));
      score += Math.min(1800, getVisibleMessageElements(el).length * 180);

      const rect = el.getBoundingClientRect();
      if (rect.top < window.innerHeight && rect.bottom > 0) score += 250;
      if (el.closest('main, [role="main"]')) score += 350;

      if (score > bestScore) {
        bestScore = score;
        best = el;
      }
    }

    cachedChatScroller = best || null;
    return cachedChatScroller;
  }

  function getPanelFreeBodyText() {
    const bodyText = String(document.body?.innerText || '');
    const panelText = String(ui.panel?.innerText || '');
    if (!panelText) return bodyText;
    return bodyText.replace(panelText, ' ');
  }

  function extractNodeTextWithEmoji(node) {
    if (!node) return '';

    if (node.nodeType === Node.TEXT_NODE) {
      return node.nodeValue || '';
    }

    if (node.nodeType !== Node.ELEMENT_NODE) {
      return '';
    }

    const el = /** @type {HTMLElement} */ (node);
    const tag = String(el.tagName || '').toUpperCase();

    if (tag === 'IMG') {
      return el.getAttribute('alt') || '';
    }

    if (tag === 'BR') {
      return '\n';
    }

    let out = '';
    for (const child of Array.from(el.childNodes || [])) {
      out += extractNodeTextWithEmoji(child);
    }

    if (tag === 'DIV' || tag === 'P' || tag === 'LI') {
      out += '\n';
    }

    return out;
  }

  function normalizeExtractedMessageText(text) {
    return String(text || '')
      .replace(/[\t\r\f\v]+/g, ' ')
      .replace(/[ \u00A0]+\n/g, '\n')
      .replace(/\n[ \u00A0]+/g, '\n')
      .replace(/\n{3,}/g, '\n\n')
      .replace(/[ \u00A0]{2,}/g, ' ')
      .trim();
  }

  function getMessageContentRoot(messageEl) {
    return messageEl.querySelector('.KTwPFW, .YjkWXv, [data-sentry-component="BaseBubbleFC"], .message-block, .k7VAKr') || messageEl;
  }

  function extractMessageText(messageEl) {
    if (!messageEl) return '';

    const contentRoot = getMessageContentRoot(messageEl);
    const lineNodes = Array.from(contentRoot.querySelectorAll('span.p'));

    if (lineNodes.length) {
      const lines = lineNodes
        .map(node => normalizeExtractedMessageText(extractNodeTextWithEmoji(node)))
        .filter(Boolean);

      if (lines.length) {
        return lines.join('\n');
      }
    }

    return normalizeExtractedMessageText(extractNodeTextWithEmoji(contentRoot));
  }

  function getVisibleMessageElements(root = document) {
    const selector = '[aria-label="message-item"], .message-item, [data-sid][data-date]';
    const nodes = Array.from(root.querySelectorAll(selector))
      .filter(el => el instanceof Element)
      .filter(isElementVisible);

    const unique = [];
    const seen = new Set();

    for (const el of nodes) {
      if (seen.has(el)) continue;
      seen.add(el);
      unique.push(el);
    }

    unique.sort((a, b) => {
      const da = num(a.getAttribute('data-date'));
      const db = num(b.getAttribute('data-date'));
      if (da > 0 && db > 0 && da !== db) return da - db;
      if (a === b) return 0;
      const pos = a.compareDocumentPosition(b);
      if (pos & Node.DOCUMENT_POSITION_FOLLOWING) return -1;
      if (pos & Node.DOCUMENT_POSITION_PRECEDING) return 1;
      return 0;
    });

    return unique;
  }

  function extractVisibleMessages(root = document) {
    return getVisibleMessageElements(root)
      .map((el, index) => {
        const text = extractMessageText(el);
        return {
          index,
          dataDate: num(el.getAttribute('data-date')),
          text,
          lines: String(text || '').split('\n').map(cleanText).filter(Boolean),
          element: el,
        };
      })
      .filter(item => item.text && item.lines.length);
  }

  function getExtractedVisibleChatText(root = document) {
    const messages = extractVisibleMessages(root);
    return messages.length ? messages.map(m => m.text).join('\n\n') : '';
  }

  function getBestChatText() {
    const extractedText = getExtractedVisibleChatText(document);
    if (extractedText) {
      return extractedText;
    }

    const scroller = findBestChatScroller();
    const scrollerText = String(scroller?.innerText || '');

    if (scrollerText && currentPageLooksLikeTargetChat(scrollerText)) {
      return scrollerText;
    }

    const bodyText = getPanelFreeBodyText();
    if (bodyText && currentPageLooksLikeTargetChat(bodyText)) {
      return bodyText;
    }

    return scrollerText || bodyText;
  }
  function keepChatScrolledToBottom(force = false) {
    const scroller = findBestChatScroller();
    if (!scroller) return false;

    const extractedText = getExtractedVisibleChatText(scroller);
    const text = cleanText(extractedText || scroller.innerText || '');
    if (!text || !currentPageLooksLikeTargetChat(text)) return false;

    const distanceToBottom = Math.max(0, scroller.scrollHeight - scroller.clientHeight - scroller.scrollTop);
    if (!force && distanceToBottom <= 8) return true;

    scroller.scrollTop = scroller.scrollHeight;
    return true;
  }

  /******************************************************************
   * تشخیص چت و استخراج بلوک‌های پیام
   ******************************************************************/
  function currentPageLooksLikeTargetChat(text) {
    const t = String(config.targetChatName || '').toLowerCase();
    const title = String(document.title || '').toLowerCase();
    const body = String(text || '').toLowerCase();
    return title.includes(t) || body.includes(`@${t}`) || body.includes(t) || looksLikeSignalFeedText(text);
  }

  function isShortRadarTimestampLine(line) {
    const compact = cleanText(line).replace(/\s+/g, '');
    return /^(?:\d{2}_\d{2}::\d{2}_\d{2}|\d{4}::\d{4})(?:AM|PM)$/i.test(compact);
  }

  function isIsoTimestampLine(line) {
    return /^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$/.test(cleanText(line));
  }

  function isAnyTimestampLine(line) {
    const c = cleanText(line);
    return isShortRadarTimestampLine(c) || isIsoTimestampLine(c);
  }

  function getLatestRadarBlock(lines) {
    for (let i = lines.length - 1; i >= 0; i--) {
      const line = cleanText(lines[i]);
      if (!isShortRadarTimestampLine(line)) continue;

      const blockLines = [line];
      for (let j = i + 1; j < lines.length; j++) {
        const next = cleanText(lines[j]);
        if (isAnyTimestampLine(next)) break;
        blockLines.push(next);
        if (blockLines.length >= 90) break;
      }

      const text = blockLines.join('\n');
      if (text.includes('⭐') || text.includes('🔴')) {
        return {
          timestampText: line,
          lines: blockLines,
          text,
          blockKey: hashString(`radar|${line}|${text}`),
        };
      }
    }
    return null;
  }

  function getLatestStrategyBlock(lines) {
    for (let i = lines.length - 1; i >= 0; i--) {
      const line = cleanText(lines[i]);
      if (!isIsoTimestampLine(line) && !isShortRadarTimestampLine(line)) continue;

      const blockLines = [line];
      for (let j = i - 1; j >= 0; j--) {
        const prev = cleanText(lines[j]);
        if (isAnyTimestampLine(prev)) break;
        blockLines.unshift(prev);
        if (blockLines.length >= 90) break;
      }

      const text = blockLines.join('\n');
      const hasMarket = /\bmarket\b/i.test(text);
      const hasSignal = blockLines.some(lineLooksLikeStrategySignal);

      if (hasSignal && (hasMarket || /خرید|فروش/.test(text))) {
        return {
          timestampText: line,
          lines: blockLines,
          text,
          blockKey: hashString(`strategy|${line}|${text}`),
        };
      }
    }
    return null;
  }

  function getLatestRadarBlockFromMessages(messages) {
    for (let i = messages.length - 1; i >= 0; i--) {
      const msg = messages[i];
      const lines = (msg?.lines || []).map(cleanText).filter(Boolean);
      if (!lines.length) continue;

      const timestampText = lines.find(isShortRadarTimestampLine) || lines.find(isAnyTimestampLine) || '';
      if (!timestampText) continue;

      const text = lines.join('\n');
      if (text.includes('⭐') || text.includes('🔴')) {
        return {
          timestampText,
          lines,
          text,
          blockKey: hashString(`radar-msg|${timestampText}|${text}`),
        };
      }
    }
    return null;
  }

  function getLatestStrategyBlockFromMessages(messages) {
    for (let i = messages.length - 1; i >= 0; i--) {
      const msg = messages[i];
      const lines = (msg?.lines || []).map(cleanText).filter(Boolean);
      if (!lines.length) continue;

      const timestampText = lines.find(isAnyTimestampLine) || '';
      if (!timestampText) continue;

      const text = lines.join('\n');
      const hasMarket = /\bmarket\b/i.test(text);
      const hasSignal = lines.some(lineLooksLikeStrategySignal);

      if (hasSignal && (hasMarket || /خرید|فروش/.test(text))) {
        return {
          timestampText,
          lines,
          text,
          blockKey: hashString(`strategy-msg|${timestampText}|${text}`),
        };
      }
    }
    return null;
  }

  function rememberLatestSeenMessage({ type, timestampText, rawText, signalKey, base, quote }) {
    state.latestSeenMessage = {
      type: type || '',
      timestampText: timestampText || '',
      rawText: rawText || '',
      signalKey: signalKey || '',
      processedAtMs: Date.now(),
      processedAtIso: new Date().toISOString(),
      base: base || '',
      quote: quote || '',
    };
  }

  async function scanVisibleChat(force = false) {
    if (scanBusy) {
      return { ok: false, reason: 'اسکن قبلی هنوز در حال اجراست.' };
    }
    scanBusy = true;

    try {
      ensureDayReset();
      keepChatScrolledToBottom(force);

      const visibleMessages = extractVisibleMessages(document);
      const raw = visibleMessages.length
        ? visibleMessages.map(m => m.text).join('\n\n')
        : getBestChatText();
      if (!raw.trim()) return { ok: false, reason: 'متن قابل اسکن پیدا نشد.' };

      if (!currentPageLooksLikeTargetChat(raw)) {
        if (force) log(`به نظر نمی‌رسد چت @${config.targetChatName} باز باشد.`, 'warn');
        return { ok: false, reason: `چت @${config.targetChatName} تشخیص داده نشد.` };
      }

      const lines = raw
        .split('\n')
        .map(cleanText)
        .filter(Boolean);

      if (!lines.length) return { ok: false, reason: 'خط قابل پردازش پیدا نشد.' };

      const radarBlock = visibleMessages.length
        ? (getLatestRadarBlockFromMessages(visibleMessages) || getLatestRadarBlock(lines))
        : getLatestRadarBlock(lines);
      const strategyBlock = visibleMessages.length
        ? (getLatestStrategyBlockFromMessages(visibleMessages) || getLatestStrategyBlock(lines))
        : getLatestStrategyBlock(lines);

      if (!radarBlock) {
        if (force) log('آخرین پیام رادار پیدا نشد.', 'warn');
      }

      if (!strategyBlock) {
        if (force) log('آخرین پیام استراتژی پیدا نشد.', 'warn');
      }

      if (radarBlock) {
        state.latestRadar = {
          blockKey: radarBlock.blockKey,
          timestampText: radarBlock.timestampText,
          text: radarBlock.text,
          lines: radarBlock.lines,
          updatedAtMs: Date.now(),
          updatedAtIso: new Date().toISOString(),
        };
        state.radarMeta = buildRadarMetaMap(radarBlock);
      } else {
        state.radarMeta = {};
      }

      let strategySignals = { buys: [], sells: [], ignoredBuys: [] };

      if (strategyBlock) {
        state.latestStrategy = {
          blockKey: strategyBlock.blockKey,
          timestampText: strategyBlock.timestampText,
          text: strategyBlock.text,
          lines: strategyBlock.lines,
          updatedAtMs: Date.now(),
          updatedAtIso: new Date().toISOString(),
        };

        strategySignals = parseStrategyBlock(strategyBlock);
      }

      state.currentIgnoredBuys = strategySignals.ignoredBuys || [];
      state.currentSellList = dedupeSignalsKeepBest(strategySignals.sells || []);
      state.currentPriorityList = buildCurrentPriorityList(strategySignals.buys || [], state.radarMeta);

      trimProcessedSignals();

      const newSellSignals = [];
      for (const s of state.currentSellList) {
        const signalKey = hashString(`${state.latestStrategy.timestampText}|sell|${s.rawLine}`);
        if (!state.processedSignals[signalKey]) {
          state.processedSignals[signalKey] = Date.now();
          rememberLatestSeenMessage({
            type: 'sell',
            timestampText: state.latestStrategy.timestampText,
            rawText: s.rawLine,
            signalKey,
            base: s.base,
            quote: s.quote || '',
          });
          newSellSignals.push({ ...s, signalKey });
        }
      }

      const newBuySignals = [];
      for (const s of state.currentPriorityList) {
        const signalKey = hashString(`${state.latestStrategy.timestampText}|buy|${s.rawLine}`);
        if (!state.processedSignals[signalKey]) {
          state.processedSignals[signalKey] = Date.now();
          rememberLatestSeenMessage({
            type: 'buy',
            timestampText: state.latestStrategy.timestampText,
            rawText: s.rawLine,
            signalKey,
            base: s.base,
            quote: s.quote || '',
          });
          newBuySignals.push({ ...s, signalKey });
        }
      }

      if (newSellSignals.length || newBuySignals.length) {
        await playNewSignalAlarm(newSellSignals, newBuySignals);
      }

      if (newSellSignals.length) {
        log(`فروش‌های جدید: ${newSellSignals.map(s => `${s.base}(${quoteLabelFa(s.quote)})`).join('، ')}`, 'warn');
        await handleSellSignals(newSellSignals);
      }

      if (newSellSignals.length && newBuySignals.length) {
        await waitBetweenTradeActions('پایان فروش‌ها و شروع خریدها');
      }

      if (newBuySignals.length) {
        log(`خریدهای جدید: ${newBuySignals.map(s => `${s.base}(${quoteLabelFa(s.quote)})`).join('، ')}`, 'ok');
        await handleBuySignals(newBuySignals);
      }

      state.lastScanAt = Date.now();
      saveState();
      cachedChatScroller = findBestChatScroller();
      return {
        ok: true,
        radarFound: !!radarBlock,
        strategyFound: !!strategyBlock,
        buyCount: state.currentPriorityList.length,
        sellCount: state.currentSellList.length,
        newBuyCount: newBuySignals.length,
        newSellCount: newSellSignals.length,
      };
    } catch (err) {
      logError(err, 'scanVisibleChat');
      return { ok: false, reason: String(err?.message || err || 'خطای نامشخص') };
    } finally {
      scanBusy = false;
    }
  }

  function trimProcessedSignals() {
    const keys = Object.keys(state.processedSignals);
    if (keys.length <= config.maxProcessedKeys) return;
    keys.sort((a, b) => state.processedSignals[b] - state.processedSignals[a]);
    const keep = new Set(keys.slice(0, config.maxProcessedKeys));
    for (const k of keys) {
      if (!keep.has(k)) delete state.processedSignals[k];
    }
  }

  function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
  }

  async function waitBetweenTradeActions(reasonText = '', delayMs = ACTION_DELAY_MS) {
    const safeDelayMs = Math.max(0, Number(delayMs) || 0);
    const suffix = reasonText ? ` (${reasonText})` : '';
    const waitText = safeDelayMs >= 60_000
      ? `${formatFaNumber(Math.round(safeDelayMs / 60_000))} دقیقه`
      : `${formatFaNumber(Math.round(safeDelayMs / 1000))} ثانیه`;

    log(`${waitText} مکث بین اقدام‌ها${suffix}`, 'info');
    await sleep(safeDelayMs);
  }

  /******************************************************************
   * پارس پیام‌ها
   ******************************************************************/

function splitSymbolAndQuote(token, explicitQuote = null) {
  const raw = toAsciiDigits(String(token || '')).trim();
  const cleaned = raw.replace(/[()\[\]{}]/g, '').replace(/[-–—]/g, '_').replace(/\s+/g, '').toUpperCase();
  if (!cleaned) return null;

  const fallbackQuote = normalizeUsdQuoteCode(explicitQuote || 'usdt');
  let base = cleaned;
  let quote = fallbackQuote;
  let quoteExplicit = false;

  const separated = cleaned.split(/[_/]/).filter(Boolean);
  if (separated.length >= 2) {
    const maybeQuote = separated[separated.length - 1];
    if (isUsdLikeQuoteCode(maybeQuote)) {
      quote = normalizeUsdQuoteCode(maybeQuote);
      base = separated.slice(0, -1).join('');
      quoteExplicit = true;
    }
  }

  if (!quoteExplicit) {
    const quoteCandidates = ['FDUSD', 'USDT', 'USDC', 'USDE', 'USDD', 'USDP', 'BUSD', 'TUSD', 'USDS', 'USDK', 'USDX', 'USD']
      .sort((a, b) => b.length - a.length);
    for (const candidate of quoteCandidates) {
      if (base.length > candidate.length && base.endsWith(candidate)) {
        quote = normalizeUsdQuoteCode(candidate);
        base = base.slice(0, -candidate.length);
        quoteExplicit = true;
        break;
      }
    }
  }

  base = String(base || '').replace(/[_/]+$/g, '');
  if (!base) return null;

  return { base: normalizeLbankBaseToken(base), quote, quoteExplicit };
}


function buildParsedBuySignal(base, quote, rawLine, patch = {}) {
  const normalizedQuote = normalizeUsdQuoteCode(quote);
  return {
    side: 'buy',
    base: normalizeLbankBaseToken(base),
    quote: normalizedQuote,
    rawLine,
    signalKey: patch.signalKey || `${normalizeLbankBaseToken(base)}|${normalizedQuote}|${hashString(rawLine)}`,
    signalFlavor: patch.signalFlavor || 'normal_buy',
    priorityHint: patch.priorityHint || 0,
    ignored: !!patch.ignored,
    reason: patch.reason || '',
    star: !!patch.star,
    red: !!patch.red,
    streak: Math.max(1, num(patch.streak) || 1),
  };
}


function parseBuyLine(line) {
  if (!/خرید/.test(line) || /فروش|معلق|احتمال|پامپ/.test(line)) return null;
  const m = line.match(/(?:🟢|✅|🟩)?\s*([A-Za-z0-9_\-\/]+)(?:\s*\(([A-Z0-9]*USD[A-Z0-9]*)\))?/i);
  if (!m) return null;
  const parsed = splitSymbolAndQuote(m[1], m[2] || 'usdt');
  if (!parsed) return null;
  return buildParsedBuySignal(parsed.base, parsed.quote, line, { signalFlavor: 'normal_buy' });
}


function parsePumpBuyLine(line) {
  if (!/خرید\s*هنگام\s*پامپ|پامپ/.test(line)) return null;
  const m = line.match(/(?:✴️|⭐|🟣)?\s*([A-Za-z0-9_\-\/]+)(?:\s*\(([A-Z0-9]*USD[A-Z0-9]*)\))?/i);
  if (!m) return null;
  const parsed = splitSymbolAndQuote(m[1], m[2] || 'usdt');
  if (!parsed) return null;
  return buildParsedBuySignal(parsed.base, parsed.quote, line, { signalFlavor: 'pump_buy', priorityHint: 10 });
}


function parsePendingBuyLine(line) {
  if (!/خرید\s*معلق/.test(line)) return null;
  const m = line.match(/(?:🔸|🟠)?\s*([A-Za-z0-9_\-\/]+)(?:\s*\(([A-Z0-9]*USD[A-Z0-9]*)\))?/i);
  if (!m) return null;
  const parsed = splitSymbolAndQuote(m[1], m[2] || 'usdt');
  if (!parsed) return null;
  return buildParsedBuySignal(parsed.base, parsed.quote, line, { ignored: true, reason: 'خرید معلق فقط نمایش داده شد.', signalFlavor: 'pending_buy' });
}


function parseProbableBuyLine(line) {
  if (!/احتمال\s*خرید/.test(line)) return null;
  const m = line.match(/(?:🟡|⚪)?\s*([A-Za-z0-9_\-\/]+)(?:\s*\(([A-Z0-9]*USD[A-Z0-9]*)\))?/i);
  if (!m) return null;
  const parsed = splitSymbolAndQuote(m[1], m[2] || 'usdt');
  if (!parsed) return null;
  return buildParsedBuySignal(parsed.base, parsed.quote, line, { ignored: true, reason: 'احتمال خرید فقط نمایش داده شد.', signalFlavor: 'probable_buy' });
}


function parseSellLine(line) {
  if (!/فروش/.test(line)) return null;
  const m = line.match(/(?:🔻|🔴|🟥)?\s*([A-Za-z0-9_\-\/]+)(?:\s*\(([A-Z0-9]*USD[A-Z0-9]*)\))?/i);
  if (!m) return null;
  const parsed = splitSymbolAndQuote(m[1], m[2] || 'usdt');
  if (!parsed) return null;
  const normalizedQuote = normalizeUsdQuoteCode(parsed.quote);
  return {
    side: 'sell',
    base: normalizeLbankBaseToken(parsed.base),
    quote: normalizedQuote,
    rawLine: line,
    signalKey: `${normalizeLbankBaseToken(parsed.base)}|${normalizedQuote}|sell|${hashString(line)}`,
  };
}

  function lineLooksLikeStrategySignal(line) {
    return !!parseBuyLine(line) ||
      !!parsePumpBuyLine(line) ||
      !!parsePendingBuyLine(line) ||
      !!parseProbableBuyLine(line) ||
      !!parseSellLine(line);
  }

  function parseStreak(line) {
    const m = cleanText(line).match(/\bx\s*(\d+)\b/i);
    return m ? Math.max(1, Number(m[1])) : 1;
  }

  function buildRadarMetaMap(radarBlock) {
    const meta = {};
    const lines = radarBlock?.lines || [];
    let orderIndex = 0;

    for (const line of lines) {
      const c = cleanText(line);
      const streak = parseStreak(c);

      if (c.includes('⭐')) {
        const m = c.match(/⭐\s*([A-Z0-9_]+)/i);
        if (m) {
          const { base } = splitSymbolAndQuote(m[1]);
          if (base) touchMetaMap(meta, base, { star: true, streak, orderIndex });
        }
      }

      if (c.includes('🔴')) {
        let m = c.match(/🔴\s*([A-Z0-9_]+)/i);
        if (!m) m = c.match(/-\s*([A-Z0-9_]+)\s*🔴/i);
        if (m) {
          const { base } = splitSymbolAndQuote(m[1]);
          if (base) touchMetaMap(meta, base, { red: true, streak, orderIndex });
        }
      }

      orderIndex += 1;
    }

    return meta;
  }

  function touchMetaMap(meta, base, patch) {
    const key = base.toUpperCase();
    const curr = meta[key] || {
      star: false,
      red: false,
      streak: 1,
      orderIndex: 9999,
    };

    curr.star = curr.star || !!patch.star;
    curr.red = curr.red || !!patch.red;
    curr.streak = Math.max(curr.streak || 1, patch.streak || 1);
    curr.orderIndex = Math.min(curr.orderIndex ?? 9999, patch.orderIndex ?? 9999);

    meta[key] = curr;
  }

  function priorityScoreFromRadar(base, radarMeta) {
    const m = radarMeta[base.toUpperCase()] || { star: false, red: false, streak: 1, orderIndex: 9999 };
    let score = 0;

    if (m.star && m.red) score += 300000;
    else if (m.star) score += 200000;
    else if (m.red) score += 100000;

    score += (m.streak || 1) * 100;
    score += Math.max(0, 1000 - (m.orderIndex || 9999));

    return score;
  }

  function parseStrategyBlock(strategyBlock) {
    const buys = [];
    const sells = [];
    const ignoredBuys = [];

    for (const line of strategyBlock?.lines || []) {
      const buy = parseBuyLine(line);
      if (buy) {
        if (buy.ignored) {
          ignoredBuys.push({ base: buy.base, reason: buy.reason });
        } else {
          buys.push(buy);
        }
        continue;
      }

      const pumpBuy = parsePumpBuyLine(line);
      if (pumpBuy) {
        if (pumpBuy.ignored) {
          ignoredBuys.push({ base: pumpBuy.base, reason: pumpBuy.reason });
        } else {
          buys.push(pumpBuy);
        }
        continue;
      }

      const pendingBuy = parsePendingBuyLine(line);
      if (pendingBuy) {
        ignoredBuys.push({
          base: pendingBuy.base,
          reason: 'خرید معلق است و فعلاً اقدامی روی آن انجام نمی‌شود',
        });
        continue;
      }

      const probableBuy = parseProbableBuyLine(line);
      if (probableBuy) {
        ignoredBuys.push({
          base: probableBuy.base,
          reason: 'احتمال خرید است و سیگنال قطعی خرید نیست',
        });
        continue;
      }

      const sell = parseSellLine(line);
      if (sell) {
        sells.push(sell);
      }
    }

    return { buys, sells, ignoredBuys };
  }

  function buildCurrentPriorityList(buys, radarMeta) {
    const unique = dedupeSignalsKeepBest(buys);

    return unique
      .map(s => {
        const meta = radarMeta[s.base.toUpperCase()] || { star: false, red: false, streak: 1 };
        return {
          ...s,
          star: !!meta.star,
          red: !!meta.red,
          streak: meta.streak || 1,
          score: priorityScoreFromRadar(s.base, radarMeta),
        };
      })
      .sort((a, b) => b.score - a.score);
  }
  /******************************************************************
   * پوزیشن‌ها / کیف پول
   ******************************************************************/

function walletMap(wallets) {
  const out = {};
  (wallets || []).forEach(w => {
    const currency = walletCurrencyUpper(w);
    if (!currency) return;
    out[currency] = w;
  });
  return out;
}


function walletAvailable(walletsMap, currency) {
  const key = String(currency || '').toUpperCase();
  const w = walletsMap[key];
  if (!w) return 0;
  return num(w.activeBalance ?? w.available ?? w.free ?? w.balance);
}

  function amountFloor(value, decimals = AMOUNT_DECIMALS) {
    const factor = 10 ** Math.max(0, decimals | 0);
    return Math.floor(num(value) * factor) / factor;
  }


function minOrderValueForQuote(quote) {
  return MIN_USDT_ORDER;
}

  async function continueManualBuyRemainingBalance(base, quote, options = {}) {
    const upperBase = normalizeLbankBaseToken(base);
    const normalizedQuote = normalizeQuoteCode(quote);
    const accountId = String(options.accountId || 'primary');
    const sweepIteration = Math.max(0, num(options.sweepIteration));
    const sequenceId = String(options.sequenceId || '');
    const retryEntry = getRetryBuy(upperBase, normalizedQuote, accountId);

    if (!sequenceId) {
      if (retryEntry) {
        upsertRetryBuy(retryEntry, {
          autoSweepComplete: true,
          autoSweepStopReason: 'شناسه دنبالهٔ خرید دستیِ تمام‌موجودی نامعتبر بود.',
          status: num(retryEntry.filledAmount) > 0 ? 'filled' : retryEntry.status,
        });
      }
      saveState();
      return false;
    }

    const guardBeforeStart = evaluateManualBuySessionGuard(retryEntry || options);
    if (guardBeforeStart.enabled && !guardBeforeStart.ok) {
      markManualBuySessionStopped(
        upperBase,
        normalizedQuote,
        guardBeforeStart.meta.manualSessionId,
        guardBeforeStart.reason,
        {
          markCancelled: guardBeforeStart.meta.manualSessionCancelledAt > 0,
          status: guardBeforeStart.meta.manualSessionCancelledAt > 0
            ? 'cancelled'
            : (num(retryEntry?.filledAmount) > 0 ? 'filled' : 'expired'),
        }
      );
      saveState();
      return false;
    }

    if (sweepIteration >= MANUAL_BUY_SWEEP_MAX_ATTEMPTS) {
      log(`خرید دستی ${upperBase} (${quoteLabelFa(normalizedQuote)}) به سقف ${formatFaNumber(MANUAL_BUY_SWEEP_MAX_ATTEMPTS)} تلاش برای مصرف باقیمانده رسید.`, 'warn');
      if (retryEntry) {
        upsertRetryBuy(retryEntry, {
          autoSweepRemainingBalance: true,
          autoSweepSequenceId: sequenceId,
          autoSweepIteration: sweepIteration,
          autoSweepComplete: true,
          autoSweepStopReason: 'به سقف تعداد تلاش رسید.',
          status: num(retryEntry.filledAmount) > 0 ? 'filled' : retryEntry.status,
        });
      }
      saveState();
      return false;
    }

    if (hasPendingBuy(upperBase, normalizedQuote, accountId)) {
      return false;
    }

    const minValue = minOrderValueForQuote(normalizedQuote);
    const wallets = await getWallets({ accountId });
    if (accountId === 'primary') {
      setBalanceSummary(wallets);
    }

    const wm = walletMap(wallets);
    const remainingBalance = Math.max(0, walletAvailable(wm, normalizedQuote));

    if (remainingBalance < minValue) {
      const balanceText = formatQuoteValue(remainingBalance, normalizedQuote);
      log(`باقی‌مانده ${quoteLabelFa(normalizedQuote)} برای خرید دستی ${upperBase} به حداقل بازار نمی‌رسد؛ ادامه متوقف شد. باقی‌مانده: ${balanceText}`, 'info');
      if (retryEntry) {
        upsertRetryBuy(retryEntry, {
          autoSweepRemainingBalance: true,
          autoSweepSequenceId: sequenceId,
          autoSweepIteration: sweepIteration,
          autoSweepComplete: true,
          autoSweepStopReason: `باقی‌مانده زیر حداقل سفارش است: ${balanceText}`,
          status: num(retryEntry.filledAmount) > 0 ? 'filled' : retryEntry.status,
          lastError: '',
        });
      }
      saveState();
      return false;
    }

    await waitBetweenTradeActions(`تلاش دوباره خرید دستیِ باقیمانده ${upperBase}`, BUY_TRY_DELAY_MS);

    const refreshedRetryEntry = getRetryBuy(upperBase, normalizedQuote, accountId) || retryEntry;
    const guardAfterDelay = evaluateManualBuySessionGuard(refreshedRetryEntry || options);
    if (guardAfterDelay.enabled && !guardAfterDelay.ok) {
      markManualBuySessionStopped(
        upperBase,
        normalizedQuote,
        guardAfterDelay.meta.manualSessionId,
        guardAfterDelay.reason,
        {
          markCancelled: guardAfterDelay.meta.manualSessionCancelledAt > 0,
          status: guardAfterDelay.meta.manualSessionCancelledAt > 0
            ? 'cancelled'
            : (num(refreshedRetryEntry?.filledAmount) > 0 ? 'filled' : 'expired'),
        }
      );
      saveState();
      return false;
    }

    const balanceText = formatQuoteValue(remainingBalance, normalizedQuote);
    log(`باقی‌مانده خرید دستی ${upperBase} (${quoteLabelFa(normalizedQuote)}) هنوز ${balanceText} است؛ سفارش بعدی ثبت می‌شود.`, 'warn');

    const signal = {
      side: 'buy',
      base: upperBase,
      quote: normalizedQuote,
      rawLine: `manual-buy-sweep|${upperBase}${quoteSuffixToken(normalizedQuote)}|${remainingBalance}|${sequenceId}|${sweepIteration + 1}`,
      signalKey: makeManualSignalKey('buy-sweep', upperBase, normalizedQuote),
      score: 0,
    };

    await executeBuy(signal, remainingBalance, {
      accountId,
      reasonLabel: 'manual_buy',
      allowWhenDryRunOff: true,
      autoSweepRemainingBalance: true,
      autoSweepSequenceId: sequenceId,
      autoSweepIteration: sweepIteration + 1,
      manualSessionId: guardAfterDelay.meta.manualSessionId,
      manualSessionStartedAt: guardAfterDelay.meta.manualSessionStartedAt,
      manualSessionExpiresAt: guardAfterDelay.meta.manualSessionExpiresAt,
      manualSessionCancelledAt: guardAfterDelay.meta.manualSessionCancelledAt,
      manualSessionStopReason: guardAfterDelay.meta.manualSessionStopReason,
    });

    return true;
  }


function reserveQtyForUsdtValue(walletQty, reservePriceUsdt) {
  const qty = Math.max(0, num(walletQty));
  const price = Math.max(0, num(reservePriceUsdt));
  if (!(qty > 0) || !(price > 0)) return 0;
  return Math.min(qty, SELL_KEEP_MAX_USDT_VALUE / price);
}

  function protectedSellQty(walletQty, desiredQty, reserveQty = 0) {
    const safeWalletQty = Math.max(0, num(walletQty));
    const safeDesiredQty = Math.max(0, num(desiredQty));
    const safeReserveQty = Math.max(0, num(reserveQty));

    if (safeWalletQty <= 0 || safeDesiredQty <= 0) return 0;

    const walletCap = amountFloor(Math.max(0, safeWalletQty - safeReserveQty));
    const sellQty = amountFloor(Math.min(safeDesiredQty, walletCap));

    return sellQty > 0 ? sellQty : 0;
  }

  function clearForceSellSkip(position) {
    if (!position) return;
    position.forceSellSkipReason = '';
    position.forceSellSkipAt = 0;
  }

  function clearForceSellState(position) {
    if (!position) return;
    position.openedAt = 0;
    clearForceSellSkip(position);
  }

  function markForceSellSkip(position, reason) {
    if (!position) return;
    position.forceSellSkipReason = String(reason || '');
    position.forceSellSkipAt = Date.now();
    position.lastUpdated = Date.now();
  }

  function sameForceSellSkip(position, reason) {
    if (!position) return false;
    return String(position.forceSellSkipReason || '') === String(reason || '');
  }

  function posKey(base, quote, accountId = 'primary') {
    const normalizedBase = normalizeLbankBaseToken(base);
    const normalizedQuote = normalizeQuoteCode(quote);
    return `${String(accountId || 'primary')}::${normalizedBase}|${normalizedQuote}`;
  }

  function listOpenPositions() {
    return Object.values(state.positions || {}).filter(p => num(p?.qty) > 0);
  }

  function findOpenPositionsForMarket(base, quote = null) {
    const safeBase = normalizeLbankBaseToken(base);
    const safeQuote = quote === null ? null : normalizeQuoteCode(quote);
    return Object.values(state.positions || {}).filter(p => {
      if (!canonicalBaseEquals(p?.base, safeBase)) return false;
      if (!(num(p?.qty) > 0)) return false;
      if (safeQuote !== null && normalizeQuoteCode(p?.quote) !== safeQuote) return false;
      return true;
    });
  }

  function getPosition(base, quote, accountId = 'primary') {
    const key = posKey(base, quote, accountId);
    const existing = state.positions[key] || {};

    state.positions[key] = {
      base: normalizeLbankBaseToken(existing.base || base || ''),
      quote: normalizeQuoteCode(existing.quote || quote || ''),
      accountId: String(existing.accountId || accountId || 'primary'),
      qty: num(existing.qty),
      buyCount: num(existing.buyCount),
      sellCount: num(existing.sellCount),
      openedAt: num(existing.openedAt),
      lastBuyAt: num(existing.lastBuyAt),
      lastSellAt: num(existing.lastSellAt),
      forceSellSkipReason: String(existing.forceSellSkipReason || ''),
      forceSellSkipAt: num(existing.forceSellSkipAt),
      lastUpdated: num(existing.lastUpdated) || Date.now(),
      entryCostQuote: Math.max(0, num(existing.entryCostQuote)),
      avgEntryPrice: Math.max(0, num(existing.avgEntryPrice)),
      realizedPnlQuote: num(existing.realizedPnlQuote),
      lastKnownPrice: Math.max(0, num(existing.lastKnownPrice)),
      lastMarketValue: Math.max(0, num(existing.lastMarketValue)),
      lastNetValue: Math.max(0, num(existing.lastNetValue)),
      lastPnlValue: num(existing.lastPnlValue),
      lastPnlPercent: num(existing.lastPnlPercent),
      lastPnlAt: num(existing.lastPnlAt),
      entryCostMode: String(existing.entryCostMode || (num(existing.qty) > 0 && num(existing.entryCostQuote) <= 0 ? 'unknown' : 'tracked')),
    };

    return state.positions[key];
  }

  function applyPositionDelta(side, base, quote, deltaQty, meta = {}) {
    const accountId = String(meta.accountId || 'primary');
    const p = getPosition(base, quote, accountId);
    const beforeQty = num(p.qty);
    const safeDelta = Math.max(0, num(deltaQty));
    const effectivePrice = Math.max(0, num(meta.price));

    if (safeDelta <= 0) return;

    if (side === 'buy') {
      p.qty = beforeQty + safeDelta;
      p.buyCount += 1;
      p.lastBuyAt = Date.now();
      clearForceSellSkip(p);

      if ((beforeQty <= 0 || !p.openedAt) && p.qty > 0) {
        p.openedAt = Date.now();
      }

      if (effectivePrice > 0) {
        p.entryCostQuote = Math.max(0, num(p.entryCostQuote) + (safeDelta * effectivePrice));
        p.avgEntryPrice = p.qty > 0 ? (p.entryCostQuote / p.qty) : 0;
        p.lastKnownPrice = effectivePrice;
        p.entryCostMode = 'tracked';
      } else if (beforeQty <= 0 && num(p.entryCostQuote) <= 0) {
        p.entryCostMode = 'unknown';
      }
    } else {
      const sellQty = Math.min(beforeQty, safeDelta);
      const avgEntry = beforeQty > 0 ? (num(p.entryCostQuote) / beforeQty) : 0;
      const reducedCost = Math.min(num(p.entryCostQuote), sellQty * avgEntry);
      const saleValue = effectivePrice > 0 ? sellQty * effectivePrice : 0;

      p.qty = Math.max(0, beforeQty - sellQty);
      p.sellCount += 1;
      p.lastSellAt = Date.now();
      clearForceSellState(p);

      if (num(p.entryCostQuote) > 0) {
        p.entryCostQuote = Math.max(0, num(p.entryCostQuote) - reducedCost);
      }
      p.avgEntryPrice = p.qty > 0 && num(p.entryCostQuote) > 0 ? (num(p.entryCostQuote) / p.qty) : 0;

      if (saleValue > 0 && reducedCost > 0) {
        p.realizedPnlQuote = num(p.realizedPnlQuote) + (saleValue - reducedCost - feeValueForQuote(saleValue, quote));
        p.lastKnownPrice = effectivePrice;
      }

      if (p.qty <= 0) {
        p.entryCostQuote = 0;
        p.avgEntryPrice = 0;
        p.lastMarketValue = 0;
        p.lastNetValue = 0;
        p.lastPnlValue = 0;
        p.lastPnlPercent = 0;
        p.lastPnlAt = Date.now();
        p.entryCostMode = 'tracked';
      }
    }

    p.lastUpdated = Date.now();
  }

  function hasPendingBuy(base, quote, accountId = null) {
    const safeAccountId = accountId === null ? null : String(accountId || 'primary');
    return Object.values(state.pending).some(p =>
      p &&
      p.side === 'buy' &&
      canonicalBaseEquals(p.base, base) &&
      p.quote === quote &&
      (safeAccountId === null || String(p.accountId || 'primary') === safeAccountId)
    );
  }

  function hasPendingSell(base, quote, accountId = null) {
    const safeAccountId = accountId === null ? null : String(accountId || 'primary');
    return Object.values(state.pending).some(p =>
      p &&
      p.side === 'sell' &&
      canonicalBaseEquals(p.base, base) &&
      p.quote === quote &&
      (safeAccountId === null || String(p.accountId || 'primary') === safeAccountId)
    );
  }

  function retryBuyKey(base, quote, accountId = 'primary') {
    return posKey(base, quote, accountId);
  }

  function getRetryBuy(base, quote, accountId = 'primary') {
    return state.retryBuys?.[retryBuyKey(base, quote, accountId)] || null;
  }

  function upsertRetryBuy(signal, patch = {}) {
    const base = normalizeLbankBaseToken(patch.base || signal?.base || '');
    const quote = normalizeQuoteCode(patch.quote || signal?.quote || '');
    const accountId = String(patch.accountId || signal?.accountId || 'primary');
    if (!base || !isUsdLikeQuoteCode(quote)) return null;

    if (!state.retryBuys || typeof state.retryBuys !== 'object') {
      state.retryBuys = {};
    }

    const key = retryBuyKey(base, quote, accountId);
    const curr = state.retryBuys[key] || {
      base,
      quote,
      accountId,
      rawLine: '',
      signalKey: '',
      score: 0,
      createdAt: Date.now(),
      updatedAt: 0,
      lastAttemptAt: 0,
      lastOrderId: '',
      desiredAllocation: 0,
      requestedAmount: 0,
      filledAmount: 0,
      filledQuoteEstimate: 0,
      estimatedPrice: 0,
      attempts: 0,
      requiresUsdtOnlyFallback: false,
      status: 'queued',
      lastError: '',
      lastResolvedStatus: '',
      lastResolvedAt: 0,
      autoSweepRemainingBalance: false,
      autoSweepSequenceId: '',
      autoSweepIteration: 0,
      autoSweepComplete: false,
      autoSweepStopReason: '',
      allowWhenDryRunOff: false,
      manualSessionId: '',
      manualSessionStartedAt: 0,
      manualSessionExpiresAt: 0,
      manualSessionCancelledAt: 0,
      manualSessionStopReason: '',
      insufficientBalanceAt: 0,
      insufficientBalanceAvailable: 0,
      insufficientBalanceNeeded: 0,
      retryAfterSellEligibleUntil: 0,
    };

    const nextDesired = patch.desiredAllocation !== undefined
      ? Math.max(num(curr.desiredAllocation), num(patch.desiredAllocation))
      : num(curr.desiredAllocation);

    Object.assign(curr, {
      base,
      quote,
      accountId,
      rawLine: patch.rawLine ?? signal?.rawLine ?? curr.rawLine,
      signalKey: patch.signalKey ?? signal?.signalKey ?? curr.signalKey,
      score: Math.max(num(curr.score), num(patch.score ?? signal?.score ?? curr.score)),
      desiredAllocation: nextDesired,
      requestedAmount: patch.requestedAmount !== undefined ? Math.max(0, num(patch.requestedAmount)) : Math.max(0, num(curr.requestedAmount)),
      filledAmount: patch.filledAmount !== undefined ? Math.max(0, num(patch.filledAmount)) : Math.max(0, num(curr.filledAmount)),
      filledQuoteEstimate: patch.filledQuoteEstimate !== undefined ? Math.max(0, num(patch.filledQuoteEstimate)) : Math.max(0, num(curr.filledQuoteEstimate)),
      estimatedPrice: patch.estimatedPrice !== undefined ? Math.max(0, num(patch.estimatedPrice)) : Math.max(0, num(curr.estimatedPrice)),
      lastAttemptAt: patch.lastAttemptAt !== undefined ? num(patch.lastAttemptAt) : num(curr.lastAttemptAt),
      lastOrderId: patch.lastOrderId !== undefined ? String(patch.lastOrderId || '') : String(curr.lastOrderId || ''),
      attempts: patch.attempts !== undefined ? Math.max(0, num(patch.attempts)) : Math.max(0, num(curr.attempts)),
      requiresUsdtOnlyFallback: patch.requiresUsdtOnlyFallback !== undefined
        ? !!patch.requiresUsdtOnlyFallback
        : !!(signal?.requiresUsdtOnlyFallback ?? curr.requiresUsdtOnlyFallback),
      status: patch.status !== undefined ? String(patch.status || curr.status || 'queued') : String(curr.status || 'queued'),
      lastError: patch.lastError !== undefined ? String(patch.lastError || '') : String(curr.lastError || ''),
      lastResolvedStatus: patch.lastResolvedStatus !== undefined ? String(patch.lastResolvedStatus || '') : String(curr.lastResolvedStatus || ''),
      lastResolvedAt: patch.lastResolvedAt !== undefined ? num(patch.lastResolvedAt) : num(curr.lastResolvedAt),
      autoSweepRemainingBalance: patch.autoSweepRemainingBalance !== undefined
        ? !!patch.autoSweepRemainingBalance
        : !!curr.autoSweepRemainingBalance,
      autoSweepSequenceId: patch.autoSweepSequenceId !== undefined ? String(patch.autoSweepSequenceId || '') : String(curr.autoSweepSequenceId || ''),
      autoSweepIteration: patch.autoSweepIteration !== undefined ? Math.max(0, num(patch.autoSweepIteration)) : Math.max(0, num(curr.autoSweepIteration)),
      autoSweepComplete: patch.autoSweepComplete !== undefined ? !!patch.autoSweepComplete : !!curr.autoSweepComplete,
      autoSweepStopReason: patch.autoSweepStopReason !== undefined ? String(patch.autoSweepStopReason || '') : String(curr.autoSweepStopReason || ''),
      allowWhenDryRunOff: patch.allowWhenDryRunOff !== undefined ? !!patch.allowWhenDryRunOff : !!curr.allowWhenDryRunOff,
      manualSessionId: patch.manualSessionId !== undefined ? String(patch.manualSessionId || '') : String(curr.manualSessionId || ''),
      manualSessionStartedAt: patch.manualSessionStartedAt !== undefined ? Math.max(0, num(patch.manualSessionStartedAt)) : Math.max(0, num(curr.manualSessionStartedAt)),
      manualSessionExpiresAt: patch.manualSessionExpiresAt !== undefined ? Math.max(0, num(patch.manualSessionExpiresAt)) : Math.max(0, num(curr.manualSessionExpiresAt)),
      manualSessionCancelledAt: patch.manualSessionCancelledAt !== undefined ? Math.max(0, num(patch.manualSessionCancelledAt)) : Math.max(0, num(curr.manualSessionCancelledAt)),
      manualSessionStopReason: patch.manualSessionStopReason !== undefined ? String(patch.manualSessionStopReason || '') : String(curr.manualSessionStopReason || ''),
      insufficientBalanceAt: patch.insufficientBalanceAt !== undefined ? Math.max(0, num(patch.insufficientBalanceAt)) : Math.max(0, num(curr.insufficientBalanceAt)),
      insufficientBalanceAvailable: patch.insufficientBalanceAvailable !== undefined ? Math.max(0, num(patch.insufficientBalanceAvailable)) : Math.max(0, num(curr.insufficientBalanceAvailable)),
      insufficientBalanceNeeded: patch.insufficientBalanceNeeded !== undefined ? Math.max(0, num(patch.insufficientBalanceNeeded)) : Math.max(0, num(curr.insufficientBalanceNeeded)),
      retryAfterSellEligibleUntil: patch.retryAfterSellEligibleUntil !== undefined ? Math.max(0, num(patch.retryAfterSellEligibleUntil)) : Math.max(0, num(curr.retryAfterSellEligibleUntil)),
      updatedAt: Date.now(),
    });

    state.retryBuys[key] = curr;
    return curr;
  }

  function clearRetryBuy(base, quote, accountId = 'primary') {
    if (!state.retryBuys) return;
    delete state.retryBuys[retryBuyKey(base, quote, accountId)];
  }

  function seedRetryBuysFromCurrentPriorityList() {
    for (const signal of state.currentPriorityList || []) {
      if (!signal?.base || !signal?.quote) continue;
      if (hasPendingBuy(signal.base, signal.quote)) continue;
      const existing = Object.values(state.retryBuys || {}).find(entry =>
        entry && entry.base === signal.base && entry.quote === signal.quote
      ) || null;
      const existingNeedsAction = existing ? retryEntryNeedsAction(existing) : false;
      const hasAnyOpenPosition = findOpenPositionsForMarket(signal.base, signal.quote).length > 0;
      if (hasAnyOpenPosition && !existingNeedsAction) continue;
      upsertRetryBuy(existing || signal, {
        accountId: String(existing?.accountId || signal?.accountId || 'primary'),
        status: existing?.status || 'queued',
        desiredAllocation: existing?.desiredAllocation || 0,
        estimatedPrice: existing?.estimatedPrice || 0,
      });
    }
  }

  function retryEntryRemainingAllocation(entry) {
    const desired = Math.max(0, num(entry?.desiredAllocation));
    const filledQuoteEstimate = Math.max(0, num(entry?.filledQuoteEstimate));
    if (desired > 0) {
      return Math.max(0, desired - filledQuoteEstimate);
    }
    return 0;
  }

  function retryEntryNeedsAction(entry) {
    if (!entry?.base || !entry?.quote) return false;
    if (hasPendingBuy(entry.base, entry.quote, entry.accountId || 'primary')) return false;
    if (entry.autoSweepRemainingBalance && entry.autoSweepComplete) return false;

    const manualGuard = evaluateManualBuySessionGuard(entry);
    if (manualGuard.enabled && !manualGuard.ok) return false;

    const requested = Math.max(0, num(entry.requestedAmount));
    const filledAmount = Math.max(0, num(entry.filledAmount));
    const remainingAllocation = retryEntryRemainingAllocation(entry);

    if (remainingAllocation > 0) return true;
    if (requested > 0 && filledAmount < (requested * BUY_RETRY_COMPLETE_TOLERANCE)) return true;

    return ['queued', 'failed', 'needs_retry', 'partial', 'validation_retry'].includes(String(entry.status || ''));
  }

  function countRetryBuysNeeded() {
    seedRetryBuysFromCurrentPriorityList();
    return Object.values(state.retryBuys || {}).filter(retryEntryNeedsAction).length;
  }

  function buildRetryPlansForQuote(entries, budget, minPerTrade) {
    const sorted = [...entries].sort((a, b) => num(b.score) - num(a.score));
    const plans = [];
    let remainingBudget = Math.max(0, num(budget));
    const flexSignals = [];

    for (const entry of sorted) {
      const manualGuard = evaluateManualBuySessionGuard(entry);
      if (manualGuard.enabled && !manualGuard.ok) {
        markManualBuySessionStopped(
          entry.base,
          entry.quote,
          manualGuard.meta.manualSessionId,
          manualGuard.reason,
          {
            markCancelled: manualGuard.meta.manualSessionCancelledAt > 0,
            status: manualGuard.meta.manualSessionCancelledAt > 0
              ? 'cancelled'
              : (num(entry.filledAmount) > 0 ? 'filled' : 'expired'),
          }
        );
        continue;
      }

      const retryOptions = {
        accountId: String(entry.accountId || 'primary'),
        allowWhenDryRunOff: !!entry.allowWhenDryRunOff,
        autoSweepRemainingBalance: !!entry.autoSweepRemainingBalance,
        autoSweepSequenceId: String(entry.autoSweepSequenceId || ''),
        autoSweepIteration: Math.max(0, num(entry.autoSweepIteration)),
        manualSessionId: String(entry.manualSessionId || ''),
        manualSessionStartedAt: Math.max(0, num(entry.manualSessionStartedAt)),
        manualSessionExpiresAt: Math.max(0, num(entry.manualSessionExpiresAt)),
        manualSessionCancelledAt: Math.max(0, num(entry.manualSessionCancelledAt)),
        manualSessionStopReason: String(entry.manualSessionStopReason || ''),
      };

      const remainingAllocation = retryEntryRemainingAllocation(entry);
      if (remainingAllocation > 0) {
        if (remainingAllocation >= minPerTrade && remainingBudget >= minPerTrade) {
          const allocation = Math.min(remainingAllocation, remainingBudget);
          if (allocation >= minPerTrade) {
            plans.push({
              signal: {
                base: entry.base,
                quote: entry.quote,
                rawLine: entry.rawLine,
                signalKey: entry.signalKey,
                score: entry.score || 0,
                requiresUsdtOnlyFallback: !!entry.requiresUsdtOnlyFallback,
                accountId: String(entry.accountId || 'primary'),
              },
              allocation,
              priority: entry.score || 0,
              options: retryOptions,
            });
            remainingBudget -= allocation;
          }
        }
        continue;
      }

      flexSignals.push({
        base: entry.base,
        quote: entry.quote,
        rawLine: entry.rawLine,
        signalKey: entry.signalKey,
        score: entry.score || 0,
        requiresUsdtOnlyFallback: !!entry.requiresUsdtOnlyFallback,
        accountId: String(entry.accountId || 'primary'),
        retryOptions,
      });
    }

    if (flexSignals.length && remainingBudget >= minPerTrade) {
      plans.push(...descendingAllocation(
        flexSignals.sort((a, b) => (b.score || 0) - (a.score || 0)),
        remainingBudget,
        minPerTrade
      ).map(plan => ({
        ...plan,
        options: plan.signal.retryOptions || {},
        signal: {
          base: plan.signal.base,
          quote: plan.signal.quote,
          rawLine: plan.signal.rawLine,
          signalKey: plan.signal.signalKey,
          score: plan.signal.score || 0,
          requiresUsdtOnlyFallback: !!plan.signal.requiresUsdtOnlyFallback,
          accountId: String(plan.signal.accountId || 'primary'),
        },
      })));
    }

    return plans;
  }

  async function resolveExecutableBuySignal(signal) {
    const base = normalizeLbankBaseToken(signal?.base || '');
    const quote = normalizeQuoteCode(signal?.quote || 'usdt');

    if (!base || !quote || !isUsdLikeQuoteCode(quote)) {
      return { ignored: true, reason: 'سیگنال خرید ناقص یا خارج از جفت‌های دلاری بود.' };
    }

    return {
      signal: {
        ...signal,
        base,
        quote,
        requiresUsdtOnlyFallback: false,
      },
    };
  }

  async function runBuyExecutionPlans(plans, label = 'خرید') {
    for (let i = 0; i < plans.length; i++) {
      const plan = plans[i];
      await executeBuy(plan.signal, plan.allocation, {
        reasonLabel: label,
        ...(plan.options || {}),
      });

      if (i < plans.length - 1) {
        await waitBetweenTradeActions(`بین خرید ${plan.signal.base} و خرید بعدی`, BUY_TRY_DELAY_MS);
      }
    }
  }

  async function retryOutstandingBuys() {
    if (!hasAnyConfiguredAccountToken()) {
      log('برای تلاش دوباره خریدها باید حداقل یک توکن فعال ثبت شده باشد.', 'warn');
      return;
    }

    seedRetryBuysFromCurrentPriorityList();
    const entries = Object.values(state.retryBuys || {}).filter(retryEntryNeedsAction);

    if (!entries.length) {
      log('خرید ناموفق یا ناقصی برای تلاش دوباره پیدا نشد.', 'info');
      return;
    }

    const executableEntries = [];
    for (const entry of entries) {
      const resolved = await resolveExecutableBuySignal(entry);
      if (resolved?.signal) {
        executableEntries.push({
          ...entry,
          base: resolved.signal.base,
          quote: resolved.signal.quote,
          requiresUsdtOnlyFallback: false,
          accountId: String(entry.accountId || 'primary'),
        });
        continue;
      }

      const reason = String(resolved?.reason || 'این خرید retry قابل‌اجرا نبود.');
      upsertRetryBuy(entry, {
        status: 'ignored',
        lastError: reason,
        requiresUsdtOnlyFallback: false,
      });
      log(`retry ${entry.base}: ${reason}`, 'warn');
    }

    if (!executableEntries.length) {
      saveState();
      return;
    }

    const plans = [];
    const groups = {};
    for (const entry of executableEntries) {
      const accountId = String(entry.accountId || 'primary');
      if (!groups[accountId]) groups[accountId] = [];
      groups[accountId].push(entry);
    }

    for (const [accountId, groupedEntries] of Object.entries(groups)) {
      const account = getAccountContext(accountId, { includeInactive: true });
      if (!account || !String(account.token || '').trim()) {
        log(`${accountLabel(accountId)} برای retry در دسترس نیست.`, 'warn');
        continue;
      }

      let walletInfo;
      try {
        walletInfo = await loadWalletMapForAccount(accountId, { updatePrimarySummary: accountId === 'primary' });
      } catch (err) {
        logError(err, `getWallets ${accountLabel(accountId)}`);
        continue;
      }

      const wm = walletInfo.map;
      const quotes = [...new Set(groupedEntries.map(e => normalizeQuoteCode(e.quote)).filter(isUsdLikeQuoteCode))];
      for (const quote of quotes) {
        plans.push(
          ...buildRetryPlansForQuote(groupedEntries.filter(e => normalizeQuoteCode(e.quote) === quote), walletAvailable(wm, quote.toUpperCase()), minOrderValueForQuote(quote)).map(plan => ({
            ...plan,
            options: { ...(plan.options || {}), accountId },
          }))
        );
      }
    }

    if (!plans.length) {
      log('ورودی retry پیدا شد ولی موجودی فعلی برای حداقل سفارش کافی نیست.', 'warn');
      return;
    }

    log(`تلاش دوباره برای خریدهای ناقص/ناموفق: ${plans.map(p => `${p.signal.base}(${quoteLabelFa(p.signal.quote)})-${accountLabel((p.options || {}).accountId || 'primary')}`).join('، ')}`, 'warn');
    await runBuyExecutionPlans(plans, 'retry');
    saveState();
  }

  function isInsufficientBalanceMessage(message) {
    const text = String(message || '').toLowerCase();
    return /insufficient|not enough|balance|fund|wallet|موجودی|کمبود|کافی|اعتبار/.test(text);
  }

  function markBuySkippedForInsufficientBalance(signal, accountId, availableQuote = 0, neededQuote = 0, reason = '') {
    const normalizedSignal = {
      ...(signal || {}),
      base: normalizeLbankBaseToken(signal?.base || ''),
      quote: normalizeQuoteCode(signal?.quote || ''),
      accountId: String(accountId || signal?.accountId || 'primary'),
    };

    if (!normalizedSignal.base || !isUsdLikeQuoteCode(normalizedSignal.quote)) return null;

    const now = Date.now();
    const minValue = minOrderValueForQuote(normalizedSignal.quote);
    const needed = Math.max(minValue, num(neededQuote));
    const available = Math.max(0, num(availableQuote));
    const shortageText = formatQuoteValue(Math.max(0, needed - available), normalizedSignal.quote);
    const finalReason = reason || `insufficient_balance: موجودی ${quoteLabelFa(normalizedSignal.quote)} برای حداقل سفارش کافی نبود. کمبود: ${shortageText}`;

    return upsertRetryBuy(normalizedSignal, {
      accountId: normalizedSignal.accountId,
      desiredAllocation: needed,
      status: 'needs_retry',
      lastError: finalReason,
      requiresUsdtOnlyFallback: false,
      insufficientBalanceAt: now,
      insufficientBalanceAvailable: available,
      insufficientBalanceNeeded: needed,
      retryAfterSellEligibleUntil: now + POST_SELL_RECENT_INSUFFICIENT_BUY_WINDOW_MS,
    });
  }

  function markUnplannedBuySignalsForInsufficientBalance(executableSignals = [], plans = [], walletMapsByAccount = {}, reasonLabel = '') {
    const planned = plannedSignalKeySet(plans);
    let marked = 0;

    for (const signal of executableSignals || []) {
      if (planned.has(buySignalPlanKey(signal))) continue;

      const quote = normalizeQuoteCode(signal?.quote || '');
      if (!isUsdLikeQuoteCode(quote)) continue;

      const minValue = minOrderValueForQuote(quote);
      const accounts = getAccountsEligibleForBuySignal(signal);
      for (const account of accounts) {
        const accountId = String(account?.id || 'primary');
        const wm = walletMapsByAccount[accountId] || {};
        const available = walletAvailable(wm, quote);
        if (available >= minValue) continue;

        const entry = markBuySkippedForInsufficientBalance(
          signal,
          accountId,
          available,
          minValue,
          reasonLabel || `insufficient_balance: موجودی ${quoteLabelFa(quote)} برای خرید ${signal.base} در ${accountLabel(accountId)} کافی نبود.`
        );
        if (entry) marked += 1;
      }
    }

    if (marked > 0) {
      log(`${formatFaNumber(marked)} خرید به علت کمبود موجودی برای retry بعد از فروش ذخیره شد.`, 'info');
    }

    return marked;
  }

  function isRecentInsufficientBalanceRetryEntry(entry, accountId = null, quote = null, now = Date.now()) {
    if (!entry) return false;

    const entryAccountId = String(entry.accountId || 'primary');
    if (accountId !== null && entryAccountId !== String(accountId || 'primary')) return false;

    const entryQuote = normalizeQuoteCode(entry.quote || '');
    if (quote !== null && entryQuote !== normalizeQuoteCode(quote)) return false;
    if (!isUsdLikeQuoteCode(entryQuote)) return false;

    const marker = Math.max(
      num(entry.insufficientBalanceAt),
      num(entry.lastAttemptAt),
      num(entry.updatedAt),
      num(entry.createdAt)
    );
    const eligibleUntil = Math.max(num(entry.retryAfterSellEligibleUntil), marker + POST_SELL_RECENT_INSUFFICIENT_BUY_WINDOW_MS);
    if (!(marker > 0) || marker < now - POST_SELL_RECENT_INSUFFICIENT_BUY_WINDOW_MS || eligibleUntil < now) return false;

    const status = String(entry.status || '');
    const lastError = String(entry.lastError || '');
    const statusLooksRetryable = ['queued', 'failed', 'needs_retry', 'partial', 'validation_retry'].includes(status);
    const errorLooksInsufficient = /insufficient_balance/i.test(lastError) || isInsufficientBalanceMessage(lastError) || num(entry.insufficientBalanceAt) > 0;

    return statusLooksRetryable && errorLooksInsufficient && retryEntryNeedsAction(entry);
  }

  function recentInsufficientBalanceEntriesForPostSell(accountId, quote, now = Date.now()) {
    return Object.values(state.retryBuys || {})
      .filter(entry => isRecentInsufficientBalanceRetryEntry(entry, accountId, quote, now))
      .sort((a, b) => {
        const scoreDiff = num(b.score) - num(a.score);
        if (Math.abs(scoreDiff) > 0.000001) return scoreDiff;
        return Math.max(num(a.insufficientBalanceAt), num(a.updatedAt), num(a.createdAt)) - Math.max(num(b.insufficientBalanceAt), num(b.updatedAt), num(b.createdAt));
      });
  }


async function convertResidualAssetAfterCompletedSell(pending, order = null) {
  return { status: 'disabled', reason: 'lbank_no_safe_universal_convert_endpoint' };
}

  async function tryRecentInsufficientBuysAfterCompletedSell(pending) {
    const accountId = String(pending?.accountId || 'primary');
    const quote = normalizeQuoteCode(pending?.quote || '');
    if (!isUsdLikeQuoteCode(quote)) return { status: 'skipped', reason: 'invalid_quote' };

    const canPlace = shouldPlaceRealOrder({ allowWhenDryRunOff: !!pending?.allowWhenDryRunOff });
    if (!canPlace) return { status: 'skipped', reason: 'real_order_disabled' };

    const entries = recentInsufficientBalanceEntriesForPostSell(accountId, quote);
    if (!entries.length) return { status: 'skipped', reason: 'no_recent_entries' };

    const walletInfo = await loadWalletMapForAccount(accountId, { updatePrimarySummary: accountId === 'primary' });
    const availableQuote = walletAvailable(walletInfo.map, quote);
    const minValue = minOrderValueForQuote(quote);
    if (availableQuote < minValue) {
      log(`بعد از فروش، خرید جاافتاده بررسی شد ولی موجودی ${quoteLabelFa(quote)} هنوز به حداقل سفارش نمی‌رسد: ${formatQuoteValue(availableQuote, quote)}`, 'info');
      return { status: 'skipped', reason: 'still_insufficient_balance', availableQuote };
    }

    const plans = buildRetryPlansForQuote(entries, availableQuote, minValue).map(plan => ({
      ...plan,
      options: {
        ...(plan.options || {}),
        accountId,
        reasonLabel: 'post_sell_retry',
        allowWhenDryRunOff: !!pending?.allowWhenDryRunOff,
      },
    }));

    if (!plans.length) {
      log('بعد از فروش، خریدهای جاافتاده پیدا شد ولی برنامه قابل اجرا با موجودی فعلی ساخته نشد.', 'warn');
      return { status: 'skipped', reason: 'no_plans' };
    }

    log(
      `بعد از فروش، retry خریدهای جاافتاده به علت کمبود موجودی شروع شد: ${plans.map(p => `${p.signal.base}(${quoteLabelFa(p.signal.quote)})`).join('، ')}`,
      'warn'
    );
    await runBuyExecutionPlans(plans, 'post_sell_retry');
    saveState();

    return { status: 'attempted', count: plans.length };
  }

  async function handleCompletedSellAftercare(pending, order = null) {
    if (!pending || pending.side !== 'sell') return;

    try {
      await convertResidualAssetAfterCompletedSell(pending, order);
    } catch (err) {
      logError(err, `convertResidualAssetAfterCompletedSell ${pending.base}/${pending.quote}`);
    }

    try {
      await tryRecentInsufficientBuysAfterCompletedSell(pending);
    } catch (err) {
      logError(err, `tryRecentInsufficientBuysAfterCompletedSell ${pending.base}/${pending.quote}`);
    }
  }

  /******************************************************************
   * تخصیص سرمایه
   ******************************************************************/
  function dedupeSignalsKeepBest(signals) {
    const byMarket = new Map();
    for (const s of signals || []) {
      const k = `${s.base}|${s.quote || ''}`;
      if (!byMarket.has(k)) {
        byMarket.set(k, s);
      }
    }
    return [...byMarket.values()];
  }

  function descendingAllocation(sortedSignals, totalBudget, minPerTrade) {
    const safeBudget = totalBudget * 0.995;
    const maxCount = Math.min(sortedSignals.length, Math.floor(safeBudget / minPerTrade));

    if (maxCount <= 0) return [];

    const chosen = sortedSignals.slice(0, maxCount);
    const weights = chosen.map((_, i) => maxCount - i);
    const allocations = Array(maxCount).fill(minPerTrade);

    let extra = safeBudget - (maxCount * minPerTrade);
    if (extra > 0) {
      const sumWeights = weights.reduce((a, b) => a + b, 0);
      for (let i = 0; i < allocations.length; i++) {
        allocations[i] += (extra * weights[i]) / sumWeights;
      }
    }

    return chosen.map((signal, idx) => ({
      signal,
      allocation: allocations[idx],
      priority: signal.score || 0,
    }));
  }

  async function loadWalletMapForAccount(accountId = 'primary', options = {}) {
    const wallets = await getWallets({ accountId, includeInactive: true });
    if (accountId === 'primary' && options.updatePrimarySummary) {
      setBalanceSummary(wallets);
    }
    return {
      wallets,
      map: walletMap(wallets),
    };
  }

  function buySignalPlanKey(signal) {
    return String(signal?.signalKey || `${signal?.base || ''}|${signal?.quote || ''}|${signal?.rawLine || ''}`);
  }

  function isPumpBuySignal(signal) {
    return String(signal?.signalFlavor || '') === 'pump_buy';
  }

  function plannedSignalKeySet(plans = []) {
    const set = new Set();
    for (const plan of plans || []) {
      set.add(buySignalPlanKey(plan?.signal || {}));
    }
    return set;
  }

  function listUnplannedPumpBuySignals(executableSignals = [], plans = []) {
    const planned = plannedSignalKeySet(plans);
    return (executableSignals || []).filter(signal => isPumpBuySignal(signal) && !planned.has(buySignalPlanKey(signal)));
  }

  function getAccountsEligibleForBuySignal(signal) {
    const accounts = [];
    const primary = getAccountContext('primary', { includeInactive: true });
    if (supportsSignalOnAccount(primary, signal, { forBackup: false })) {
      accounts.push(primary);
    }

    for (const account of getActiveSecondaryAccounts()) {
      if (supportsSignalOnAccount(account, signal, { forBackup: true })) {
        accounts.push(account);
      }
    }

    return accounts;
  }

  function listLowGainPositionsForPumpFunding(signal, accountId, walletsMap = {}) {
    const normalizedAccountId = String(accountId || 'primary');
    const quote = normalizeQuoteCode(signal?.quote || '');
    const targetBase = normalizeLbankBaseToken(signal?.base || '');

    return listOpenPositions()
      .filter(position => {
        if (String(position?.accountId || 'primary') !== normalizedAccountId) return false;
        if (normalizeQuoteCode(position?.quote || '') !== quote) return false;
        if (canonicalBaseEquals(position?.base, targetBase)) return false;
        if (!(num(position?.qty) > 0)) return false;
        if (!(num(position?.lastPnlAt) > 0)) return false;
        return num(position?.lastPnlPercent) < PUMP_BUY_LOW_GAIN_SELL_THRESHOLD_PERCENT;
      })
      .map(position => {
        const base = normalizeLbankBaseToken(position.base);
        const walletQty = Math.max(0, walletAvailable(walletsMap, base));
        const qty = amountFloor(Math.min(num(position.qty), walletQty));
        const estPrice = Math.max(0, num(position.lastKnownPrice));
        const estNotional = qty * estPrice;
        return {
          position,
          base,
          quote,
          qty,
          walletQty,
          estNotional,
          pnlPercent: num(position.lastPnlPercent),
        };
      })
      .filter(item => item.qty > 0)
      .sort((a, b) => {
        const pnlDiff = a.pnlPercent - b.pnlPercent;
        if (Math.abs(pnlDiff) > 0.000001) return pnlDiff;
        return b.estNotional - a.estNotional;
      });
  }

  async function tryFundPumpBuysBySellingLowGainPositions(executableSignals = [], currentPlans = [], walletMapsByAccount = {}) {
    if (!config.sellLowGainHoldingsForPumpBuys) {
      return { sold: false, affectedAccounts: [] };
    }

    const pumpSignals = listUnplannedPumpBuySignals(executableSignals, currentPlans);
    if (!pumpSignals.length) {
      return { sold: false, affectedAccounts: [] };
    }

    await refreshOpenPositionsPnl(true);

    let soldAny = false;
    const affectedAccounts = new Set();
    const soldPositionKeys = new Set();

    for (const signal of pumpSignals) {
      const quote = normalizeQuoteCode(signal.quote);
      const minValue = minOrderValueForQuote(quote);
      const accounts = getAccountsEligibleForBuySignal(signal);

      for (const account of accounts) {
        const accountId = String(account.id || 'primary');
        const wm = walletMapsByAccount[accountId] || {};
        const availableQuote = walletAvailable(wm, quote);

        if (availableQuote >= minValue) {
          continue;
        }

        const candidates = listLowGainPositionsForPumpFunding(signal, accountId, wm)
          .filter(item => !soldPositionKeys.has(posKey(item.base, item.quote, accountId)));

        if (!candidates.length) {
          continue;
        }

        const accountText = accountLabel(accountId);
        const quoteText = quoteLabelFa(quote);
        const shortageText = formatQuoteValue(Math.max(0, minValue - availableQuote), quote);
        log(`برای خرید هنگام پامپ ${signal.base} (${quoteText}) در ${accountText} موجودی کافی نیست؛ تلاش برای تأمین کمبود با فروش پوزیشن‌های زیر ${formatSignedPercent(PUMP_BUY_LOW_GAIN_SELL_THRESHOLD_PERCENT)} سود. کمبود حداقلی: ${shortageText}`, 'warn');

        let expectedRaised = 0;
        let accountSold = false;
        for (let i = 0; i < candidates.length; i++) {
          const item = candidates[i];
          soldPositionKeys.add(posKey(item.base, item.quote, accountId));

          const result = await executeSell(item.base, item.quote, item.qty, false, item.walletQty, {
            accountId,
            keepReserve: false,
            reasonLabel: 'pump_funding',
          });

          if (['placed', 'dry_run'].includes(String(result?.status || ''))) {
            soldAny = true;
            accountSold = true;
            affectedAccounts.add(accountId);
            expectedRaised += Math.max(0, num(result?.notional || item.estNotional));
          }

          if ((availableQuote + expectedRaised) >= minValue) {
            break;
          }

          if (i < candidates.length - 1) {
            await waitBetweenTradeActions(`بین فروش تأمین خرید پامپ ${item.base} و فروش بعدی`, BUY_TRY_DELAY_MS);
          }
        }

        if (accountSold) {
          break;
        }
      }
    }

    return { sold: soldAny, affectedAccounts: [...affectedAccounts] };
  }


async function buildAccountAwareBuyPlans(executableSignals, walletMapsByAccount = {}) {
  const plans = [];
  const signals = dedupeSignalsKeepBest(executableSignals)
    .filter(signal => isUsdLikeQuoteCode(normalizeQuoteCode(signal.quote)))
    .sort((a, b) => (b.priorityScore || 0) - (a.priorityScore || 0));

  const accounts = getActiveSecondaryAccounts();
  accounts.unshift(getAccountContext('primary'));

  for (const account of accounts) {
    if (!account?.active || !account.token || !account.secretKey) continue;
    const walletInfo = walletMapsByAccount[account.id] || {};
    const walletMapForAccount = walletInfo.map || walletInfo || {};
    const resolvedSignals = [];

    for (const signal of signals) {
      if (!supportsSignalOnAccount(account, signal)) continue;
      try {
        const resolved = await resolveBestUsdAllowedPairForWallet(signal.base, walletMapForAccount, {
          accountId: account.id,
          preferredQuote: signal.quote,
          preferExplicit: false,
          skipMarketStatsBudget: true,
        });
        if (!resolved.ok) {
          markBuySkippedForInsufficientBalance(signal, account.id, 0, MIN_USDT_ORDER, resolved.reason || 'no_allowed_usd_pair');
          continue;
        }
        resolvedSignals.push({
          ...signal,
          quote: resolved.quote,
          signalKey: signal.signalKey || `${signal.base}|${resolved.quote}|${hashString(signal.rawLine || '')}`,
          selectedAllowedPair: resolved.symbol,
        });
      } catch (err) {
        logError(err, `resolve allowed pair ${signal.base} ${accountLabel(account.id)}`);
      }
    }

    const groupedByQuote = new Map();
    for (const signal of resolvedSignals) {
      const quote = normalizeQuoteCode(signal.quote);
      if (!groupedByQuote.has(quote)) groupedByQuote.set(quote, []);
      groupedByQuote.get(quote).push(signal);
    }

    for (const [quote, quoteSignals] of groupedByQuote.entries()) {
      const budget = walletAvailable(walletMapForAccount, quote.toUpperCase());
      const minPerTrade = minOrderValueForQuote(quote);
      if (!(budget >= minPerTrade)) continue;
      const accountPlans = descendingAllocation(quoteSignals, budget, minPerTrade);
      accountPlans.forEach(plan => plans.push({ ...plan, accountId: account.id, quote }));
    }
  }

  return plans;
}

  async function handleBuySignals(signals) {
    if (!hasAnyConfiguredAccountToken()) {
      log('سیگنال خرید دیده شد ولی هیچ توکن فعالی ثبت نشده است.', 'warn');
      return;
    }

    const unique = dedupeSignalsKeepBest(signals).sort((a, b) => (b.score || 0) - (a.score || 0));
    const executableSignals = [];

    for (const signal of unique) {
      const resolved = await resolveExecutableBuySignal(signal);
      if (resolved?.signal) {
        executableSignals.push(resolved.signal);
        continue;
      }

      const reason = String(resolved?.reason || 'سیگنال خرید قابل‌اجرا نبود.');
      upsertRetryBuy(signal, {
        status: 'ignored',
        lastError: reason,
        requiresUsdtOnlyFallback: false,
      });
      log(`${signal.base}: ${reason}`, 'warn');
    }

    if (!executableSignals.length) {
      saveState();
      return;
    }

    const walletMapsByAccount = {};
    const primaryWallets = await loadWalletMapForAccount('primary', { updatePrimarySummary: true });
    walletMapsByAccount.primary = primaryWallets.map;

    for (const account of getActiveSecondaryAccounts()) {
      try {
        const info = await loadWalletMapForAccount(account.id);
        walletMapsByAccount[account.id] = info.map;
      } catch (err) {
        logError(err, `getWallets ${accountLabel(account.id)}`);
      }
    }

    let allPlans = await buildAccountAwareBuyPlans(executableSignals, walletMapsByAccount);

    const fundingResult = await tryFundPumpBuysBySellingLowGainPositions(executableSignals, allPlans, walletMapsByAccount);
    if (fundingResult.sold) {
      await waitBetweenTradeActions('بعد از فروش برای تأمین خرید پامپ', BUY_TRY_DELAY_MS);
      for (const accountId of fundingResult.affectedAccounts) {
        try {
          const info = await loadWalletMapForAccount(accountId, { updatePrimarySummary: accountId === 'primary' });
          walletMapsByAccount[accountId] = info.map;
        } catch (err) {
          logError(err, `refresh wallets after pump funding ${accountLabel(accountId)}`);
        }
      }
      allPlans = await buildAccountAwareBuyPlans(executableSignals, walletMapsByAccount);
    }

    markUnplannedBuySignalsForInsufficientBalance(
      executableSignals,
      allPlans,
      walletMapsByAccount,
      'insufficient_balance: سیگنال خرید به علت کمبود موجودی اجرا نشد و تا ۵ دقیقه بعد از فروش بعدی دوباره امتحان می‌شود.'
    );

    if (!allPlans.length) {
      for (const signal of executableSignals) {
        if (!isPumpBuySignal(signal)) continue;
        const now = Date.now();
        upsertRetryBuy(signal, {
          status: 'needs_retry',
          lastError: 'insufficient_balance: برای خرید هنگام پامپ موجودی کافی پیدا نشد و تأمین از پوزیشن‌های زیر ۱٪ هم خرید قابل اجرا نساخت.',
          requiresUsdtOnlyFallback: false,
          insufficientBalanceAt: now,
          retryAfterSellEligibleUntil: now + POST_SELL_RECENT_INSUFFICIENT_BUY_WINDOW_MS,
        });
      }
      log('برای هیچ حسابی موجودی کافی برای حداقل سفارش پیدا نشد.', 'warn');
      return;
    }

    await runBuyExecutionPlans(allPlans, 'signal');
    saveState();
  }

  async function executeBuy(signal, allocation, options = {}) {
    const resolvedSignal = await resolveExecutableBuySignal(signal);
    if (!resolvedSignal?.signal) {
      const reason = String(resolvedSignal?.reason || 'سیگنال خرید قابل‌اجرا نبود.');
      upsertRetryBuy(signal, {
        desiredAllocation: allocation,
        status: 'ignored',
        lastError: reason,
        requiresUsdtOnlyFallback: false,
        accountId: String(options.accountId || signal?.accountId || 'primary'),
      });
      log(reason, 'warn');
      return;
    }

    const executableSignal = resolvedSignal.signal;
    const { base, quote } = executableSignal;
    const accountId = String(options.accountId || executableSignal.accountId || 'primary');
    const accountText = accountLabel(accountId);
    const retryEntry = upsertRetryBuy(executableSignal, {
      accountId,
      desiredAllocation: allocation,
      requiresUsdtOnlyFallback: !!executableSignal.requiresUsdtOnlyFallback,
      autoSweepRemainingBalance: !!options.autoSweepRemainingBalance,
      autoSweepSequenceId: String(options.autoSweepSequenceId || ''),
      autoSweepIteration: Math.max(0, num(options.autoSweepIteration)),
      autoSweepComplete: false,
      autoSweepStopReason: '',
      allowWhenDryRunOff: !!options.allowWhenDryRunOff,
      manualSessionId: String(options.manualSessionId || ''),
      manualSessionStartedAt: Math.max(0, num(options.manualSessionStartedAt)),
      manualSessionExpiresAt: Math.max(0, num(options.manualSessionExpiresAt)),
      manualSessionCancelledAt: Math.max(0, num(options.manualSessionCancelledAt)),
      manualSessionStopReason: String(options.manualSessionStopReason || ''),
    });

    const manualGuard = evaluateManualBuySessionGuard(retryEntry || options);
    if (manualGuard.enabled && !manualGuard.ok) {
      markManualBuySessionStopped(
        base,
        quote,
        manualGuard.meta.manualSessionId,
        manualGuard.reason,
        {
          markCancelled: manualGuard.meta.manualSessionCancelledAt > 0,
          status: manualGuard.meta.manualSessionCancelledAt > 0
            ? 'cancelled'
            : (num(retryEntry?.filledAmount) > 0 ? 'filled' : 'expired'),
        }
      );
      log(`خرید دستی ${base} (${quoteLabelFa(quote)}) متوقف شد: ${manualGuard.reason}`, 'warn');
      saveState();
      return;
    }

    try {
      const stats = await getMarketStats(base, quote, { accountId });
      if (!stats) {
        const msg = `آمار بازار برای ${base}/${quote} پیدا نشد.`;
        retryEntry && upsertRetryBuy(executableSignal, {
          accountId,
          status: 'failed',
          lastError: msg,
          allowWhenDryRunOff: !!options.allowWhenDryRunOff,
        });
        log(msg, 'warn');
        return;
      }

      const price = num(stats.bestSell || stats.latest || stats.mark || stats.dayClose);
      if (!price || price <= 0) {
        const msg = `قیمت معتبر برای ${base}/${quote} پیدا نشد.`;
        retryEntry && upsertRetryBuy(executableSignal, {
          accountId,
          status: 'failed',
          lastError: msg,
          estimatedPrice: 0,
          allowWhenDryRunOff: !!options.allowWhenDryRunOff,
        });
        log(msg, 'warn');
        return;
      }

      const allowedInfo = normalizeAllowedUsdPairInfo(await getAllowedUsdPairInfo(base, { accountId, quote }), base, quote);
      if (!isAllowedUsdPairInfoExact(allowedInfo, base, quote)) {
        const hasSameBaseUsdPair = Array.isArray(allowedInfo.sameBase) && allowedInfo.sameBase.length > 0;
        if (!hasSameBaseUsdPair) {
          const msg = makeUnsupportedPairMessage(base, allowedInfo);
          retryEntry && upsertRetryBuy(executableSignal, {
            accountId,
            status: 'failed',
            lastError: msg,
            estimatedPrice: price,
            allowWhenDryRunOff: !!options.allowWhenDryRunOff,
          });
          log(msg, 'warn');
          return;
        }
        log(`هشدار: جفت ${displayLbankPairSymbol(lbankSymbol(base, quote))} در کش محلی با قطعیت تأیید نشد، اما چون جفت‌های دلاری همین نماد وجود دارد، تست سفارش LBank معیار نهایی خواهد بود.`, 'warn');
      }

      const pairRules = await getLbankPairRules(base, quote, { accountId });
      const amount = allocation / price;
      if (!amount || amount <= 0) {
        const msg = `مقدار خرید برای ${base}/${quote} صفر یا منفی شد.`;
        retryEntry && upsertRetryBuy(executableSignal, {
          accountId,
          status: 'failed',
          lastError: msg,
          estimatedPrice: price,
          allowWhenDryRunOff: !!options.allowWhenDryRunOff,
        });
        log(msg, 'warn');
        return;
      }

      const minValue = safeMinUsdtOrderValue(price, pairRules);
      const minQty = Math.max(0, num(pairRules?.minQty));
      if (minQty > 0 && amount < minQty) {
        const msg = `مقدار خرید ${base}/${quote} کمتر از حداقل مقدار LBank برای این جفت است. حداقل مقدار: ${formatAmount(minQty)} ${base}`;
        retryEntry && upsertRetryBuy(executableSignal, {
          accountId,
          status: 'failed',
          lastError: msg,
          estimatedPrice: price,
          allowWhenDryRunOff: !!options.allowWhenDryRunOff,
        });
        log(msg, 'warn');
        return;
      }

      if (allocation < minValue) {
        const msg = `بودجه تخصیصی برای ${base}/${quote} کمتر از حداقل امن LBank است. حداقل امن فعلی: ${formatQuoteValue(minValue, quote)}`;
        retryEntry && upsertRetryBuy(executableSignal, {
          accountId,
          status: 'failed',
          lastError: msg,
          estimatedPrice: price,
          allowWhenDryRunOff: !!options.allowWhenDryRunOff,
        });
        log(msg, 'warn');
        return;
      }

      const prettyAlloc = formatQuoteValue(allocation, quote);

      const quoteLabel = quoteLabelFa(quote);
      const actionLabel = options.reasonLabel === 'manual_buy'
        ? 'خرید دستی'
        : (options.reasonLabel === 'post_sell_retry' ? 'خرید بعد از فروش' : 'خرید');
      const planText =
        `${actionLabel} ${base} (${quoteLabel}) | حساب: ${accountText} | بودجه: ${prettyAlloc} | ` +
        `قیمت تقریبی: ${formatPrice(price, quote)} | مقدار: ${formatAmount(amount)} | ` +
        `اولویت: ${formatFaNumber(executableSignal.score || 0)}`;

      upsertRetryBuy(executableSignal, {
        accountId,
        desiredAllocation: allocation,
        estimatedPrice: price,
        status: 'attempting',
        lastError: '',
        attempts: num(retryEntry?.attempts) + 1,
        lastAttemptAt: Date.now(),
        autoSweepRemainingBalance: !!options.autoSweepRemainingBalance,
        autoSweepSequenceId: String(options.autoSweepSequenceId || ''),
        autoSweepIteration: Math.max(0, num(options.autoSweepIteration)),
        autoSweepComplete: false,
        autoSweepStopReason: '',
        allowWhenDryRunOff: !!options.allowWhenDryRunOff,
        manualSessionId: String(options.manualSessionId || ''),
        manualSessionStartedAt: Math.max(0, num(options.manualSessionStartedAt)),
        manualSessionExpiresAt: Math.max(0, num(options.manualSessionExpiresAt)),
        manualSessionCancelledAt: Math.max(0, num(options.manualSessionCancelledAt)),
        manualSessionStopReason: String(options.manualSessionStopReason || ''),
      });

      const placeRealOrder = shouldPlaceRealOrder(options);

      if (!placeRealOrder) {
        upsertRetryBuy(executableSignal, {
          accountId,
          status: 'dry_run',
          estimatedPrice: price,
          allowWhenDryRunOff: !!options.allowWhenDryRunOff,
        });
        log(`[شبیه‌سازی] ${planText}`, 'ok');
        return;
      }

      const { order, amount: placedAmount } = await placeMarketOrder('buy', base, quote, amount, price, { accountId });

      state.pending[String(order.id)] = {
        side: 'buy',
        base: normalizeLbankBaseToken(base),
        quote,
        symbol: lbankSymbol(base, quote),
        accountId,
        requestedAmount: num(order.amount || placedAmount || amount),
        appliedMatchedAmount: 0,
        createdAt: Date.now(),
        estimatedPrice: price,
        retryKey: retryBuyKey(base, quote, accountId),
        autoSweepRemainingBalance: !!options.autoSweepRemainingBalance,
        autoSweepSequenceId: String(options.autoSweepSequenceId || ''),
        autoSweepIteration: Math.max(0, num(options.autoSweepIteration)),
        manualSessionId: String(options.manualSessionId || ''),
        manualSessionStartedAt: Math.max(0, num(options.manualSessionStartedAt)),
        manualSessionExpiresAt: Math.max(0, num(options.manualSessionExpiresAt)),
        manualSessionCancelledAt: Math.max(0, num(options.manualSessionCancelledAt)),
        manualSessionStopReason: String(options.manualSessionStopReason || ''),
      };

      upsertRetryBuy(executableSignal, {
        accountId,
        desiredAllocation: allocation,
        estimatedPrice: price,
        requestedAmount: num(order.amount || placedAmount || amount),
        status: 'pending',
        lastOrderId: String(order.id),
        lastError: '',
        autoSweepRemainingBalance: !!options.autoSweepRemainingBalance,
        autoSweepSequenceId: String(options.autoSweepSequenceId || ''),
        autoSweepIteration: Math.max(0, num(options.autoSweepIteration)),
        allowWhenDryRunOff: !!options.allowWhenDryRunOff,
        manualSessionId: String(options.manualSessionId || ''),
        manualSessionStartedAt: Math.max(0, num(options.manualSessionStartedAt)),
        manualSessionExpiresAt: Math.max(0, num(options.manualSessionExpiresAt)),
        manualSessionCancelledAt: Math.max(0, num(options.manualSessionCancelledAt)),
        manualSessionStopReason: String(options.manualSessionStopReason || ''),
      });

      saveState();
      log(`سفارش خرید ثبت شد: ${planText} | شناسه سفارش: ${order.id}`, 'ok');
    } catch (err) {
      const message = String(err?.message || err || '');
      const insufficientBalance = isInsufficientBalanceMessage(message);
      const now = Date.now();
      upsertRetryBuy(executableSignal, {
        accountId,
        desiredAllocation: allocation,
        status: insufficientBalance ? 'needs_retry' : (shouldRetryOrderPlacement(message) ? 'validation_retry' : 'failed'),
        lastError: insufficientBalance ? `insufficient_balance: ${message}` : message,
        allowWhenDryRunOff: !!options.allowWhenDryRunOff,
        insufficientBalanceAt: insufficientBalance ? now : undefined,
        insufficientBalanceNeeded: insufficientBalance ? allocation : undefined,
        retryAfterSellEligibleUntil: insufficientBalance ? now + POST_SELL_RECENT_INSUFFICIENT_BUY_WINDOW_MS : undefined,
      });
      logError(err, `executeBuy ${base}/${quote} @ ${accountText}`);
    }
  }

  /******************************************************************
   * منطق فروش
   ******************************************************************/
  async function handleSellSignals(signals) {
    if (!hasAnyConfiguredAccountToken()) {
      log('سیگنال فروش دیده شد ولی هیچ توکن فعالی ثبت نشده است.', 'warn');
      return;
    }

    const sellTasks = [];

    for (const sig of signals) {
      let targets = [];

      if (sig.quote) {
        targets = findOpenPositionsForMarket(sig.base, sig.quote).map(p => ({ base: p.base, quote: p.quote, accountId: p.accountId || 'primary' }));
      } else {
        targets = Object.values(state.positions)
          .filter(p => p.base === sig.base && num(p.qty) > 0)
          .map(p => ({ base: p.base, quote: p.quote, accountId: p.accountId || 'primary' }));
      }

      if (!targets.length) {
        log(`برای فروش ${sig.base} پوزیشن محلی فعالی پیدا نشد.`, 'warn');
        continue;
      }

      for (const t of targets) {
        sellTasks.push({ sig, target: t });
      }
    }

    const availableBaseByAccount = {};
    async function ensureSellSignalAccountBalances(accountId) {
      const key = String(accountId || 'primary');
      if (availableBaseByAccount[key]) return availableBaseByAccount[key];
      const walletInfo = await loadWalletMapForAccount(key, { updatePrimarySummary: key === 'primary' });
      const bucket = {};
      for (const [currency, w] of Object.entries(walletInfo.map)) {
        bucket[currency.toUpperCase()] = walletAvailable(walletInfo.map, currency);
      }
      availableBaseByAccount[key] = bucket;
      return bucket;
    }

    for (let i = 0; i < sellTasks.length; i++) {
      const { sig, target: t } = sellTasks[i];
      const accountId = String(t.accountId || 'primary');
      const availableBase = await ensureSellSignalAccountBalances(accountId);
      const p = getPosition(t.base, t.quote, accountId);
      const trackedQty = num(p.qty);
      const baseWalletQty = availableBase[t.base] || 0;

      let desiredQty = Math.min(trackedQty, baseWalletQty);
      if ((!desiredQty || desiredQty <= 0) && baseWalletQty > 0 && sig.quote) {
        desiredQty = baseWalletQty;
        log(`پوزیشن محلی ${t.base}/${t.quote} در ${accountLabel(accountId)} صفر یا قدیمی بود؛ فروش با تکیه بر موجودی آزاد کیف پول انجام می‌شود و کمتر از ${formatFaNumber(SELL_KEEP_MAX_USDT_VALUE)} USDT از خود دارایی باقی می‌ماند.`, 'warn');
      }

      if (!desiredQty || desiredQty <= 0) {
        log(`موجودی آزاد ${t.base} برای فروش در بازار ${t.quote} در ${accountLabel(accountId)} کافی نیست.`, 'warn');
      } else {
        const result = await executeSell(t.base, t.quote, desiredQty, false, baseWalletQty, { accountId });
        if (result?.status === 'placed') {
          availableBase[t.base] = Math.max(0, baseWalletQty - num(result.sellQty));
        }
      }

      if (i < sellTasks.length - 1) {
        await waitBetweenTradeActions(`بین فروش ${t.base} و فروش بعدی`);
      }
    }

    saveState();
  }


async function executeSell(base, quote, qty, forced = false, walletQty = null, options = {}) {
  try {
    const accountId = String(options.accountId || 'primary');
    const baseUpper = normalizeLbankBaseToken(base);
    const desiredQty = Math.max(0, num(qty));
    const effectiveWalletQty = walletQty === null ? null : Math.max(0, num(walletQty));
    const keepReserve = options.keepReserve !== undefined ? !!options.keepReserve : (effectiveWalletQty !== null);

    const normalizedQuote = normalizeQuoteCode(quote || 'usdt');
    const stats = await getMarketStats(baseUpper, normalizedQuote, { accountId });
    if (!stats) {
      log(`آمار بازار برای فروش ${baseUpper}/${quoteLabelFa(normalizedQuote)} در ${accountLabel(accountId)} پیدا نشد.`, 'warn');
      return { status: 'error', reason: 'missing_stats' };
    }

    const price = num(stats.bestBuy || stats.latest || stats.mark || stats.dayClose);
    if (!price || price <= 0) {
      log(`قیمت معتبر برای فروش ${baseUpper}/${quoteLabelFa(normalizedQuote)} در ${accountLabel(accountId)} پیدا نشد.`, 'warn');
      return { status: 'error', reason: 'missing_price' };
    }

    const pairRules = await getLbankPairRules(baseUpper, normalizedQuote, { accountId });
    const minQty = Math.max(0, num(pairRules?.minQty));
    let reserveQty = 0;
    let sellQty = desiredQty;
    if (effectiveWalletQty !== null) {
      if (keepReserve) {
        reserveQty = reserveQtyForUsdtValue(effectiveWalletQty, price);
        sellQty = protectedSellQty(effectiveWalletQty, desiredQty, reserveQty);

        if (!sellQty || sellQty <= 0) {
          const keepText = formatAmount(Math.max(0, effectiveWalletQty));
          const reason = forced ? 'forced_keep_reserve' : 'keep_reserve';
          log(`برای فروش ${baseUpper}/${quoteLabelFa(normalizedQuote)} در ${accountLabel(accountId)} بعد از نگه‌داشتن کمتر از ${formatFaNumber(SELL_KEEP_MAX_USDT_VALUE)} USDT، مقدار قابل‌فروش باقی نماند. موجودی آزاد فعلی: ${keepText}`, forced ? 'warn' : 'info');
          return { status: 'skipped', reason, sellQty: 0, walletQty: effectiveWalletQty, reserveQty, reservePriceUsdt: price };
        }
      } else {
        sellQty = amountFloor(Math.min(desiredQty, effectiveWalletQty));
        if (!sellQty || sellQty <= 0) {
          log(`برای فروش ${baseUpper}/${quoteLabelFa(normalizedQuote)} در ${accountLabel(accountId)} مقدار قابل‌فروشی از موجودی آزاد پیدا نشد.`, forced ? 'warn' : 'info');
          return { status: 'skipped', reason: 'no_wallet_qty', sellQty: 0, walletQty: effectiveWalletQty };
        }
      }
    }

    sellQty = amountFloorByPairRules(sellQty, pairRules);
    if (minQty > 0 && sellQty < minQty) {
      log(`مقدار فروش ${baseUpper}/USDT کمتر از حداقل مقدار LBank برای این جفت است. حداقل مقدار: ${formatAmount(minQty)} ${baseUpper}`, forced ? 'warn' : 'info');
      return { status: 'skipped', reason: 'below_min_quantity', sellQty, price, minQty };
    }

    const notional = sellQty * price;
    const minValue = safeMinUsdtOrderValue(price, pairRules);
    if (notional < minValue) {
      log(`ارزش فروش ${baseUpper}/${quoteLabelFa(normalizedQuote)} در ${accountLabel(accountId)} بعد از نگه‌داشتن ریزمانده کمتر از حداقل امن LBank است. حداقل امن فعلی: ${formatQuoteValue(minValue, normalizedQuote)}`, forced ? 'warn' : 'info');
      return { status: 'skipped', reason: 'below_min_value', sellQty, price, notional };
    }

    const planText = `${baseUpper}/${quoteLabelFa(normalizedQuote)} | مقدار ${formatAmount(sellQty)} | حدود قیمت ${formatPrice(price, normalizedQuote)} | حساب: ${accountLabel(accountId)}`;
    const placeRealOrder = shouldPlaceRealOrder(options);

    if (!placeRealOrder) {
      log(`[شبیه‌سازی] فروش ${planText}`, 'ok');
      return { status: 'dry_run', sellQty, price, notional };
    }

    const { order, amount: placedAmount } = await placeMarketOrder('sell', baseUpper, normalizedQuote, sellQty, price, { accountId });

    state.pending[String(order.id)] = {
      side: 'sell',
      base: baseUpper,
      quote: normalizedQuote,
      symbol: lbankSymbol(baseUpper, normalizedQuote),
      accountId,
      requestedAmount: num(order.amount || placedAmount || sellQty),
      appliedMatchedAmount: 0,
      createdAt: Date.now(),
      estimatedPrice: price,
      forced: !!forced,
    };

    saveState();
    log(`سفارش فروش ثبت شد: ${planText} | شناسه سفارش: ${order.id}`, 'ok');
    return { status: 'placed', order, sellQty: num(order.amount || placedAmount || sellQty), price, notional };
  } catch (err) {
    logError(err, `executeSell ${base}/${quoteLabelFa(normalizeQuoteCode(quote || 'usdt'))}`);
    return { status: 'error', reason: String(err?.message || err) };
  }
}

  async function reconcilePendingOrders() {
    if (reconcileBusy) return;
    if (!hasAnyConfiguredAccountToken()) return;

    const ids = Object.keys(state.pending);
    if (!ids.length) return;

    reconcileBusy = true;

    try {
      for (const id of ids) {
        const pending = state.pending[id];
        if (!pending) continue;

        const accountId = String(pending.accountId || 'primary');
        let order;
        try {
          order = await getOrderStatus(id, { accountId, includeInactive: true });
        } catch (err) {
          logError(err, `getOrderStatus ${id}`);
          continue;
        }

        if (!order) continue;

        const matched = num(order.matchedAmount || 0);
        const prevApplied = num(pending.appliedMatchedAmount || 0);
        const delta = Math.max(0, matched - prevApplied);
        const effectivePrice = Math.max(0, num(order.averagePrice || order.avgPrice || order.matchedPrice || order.price || pending.estimatedPrice));

        if (delta > 0) {
          applyPositionDelta(pending.side, pending.base, pending.quote, delta, { price: effectivePrice, accountId });
          pending.appliedMatchedAmount = matched;

          if (pending.side === 'buy' && pending.retryKey) {
            const entry = state.retryBuys?.[pending.retryKey];
            if (entry) {
              entry.filledAmount = Math.max(num(entry.filledAmount), matched);
              entry.filledQuoteEstimate = Math.max(num(entry.filledQuoteEstimate), matched * Math.max(0, effectivePrice));
              entry.estimatedPrice = Math.max(num(entry.estimatedPrice), effectivePrice);
              entry.updatedAt = Date.now();
            }
          }

          log(
            `به‌روزرسانی پُرشدن سفارش: ${pending.side === 'buy' ? 'خرید' : 'فروش'} ${pending.base}/${pending.quote} | حساب: ${accountLabel(accountId)} | ` +
            `افزوده‌شده: ${formatAmount(delta)} | مجموع پرشده: ${formatAmount(matched)}`,
            'ok'
          );
        }

        const status = String(order.status || '');
        const stillOpen = ['Active', 'Inactive'].includes(status);

        if (!stillOpen) {
          const retryEntry = pending.side === 'buy' && pending.retryKey ? state.retryBuys?.[pending.retryKey] : null;

          let doneByAllocation = matched > 0;

          if (retryEntry) {
            retryEntry.lastResolvedStatus = status || 'نامشخص';
            retryEntry.lastResolvedAt = Date.now();
            retryEntry.filledAmount = Math.max(num(retryEntry.filledAmount), matched);
            retryEntry.filledQuoteEstimate = Math.max(num(retryEntry.filledQuoteEstimate), matched * Math.max(0, effectivePrice || retryEntry.estimatedPrice));
            retryEntry.updatedAt = Date.now();

            const desiredAllocation = Math.max(0, num(retryEntry.desiredAllocation));
            doneByAllocation = desiredAllocation > 0
              ? num(retryEntry.filledQuoteEstimate) >= (desiredAllocation * BUY_RETRY_COMPLETE_TOLERANCE)
              : matched > 0;

            retryEntry.status = doneByAllocation ? 'filled' : (matched > 0 ? 'partial' : 'needs_retry');
            retryEntry.lastError = doneByAllocation ? '' : (retryEntry.lastError || `سفارش ${id} با وضعیت ${status || 'نامشخص'} تمام شد.`);
          }

          const shouldAutoSweepRemainingBalance =
            pending.side === 'buy' &&
            !!pending.autoSweepRemainingBalance;

          delete state.pending[id];
          log(`سفارش ${id} در ${accountLabel(accountId)} با وضعیت ${status || 'نامشخص'} تمام شد.`, status === 'Done' ? 'ok' : 'warn');

          if (pending.side === 'sell' && status === 'Done' && num(order.matchedAmount || pending.appliedMatchedAmount) > 0) {
            await handleCompletedSellAftercare(pending, order);
          }

          if (shouldAutoSweepRemainingBalance && !hasPendingBuy(pending.base, pending.quote, accountId)) {
            await continueManualBuyRemainingBalance(pending.base, pending.quote, {
              sequenceId: String(pending.autoSweepSequenceId || retryEntry?.autoSweepSequenceId || ''),
              sweepIteration: Math.max(0, num(pending.autoSweepIteration)),
              manualSessionId: String(pending.manualSessionId || retryEntry?.manualSessionId || ''),
              manualSessionStartedAt: Math.max(0, num(pending.manualSessionStartedAt || retryEntry?.manualSessionStartedAt)),
              manualSessionExpiresAt: Math.max(0, num(pending.manualSessionExpiresAt || retryEntry?.manualSessionExpiresAt)),
              manualSessionCancelledAt: Math.max(0, num(pending.manualSessionCancelledAt || retryEntry?.manualSessionCancelledAt)),
              manualSessionStopReason: String(pending.manualSessionStopReason || retryEntry?.manualSessionStopReason || ''),
              accountId,
            });
          } else if (retryEntry && retryEntry.autoSweepRemainingBalance && doneByAllocation) {
            retryEntry.autoSweepComplete = true;
            retryEntry.autoSweepStopReason = '';
            retryEntry.updatedAt = Date.now();
          }
        } else {
          state.pending[id] = pending;
        }
      }

      await refreshOpenPositionsPnl(true);
      saveState();
    } catch (err) {
      logError(err, 'reconcilePendingOrders');
    } finally {
      reconcileBusy = false;
    }
  }

  /******************************************************************
   * فروش اجباری بعد از ۴ ساعت یا حد ضرر PnL
   ******************************************************************/
  async function checkForceSell() {
    if (forceSellBusy) return;
    if (!hasAnyConfiguredAccountToken()) return;

    const hasOpenPositions = Object.values(state.positions || {}).some(p => num(p.qty) > 0 && (p.openedAt || 0) > 0);
    if (!hasOpenPositions) return;

    forceSellBusy = true;

    try {
      const now = Date.now();
      await refreshOpenPositionsPnl(true);

      const openPositions = listOpenPositions()
        .filter(p => (p.openedAt || 0) > 0)
        .sort((a, b) => (a.openedAt || 0) - (b.openedAt || 0));

      if (!openPositions.length) return;

      const availableBaseByAccount = {};
      async function ensureForceSellAccountBalances(accountId) {
        const key = String(accountId || 'primary');
        if (availableBaseByAccount[key]) return availableBaseByAccount[key];
        const walletInfo = await loadWalletMapForAccount(key, { updatePrimarySummary: key === 'primary' });
        const bucket = {};
        for (const [currency, w] of Object.entries(walletInfo.map)) {
          bucket[currency.toUpperCase()] = walletAvailable(walletInfo.map, currency);
        }
        availableBaseByAccount[key] = bucket;
        return bucket;
      }

      for (const p of openPositions) {
        const accountId = String(p.accountId || 'primary');
        const base = p.base.toUpperCase();
        const quote = p.quote;

        if (hasPendingSell(base, quote, accountId)) continue;

        const dueByMaxHold = hasReachedMaxHold(p, now);
        const dueByPnlStop = hasReachedPnlForceSell(p, now);

        if (!dueByMaxHold && !dueByPnlStop) continue;

        const availableBase = await ensureForceSellAccountBalances(accountId);
        const walletQty = availableBase[base] || 0;
        const desiredQty = Math.min(num(p.qty), walletQty);

        if (!desiredQty || desiredQty <= 0) {
          log(`فروش اجباری ${base}/${quote} در ${accountLabel(accountId)} فعال شد ولی موجودی آزاد برای فروش نیست.`, 'warn');
          continue;
        }

        const reasonText = dueByPnlStop
          ? `PnL به ${formatSignedPercent(num(p.lastPnlPercent))} رسیده و حد ضرر ${formatSignedPercent(FORCE_SELL_PNL_THRESHOLD_PERCENT)} بعد از ${formatRemaining(FORCE_SELL_MIN_AGE_MS)} فعال شده است`
          : `مهلت نگه‌داری ${formatRemaining(HOLD_MAX_MS)} تمام شده است`;

        log(`فروش اجباری ${base}/${quote} در ${accountLabel(accountId)}: ${reasonText}. در حال فروش با نگه‌داشتن کمتر از ${formatFaNumber(SELL_KEEP_MAX_USDT_VALUE)} USDT از خود دارایی...`, 'warn');
        const result = await executeSell(base, quote, desiredQty, true, walletQty, { accountId });

        if (result?.status === 'placed') {
          clearForceSellSkip(p);
          availableBase[base] = Math.max(0, walletQty - num(result.sellQty));
          continue;
        }

        if (result?.status === 'skipped' && (result.reason === 'forced_below_min_notional' || result.reason === 'forced_keep_reserve')) {
          if (!sameForceSellSkip(p, result.reason)) {
            markForceSellSkip(p, result.reason);
            log(`فروش اجباری ${base}/${quote} در ${accountLabel(accountId)} برای ریزمانده/کمتر از حداقل ال‌بانک نادیده گرفته شد و دیگر تکرار نمی‌شود مگر مقدار دوباره بیشتر شود.`, 'warn');
          }
          continue;
        }

        clearForceSellSkip(p);
      }

      saveState();
    } catch (err) {
      logError(err, 'checkForceSell');
    } finally {
      forceSellBusy = false;
    }
  }
})();