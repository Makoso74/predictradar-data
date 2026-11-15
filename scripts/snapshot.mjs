#!/usr/bin/env node
/**
 * Official public-API snapshot for predictradar-data.
 * Pure Node 20+: global fetch, no npm deps, no API keys.
 *
 * Sources (each isolated; on failure the previous file's block is reused with stale:true):
 *   kalshi      markets (seed + event + series discovery) + batch order books (field `book`, not live)
 *   limitless   active markets
 *   predictit   all markets (one request per run; official limit 5 req/min)
 *   hyperliquid HIP-4 outcomeMeta + "#" coin contexts (spotMetaAndAssetCtxs) + titles/dates from Kairos md
 *   predictfun  ~30 active Predict.fun markets by 24h volume (Kairos public search + md batch, no key)
 *   opinion     top 40 Opinion markets by volume24h + Yes top of book (<= 4 req/s)
 *
 * Env (for tests / overrides):
 *   SNAPSHOT_OUT                 default: <repo>/data/snapshot.json
 *   SNAPSHOT_KALSHI_BASE         default: https://api.elections.kalshi.com/trade-api/v2
 *   SNAPSHOT_LIMITLESS_ACTIVE    default: https://api.limitless.exchange/markets/active
 *   SNAPSHOT_PREDICTIT_URL       default: https://www.predictit.org/api/marketdata/all/
 *   SNAPSHOT_HL_INFO             default: https://api.hyperliquid.xyz/info
 *   SNAPSHOT_KAIROS_MD           default: https://md.kairos.trade
 *   SNAPSHOT_KAIROS_SEARCH       default: https://data.kairos.trade
 *   SNAPSHOT_OPINION_BASE        default: https://openapi.opinion.trade/openapi
 *   SNAPSHOT_SOURCE_DEADLINE_SEC default: 100 (hard cap per new source: hyperliquid, predictfun, opinion)
 *   SNAPSHOT_MAX_BYTES           default: 1800000 (size cap for shrink; tests only)
 *   SNAPSHOT_ONLY                optional comma list of sources to fetch; others are copied from the
 *                                previous file as stale (tests only)
 *
 * Modes:
 *   node scripts/snapshot.mjs                      fetch all sources, write SNAPSHOT_OUT
 *   node scripts/snapshot.mjs --fingerprint FILE   print sha256 of FILE without time-only fields
 *                                                  (generatedAt, sources.*.fetchedAt/previousFetchedAt/
 *                                                  *FetchedAt, predictit[].timeStamp, opinion book.at);
 *                                                  used to skip unchanged pushes.
 */
import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const OUT = resolve(process.env.SNAPSHOT_OUT || `${ROOT}/data/snapshot.json`);

const KALSHI_BASE = (process.env.SNAPSHOT_KALSHI_BASE || "https://api.elections.kalshi.com/trade-api/v2").replace(/\/$/, "");
const LIMITLESS_ACTIVE = process.env.SNAPSHOT_LIMITLESS_ACTIVE || "https://api.limitless.exchange/markets/active";
const PREDICTIT_URL = process.env.SNAPSHOT_PREDICTIT_URL || "https://www.predictit.org/api/marketdata/all/";
const HL_INFO = process.env.SNAPSHOT_HL_INFO || "https://api.hyperliquid.xyz/info";
const KAIROS_MD = (process.env.SNAPSHOT_KAIROS_MD || "https://md.kairos.trade").replace(/\/$/, "");
const KAIROS_SEARCH = (process.env.SNAPSHOT_KAIROS_SEARCH || "https://data.kairos.trade").replace(/\/$/, "");
const OPINION_BASE = (process.env.SNAPSHOT_OPINION_BASE || "https://openapi.opinion.trade/openapi").replace(/\/$/, "");
const SOURCE_DEADLINE_MS = Number(process.env.SNAPSHOT_SOURCE_DEADLINE_SEC || 100) * 1000;
const ONLY = (process.env.SNAPSHOT_ONLY || "").split(",").map((s) => s.trim()).filter(Boolean);

const TIMEOUT_MS = 20_000;
const RETRIES = 1; // one retry = two attempts
const RETRY_WAIT_MS = 1_000;
const MAX_KALSHI = 80;
const MAX_LIMITLESS = 150;
const LIMITLESS_PAGE_SIZE = 25;
const TARGET_MAX_BYTES = Number(process.env.SNAPSHOT_MAX_BYTES || 1_800_000); // env only for tests
const UA = "predictradar-data-snapshot/1 (+https://github.com/Makoso74/predictradar-data)";

const KALSHI_GAP_MS = 150; // unauthenticated Kalshi answers 429 on bursts of ~10 req/s
const KALSHI_BOOK_LEVELS = 5;
const KALSHI_BOOK_BATCH = 100; // API max tickers per /markets/orderbooks call
const KALSHI_DROP_STATUSES = new Set(["closed", "determined", "finalized", "settled"]);

// Seed from /workspace/kalshi-research/catalog.json plus the Dec 2026 Fed event.
// Seeds that are closed/finalized/settled (or 404) are dropped automatically each run.
const KALSHI_EVENT_TICKERS = ["KXFEDDECISION-26DEC"];
const KALSHI_SEED_TICKERS = [
  "KXFEDDECISION-26DEC-H26",
  "KXFED-26DEC-T4.25",
  "KXBTCD-26NOV1417-T66999.99",
  "KXETHD-26NOV1417-T1899.99",
  "KXCPIYOY-26NOV-T3.6",
  "KXBTCMAXMON-BTC-26NOV30-8250000",
  "KXNFLGAME-26NOV15SFDAL-SF",
  "KXEPLGAME-26NOV21MCIFUL-MCI",
  "KXUFCFIGHT-26NOV14GANHOK-HOK",
  "KXBOND-30-ROB",
  "KXINXU-26NOV16H1600-T7434.9999",
  "KXSB-27-MIN",
];
// Rolling discovery: open markets of these series, top N by volume_24h (new dates appear by themselves).
const KALSHI_SERIES = [
  ["KXNFLGAME", 8],
  ["KXNBAGAME", 8],
  ["KXNHLGAME", 4],
  ["KXNCAAFGAME", 4],
  ["KXEPLGAME", 4],
  ["KXUFCFIGHT", 3],
  ["KXBTCD", 4],
  ["KXETHD", 2],
  ["KXBTCMAXMON", 3],
  ["KXFED", 3],
  ["KXCPIYOY", 2],
  ["KXINXU", 3],
];

const HL_KAIROS_PAGES = 3; // 200 per page
const PREDICTFUN_MAX = 30;
const PREDICTFUN_QUERIES = (process.env.SNAPSHOT_PREDICTFUN_QUERIES ||
  "will,win,vs,bitcoin,ethereum,up or down,price,2026,nba,nfl,election,fed").split(",").map((s) => s.trim()).filter(Boolean);
const PREDICTFUN_GAP_MS = 1_100; // Kairos search: 60 req/min anonymous
const PREDICTFUN_MIN_TTL_MS = 30 * 60_000; // skip markets expiring within 30 min (5m/15m up-down)
const OPINION_MAX = 40;
const OPINION_GAP_MS = 260; // <= 4 req/s (live limit measured ~5 req/s)
const OPINION_MAX_BOOKS = 120; // ~32 s at 4 req/s
const OPINION_MAX_CHILDREN = 8;
const OPINION_RULES_MAX = 1500;
const HL_DESCRIPTION_MAX = 600;

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function shortUrl(url) {
  const u = String(url);
  return u.length > 160 ? u.slice(0, 160) + "…" : u;
}

function httpError(status, message) {
  const e = new Error(message || `HTTP ${status}`);
  e.httpStatus = status;
  return e;
}

async function fetchJson(url, opts = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= RETRIES; attempt++) {
    if (opts.deadline && Date.now() > opts.deadline) throw new Error(`source deadline reached before ${shortUrl(url)}`);
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
    try {
      const headers = { Accept: "application/json", "User-Agent": UA };
      if (opts.body !== undefined) headers["Content-Type"] = "application/json";
      const res = await fetch(url, {
        method: opts.body !== undefined ? "POST" : "GET",
        body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
        signal: ctrl.signal,
        headers,
      });
      if (!res.ok) {
        const retryAfter = res.headers.get("retry-after");
        lastErr = httpError(res.status, `HTTP ${res.status} for ${shortUrl(url)}`);
        const retryable = res.status === 429 || res.status >= 500;
        if (retryable && attempt < RETRIES) {
          const wait = retryAfter && /^\d+$/.test(retryAfter) ? Number(retryAfter) * 1000 : RETRY_WAIT_MS * (attempt + 1);
          await sleep(Math.min(wait, 10_000));
          continue;
        }
        throw lastErr;
      }
      return await res.json();
    } catch (e) {
      if (e.httpStatus) throw e;
      if (e.message === "fetch failed" && e.cause) e.message = `fetch failed (${e.cause.code || e.cause.message}) for ${shortUrl(url)}`;
      lastErr = e.name === "AbortError" ? new Error(`timeout ${TIMEOUT_MS}ms for ${shortUrl(url)}`) : e;
      if (attempt < RETRIES) {
        await sleep(RETRY_WAIT_MS * (attempt + 1));
        continue;
      }
      throw lastErr;
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastErr;
}

function failSource(name, err, fetchedAt) {
  return {
    name,
    ok: false,
    fetchedAt,
    error: String(err && err.message ? err.message : err),
    httpStatus: Number.isInteger(err && err.httpStatus) ? err.httpStatus : null,
    items: null,
  };
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function round(v, d = 4) {
  return v == null ? null : Number(v.toFixed(d));
}

/* ------------------------------- Kalshi ------------------------------- */

let kalshiLast = 0;
async function kalshiGet(url) {
  const wait = kalshiLast + KALSHI_GAP_MS - Date.now();
  if (wait > 0) await sleep(wait);
  kalshiLast = Date.now();
  return fetchJson(url);
}

function pickKalshi(m) {
  const volume_fp = m.volume_fp ?? m.volume ?? null;
  const volume_24h_fp = m.volume_24h_fp ?? m.volume_24h ?? null;
  const open_interest_fp = m.open_interest_fp ?? m.open_interest ?? null;
  return {
    ticker: m.ticker ?? null,
    title: m.title ?? null,
    status: m.status ?? null,
    close_time: m.close_time ?? null,
    expected_expiration_time: m.expected_expiration_time ?? null,
    event_ticker: m.event_ticker ?? null,
    volume: volume_fp,
    volume_fp,
    volume_24h: volume_24h_fp,
    volume_24h_fp,
    open_interest: open_interest_fp,
    open_interest_fp,
    yes_bid: m.yes_bid_dollars ?? m.yes_bid ?? null,
    yes_ask: m.yes_ask_dollars ?? m.yes_ask ?? null,
    // As Kalshi REST returns them (dollar strings). Null when absent; never derived from yes_*.
    no_bid: m.no_bid_dollars ?? m.no_bid ?? null,
    no_ask: m.no_ask_dollars ?? m.no_ask ?? null,
    rules_primary: m.rules_primary ?? null,
    rules_secondary: m.rules_secondary ?? null,
  };
}

function kalshiDead(m) {
  if (KALSHI_DROP_STATUSES.has(String(m.status || "").toLowerCase())) return true;
  const close = Date.parse(m.close_time || "");
  return Number.isFinite(close) && close < Date.now();
}

/** Bid levels best-first: [[priceUSD, sizeContracts], ...] as Kalshi strings. */
function topBids(levels, n) {
  return (Array.isArray(levels) ? levels : [])
    .filter((l) => Array.isArray(l) && num(l[0]) != null && num(l[1]) != null)
    .sort((a, b) => num(b[0]) - num(a[0]))
    .slice(0, n)
    .map((l) => [String(l[0]), String(l[1])]);
}

function kalshiBook(ob) {
  const fp = ob.orderbook_fp || {};
  const yesAll = Array.isArray(fp.yes_dollars) ? fp.yes_dollars : [];
  const noAll = Array.isArray(fp.no_dollars) ? fp.no_dollars : [];
  const yesBids = topBids(yesAll, KALSHI_BOOK_LEVELS);
  const noBids = topBids(noAll, KALSHI_BOOK_LEVELS);
  // Kalshi books hold bids only. Buying YES lifts the best NO bid: yesAsk = 1 - bestNoBid (same size).
  const ask = (bids) => (bids.length ? { price: (1 - num(bids[0][0])).toFixed(4), size: bids[0][1] } : null);
  return {
    yesBids,
    noBids,
    yesAsk: ask(noBids),
    noAsk: ask(yesBids),
    levels: { yes: yesAll.length, no: noAll.length },
  };
}

async function fetchKalshiBooks(tickers) {
  const books = new Map();
  for (let i = 0; i < tickers.length; i += KALSHI_BOOK_BATCH) {
    const q = new URLSearchParams();
    // Repeated `tickers=` params; a comma list is read as one (unknown) ticker.
    for (const t of tickers.slice(i, i + KALSHI_BOOK_BATCH)) q.append("tickers", t);
    const data = await kalshiGet(`${KALSHI_BASE}/markets/orderbooks?${q}`);
    for (const ob of data.orderbooks || []) {
      if (ob && ob.ticker) books.set(ob.ticker, kalshiBook(ob));
    }
  }
  return books;
}

async function fetchKalshi(prev) {
  const fetchedAt = nowIso();
  const byTicker = new Map();
  const priority = new Set();

  for (const eventTicker of KALSHI_EVENT_TICKERS) {
    let cursor = "";
    for (let page = 0; page < 10; page++) {
      const q = new URLSearchParams({ event_ticker: eventTicker, limit: "200" });
      if (cursor) q.set("cursor", cursor);
      const data = await kalshiGet(`${KALSHI_BASE}/markets?${q}`);
      for (const m of data.markets || []) {
        if (m && m.ticker) {
          byTicker.set(m.ticker, m);
          priority.add(m.ticker);
        }
      }
      cursor = data.cursor || "";
      if (!cursor || !(data.markets || []).length) break;
    }
  }

  const missingSeeds = [];
  for (const ticker of KALSHI_SEED_TICKERS) {
    priority.add(ticker);
    if (byTicker.has(ticker)) continue;
    try {
      const data = await kalshiGet(`${KALSHI_BASE}/markets/${encodeURIComponent(ticker)}`);
      const m = data.market || data;
      if (m && m.ticker) byTicker.set(m.ticker, m);
    } catch (e) {
      // Seed ticker may have expired or 404; skip, don't fail the whole source.
      if (e.httpStatus === 404) {
        missingSeeds.push(ticker);
        continue;
      }
      throw e;
    }
  }

  // Series discovery is best-effort: one failing series does not fail Kalshi.
  const discovered = [];
  const seriesErrors = [];
  for (const [series, topN] of KALSHI_SERIES) {
    try {
      const q = new URLSearchParams({ series_ticker: series, status: "open", limit: "200", mve_filter: "exclude" });
      const data = await kalshiGet(`${KALSHI_BASE}/markets?${q}`);
      const ms = (data.markets || [])
        .filter((m) => m && m.ticker && !kalshiDead(m) && num(m.volume_24h_fp ?? m.volume_24h) > 0)
        .sort((a, b) => num(b.volume_24h_fp ?? b.volume_24h) - num(a.volume_24h_fp ?? a.volume_24h))
        .slice(0, topN);
      discovered.push(...ms);
    } catch (e) {
      seriesErrors.push(`${series}: ${e.message}`);
    }
  }
  discovered.sort((a, b) => num(b.volume_24h_fp ?? b.volume_24h) - num(a.volume_24h_fp ?? a.volume_24h));

  const dropped = [];
  const ordered = [];
  for (const t of priority) {
    const m = byTicker.get(t);
    if (!m) continue;
    if (kalshiDead(m)) dropped.push(m.ticker);
    else ordered.push(m);
  }
  for (const m of discovered) {
    if (!priority.has(m.ticker) && !ordered.some((x) => x.ticker === m.ticker)) ordered.push(m);
  }
  const truncated = ordered.length > MAX_KALSHI;
  const items = ordered
    .slice(0, MAX_KALSHI)
    .map(pickKalshi)
    .sort((a, b) => String(a.ticker).localeCompare(String(b.ticker)));

  // Order books ride on top: a failure here never fails the Kalshi source.
  const bookFetchedAt = nowIso();
  let bookMeta;
  try {
    const books = await fetchKalshiBooks(items.map((m) => m.ticker));
    for (const m of items) m.book = books.get(m.ticker) || null;
    bookMeta = { ok: true, fetchedAt: bookFetchedAt, count: books.size };
  } catch (e) {
    const prevBooks = new Map(
      (Array.isArray(prev?.kalshi) ? prev.kalshi : []).filter((m) => m && m.book).map((m) => [m.ticker, m.book]),
    );
    let reused = 0;
    for (const m of items) {
      const b = prevBooks.get(m.ticker);
      m.book = b ? { ...b, stale: true } : null;
      if (b) reused++;
    }
    bookMeta = {
      ok: false,
      fetchedAt: bookFetchedAt,
      count: reused,
      stale: reused > 0,
      previousFetchedAt: prev?.sources?.kalshi?.book?.fetchedAt || null,
      error: e.message,
    };
  }
  bookMeta.levelsMax = KALSHI_BOOK_LEVELS;
  bookMeta.units = "price: USD per contract (pays $1) as string; size: contracts as string; yesBids/noBids best first; yesAsk = 1 - best noBid, noAsk = 1 - best yesBid; snapshot price, not live";

  const extraMeta = { book: bookMeta };
  if (dropped.length) extraMeta.droppedSeeds = dropped;
  if (missingSeeds.length) extraMeta.missingSeeds = missingSeeds;
  if (seriesErrors.length) extraMeta.seriesErrors = seriesErrors;
  extraMeta.discovered = discovered.length;
  return {
    name: "kalshi",
    ok: true,
    fetchedAt,
    error: null,
    httpStatus: 200,
    truncated,
    extraMeta,
    items,
  };
}

function pickLimitless(m) {
  const tokens = m.tokens || {};
  const expMs = Number(m.expirationTimestamp);
  return {
    slug: m.slug ?? null,
    title: m.title ?? null,
    status: m.status ?? null,
    volume: m.volume ?? null,
    volumeFormatted: m.volumeFormatted ?? null,
    description: m.description ?? null,
    expirationTimestamp: m.expirationTimestamp ?? null,
    expiration: Number.isFinite(expMs) ? new Date(expMs).toISOString() : (m.expirationDate ?? null),
    yesTokenId: tokens.yes ?? m.yesTokenId ?? null,
    noTokenId: tokens.no ?? m.noTokenId ?? null,
    marketType: m.marketType ?? null,
    outcomes: m.marketType === "group" && Array.isArray(m.markets)
      ? m.markets.slice(0, 30).map((x) => ({
          slug: x.slug ?? null,
          title: x.title ?? null,
          yesTokenId: x.tokens?.yes ?? null,
          noTokenId: x.tokens?.no ?? null,
          volumeFormatted: x.volumeFormatted ?? null,
        }))
      : null,
  };
}

async function fetchLimitless() {
  const fetchedAt = nowIso();
  const items = [];
  const maxPages = Math.ceil(MAX_LIMITLESS / LIMITLESS_PAGE_SIZE) + 1;
  for (let page = 1; page <= maxPages && items.length < MAX_LIMITLESS; page++) {
    const q = new URLSearchParams({
      limit: String(LIMITLESS_PAGE_SIZE),
      page: String(page),
      sortBy: "high_value",
    });
    const data = await fetchJson(`${LIMITLESS_ACTIVE}?${q}`);
    const batch = data.data || data.markets || [];
    if (!batch.length) break;
    for (const m of batch) {
      items.push(pickLimitless(m));
      if (items.length >= MAX_LIMITLESS) break;
    }
    if (batch.length < LIMITLESS_PAGE_SIZE) break;
  }
  items.sort((a, b) => String(a.slug).localeCompare(String(b.slug)));
  return {
    name: "limitless",
    ok: true,
    fetchedAt,
    error: null,
    httpStatus: 200,
    truncated: items.length >= MAX_LIMITLESS,
    items,
  };
}

function pickPredictIt(m) {
  const contracts = (m.contracts || [])
    .map((c) => ({
      id: c.id ?? null,
      name: c.name ?? null,
      shortName: c.shortName ?? null,
      status: c.status ?? null,
      // Raw string as PredictIt sends it: ISO without zone (America/New_York), "NA", "N/A" or prose.
      dateEnd: c.dateEnd ?? null,
      displayOrder: c.displayOrder ?? null,
      lastTradePrice: c.lastTradePrice ?? null,
      bestBuyYesCost: c.bestBuyYesCost ?? null,
      bestBuyNoCost: c.bestBuyNoCost ?? null,
      bestSellYesCost: c.bestSellYesCost ?? null,
      bestSellNoCost: c.bestSellNoCost ?? null,
      lastClosePrice: c.lastClosePrice ?? null,
    }))
    .sort((a, b) => String(a.id).localeCompare(String(b.id), undefined, { numeric: true }));
  return {
    id: m.id ?? null,
    name: m.name ?? null,
    shortName: m.shortName ?? null,
    url: m.url ?? null,
    // Raw PredictIt time without zone; it is America/New_York, not UTC. Use fetchedAt/generatedAt for freshness.
    timeStamp: m.timeStamp ?? null,
    timeStampZone: "America/New_York",
    contractsTotal: (m.contracts || []).length,
    status: m.status ?? null,
    contracts,
  };
}

async function fetchPredictIt() {
  const fetchedAt = nowIso();
  const data = await fetchJson(PREDICTIT_URL);
  const items = (data.markets || []).map(pickPredictIt).sort((a, b) => Number(a.id) - Number(b.id));
  return {
    name: "predictit",
    ok: true,
    fetchedAt,
    error: null,
    httpStatus: 200,
    items,
  };
}

/* ----------------------------- Hyperliquid ----------------------------- */

function parseHlDescription(desc) {
  const out = {};
  for (const part of String(desc || "").split("|")) {
    const i = part.indexOf(":");
    if (i > 0) out[part.slice(0, i)] = part.slice(i + 1);
  }
  return out;
}

/** "20261123-0000" (UTC) -> ISO */
function hlTime(s) {
  const m = /^(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})$/.exec(String(s || ""));
  return m ? `${m[1]}-${m[2]}-${m[3]}T${m[4]}:${m[5]}:00.000Z` : null;
}

function hlTemplate(name, fields) {
  return String(name || "")
    .replace(/^template:/, "")
    .replace(/\{(\w+)\}/g, (_, k) => fields[k] ?? `{${k}}`);
}

async function fetchKairosAll(provider, pages, deadline) {
  const out = new Map();
  let cursor = "";
  for (let p = 0; p < pages; p++) {
    const q = new URLSearchParams({ provider, limit: "200" });
    if (cursor) q.set("cursor", cursor);
    const data = await fetchJson(`${KAIROS_MD}/v1/markets?${q}`, { deadline });
    for (const m of data.markets || []) if (m && m.market_id != null) out.set(String(m.market_id), m);
    cursor = data.next_cursor || "";
    if (!data.has_more || !cursor) break;
  }
  return out;
}

async function fetchHyperliquid(prev, deadline) {
  const fetchedAt = nowIso();
  const meta = await fetchJson(HL_INFO, { body: { type: "outcomeMeta" }, deadline });
  if (!meta || !Array.isArray(meta.outcomes)) throw new Error("outcomeMeta: no outcomes[]");

  let ctxByCoin = new Map();
  let ctxError = null;
  try {
    const spot = await fetchJson(HL_INFO, { body: { type: "spotMetaAndAssetCtxs" }, deadline });
    const ctxs = Array.isArray(spot) && Array.isArray(spot[1]) ? spot[1] : [];
    ctxByCoin = new Map(ctxs.filter((c) => String(c.coin || "").startsWith("#")).map((c) => [c.coin, c]));
  } catch (e) {
    ctxError = `spotMetaAndAssetCtxs: ${e.message}`;
  }

  let kairos = new Map();
  let kairosError = null;
  try {
    kairos = await fetchKairosAll("hyperliquid", HL_KAIROS_PAGES, deadline);
  } catch (e) {
    kairosError = `kairos md: ${e.message}`;
  }

  const questionOf = new Map();
  for (const q of meta.questions || []) {
    for (const o of [q.fallbackOutcome, ...(q.namedOutcomes || [])]) if (o != null) questionOf.set(o, q.question);
  }

  const items = meta.outcomes.map((o) => {
    const fields = parseHlDescription(o.description);
    const coinYes = `#${10 * o.outcome}`;
    const coinNo = `#${10 * o.outcome + 1}`;
    const cy = ctxByCoin.get(coinYes) || null;
    const cn = ctxByCoin.get(coinNo) || null;
    const k = kairos.get(String(o.outcome)) || null;
    const vol = cy || cn ? round((num(cy?.dayNtlVlm) || 0) + (num(cn?.dayNtlVlm) || 0), 2) : null;
    const endAt = k?.end_date || hlTime(fields.resolutionDeadline) || hlTime(fields.time) || hlTime(fields.expiry) || null;
    return {
      outcome: o.outcome,
      coinYes,
      coinNo,
      template: String(o.name || "").replace(/^template:/, ""),
      title: k?.title || null,
      sides: (o.sideSpecs || []).map((s) => hlTemplate(s.name, fields)),
      description: String(o.description || "").slice(0, HL_DESCRIPTION_MAX),
      questionId: questionOf.get(o.outcome) ?? null,
      kairosEventId: k?.event_id ?? null,
      category: k?.category ?? null,
      endAt,
      scheduledStart: hlTime(fields.scheduledStart),
      quoteToken: o.quoteToken ?? null,
      venue: o.venue ?? null,
      deployerFeeScale: o.deployerFeeScale ?? null,
      volume24hUsd: vol, // dayNtlVlm Yes + No coins, quote token (USDC)
      markPxYes: cy?.markPx ?? null, // reference only, NOT live; live price = WS l2Book/bbo
      markPxNo: cn?.markPx ?? null,
      circulatingSupplyYes: cy?.circulatingSupply ?? null,
    };
  });
  items.sort((a, b) => a.outcome - b.outcome);

  const questions = (meta.questions || [])
    .map((q) => ({
      question: q.question,
      template: String(q.name || "").replace(/^template:/, ""),
      description: String(q.description || "").slice(0, HL_DESCRIPTION_MAX),
      fallbackOutcome: q.fallbackOutcome ?? null,
      namedOutcomes: q.namedOutcomes || [],
      settledNamedOutcomes: q.settledNamedOutcomes || [],
    }))
    .sort((a, b) => a.question - b.question);

  const extraMeta = {};
  const partial = [ctxError, kairosError].filter(Boolean);
  if (partial.length) extraMeta.partialErrors = partial;
  extraMeta.feeScale = meta.feeScale ?? null;
  return {
    name: "hyperliquid",
    ok: true,
    fetchedAt,
    error: null,
    httpStatus: 200,
    extraMeta,
    extras: { hyperliquidQuestions: questions },
    items,
  };
}

/* ------------------------------ Predict.fun ----------------------------- */

function slugDate(s) {
  const all = String(s || "").match(/\d{4}-\d{2}-\d{2}/g);
  return all ? all[all.length - 1] : null;
}

async function fetchPredictfun(prev, deadline) {
  const fetchedAt = nowIso();
  const found = new Map();
  const errors = [];
  for (let i = 0; i < PREDICTFUN_QUERIES.length; i++) {
    if (i) await sleep(PREDICTFUN_GAP_MS);
    if (Date.now() > deadline - 15_000) {
      errors.push("query budget exhausted");
      break;
    }
    const q = new URLSearchParams({ q: PREDICTFUN_QUERIES[i], provider: "predictfun", limit: "100", statuses: "open" });
    try {
      const data = await fetchJson(`${KAIROS_SEARCH}/search/markets?${q}`, { deadline });
      for (const m of data.results || []) if (m && m.market_id != null) found.set(String(m.market_id), m);
    } catch (e) {
      errors.push(`${PREDICTFUN_QUERIES[i]}: ${e.message}`);
      if (!found.size && errors.length >= 3) break; // search is down; do not burn the budget
    }
  }
  if (!found.size) throw new Error(`predictfun search: nothing found (${errors[0] || "empty"}; ${errors.length} query errors)`);

  const now = Date.now();
  const yesterday = new Date(now - 86_400_000).toISOString().slice(0, 10);
  const picked = [...found.values()]
    .filter((m) => String(m.status || "").toLowerCase() === "open")
    .filter((m) => !m.book_unavailable)
    .filter((m) => num(m.volume_24h) > 0)
    .filter((m) => {
      const exp = Date.parse(m.expires_at || "");
      return !Number.isFinite(exp) || exp > now + PREDICTFUN_MIN_TTL_MS;
    })
    .filter((m) => !/updown-(5m|15m)/.test(String(m.slug || m.event_id || "")))
    .filter((m) => {
      const d = slugDate(m.slug || m.event_id);
      return !d || d >= yesterday;
    })
    .sort((a, b) => num(b.volume_24h) - num(a.volume_24h))
    .slice(0, PREDICTFUN_MAX);

  // Kairos md batch: tick size, fee, status. Best-effort.
  let md = {};
  try {
    const data = await fetchJson(`${KAIROS_MD}/v1/markets/batch`, {
      body: { provider: "predictfun", market_ids: picked.map((m) => String(m.market_id)) },
      deadline,
    });
    md = data.markets || {};
  } catch (e) {
    errors.push(`md batch: ${e.message}`);
  }

  const items = picked
    .filter((m) => {
      const st = md[String(m.market_id)]?.status;
      return !st || String(st).toLowerCase() === "open";
    })
    .map((m) => {
      const d = md[String(m.market_id)] || {};
      const eventSlug = m.event_id || d.event_id || null;
      return {
        id: String(m.market_id),
        wsTopic: `predictOrderbook/${m.market_id}`,
        title: m.name ?? d.title ?? null,
        eventTitle: m.event_name ?? null,
        eventSlug,
        url: eventSlug ? `https://predict.fun/market/${eventSlug}` : null,
        category: m.category ?? d.category ?? null,
        outcomes: Array.isArray(m.outcomes) ? m.outcomes : [],
        outcomeLabel: m.outcome_label ?? null,
        expiresAt: m.expires_at ?? d.end_date ?? null,
        dateHint: slugDate(eventSlug), // YYYY-MM-DD parsed from the slug, may be null
        volume24hUsd: num(m.volume_24h), // per Kairos search (USD stablecoin), not cross-checked with predict.fun
        volume1hUsd: num(m.volume_1h),
        liquidityUsd: num(m.liquidity),
        tickSize: num(d.tick_size),
        feeRateBps: num(d.raw?.fee_rate_bps),
        negRisk: d.neg_risk ?? null,
      };
    });
  if (!items.length) throw new Error("predictfun: no active markets after filters");
  const extraMeta = { queries: PREDICTFUN_QUERIES.length, candidates: found.size, via: "kairos" };
  if (errors.length) extraMeta.partialErrors = errors;
  return { name: "predictfun", ok: true, fetchedAt, error: null, httpStatus: 200, extraMeta, items };
}

/* -------------------------------- Opinion ------------------------------- */

let opinionLast = 0;
async function opinionGet(path, deadline) {
  const wait = opinionLast + OPINION_GAP_MS - Date.now();
  if (wait > 0) await sleep(wait);
  opinionLast = Date.now();
  const data = await fetchJson(`${OPINION_BASE}${path}`, { deadline });
  if (!data || data.errno !== 0) throw new Error(`opinion ${path}: errno ${data && data.errno} ${data && data.errmsg}`);
  return data.result;
}

function opinionTop(result) {
  const lv = (arr) => (Array.isArray(arr) ? arr : []).filter((l) => num(l.price) != null && num(l.size) > 0);
  const asks = lv(result.asks).sort((a, b) => num(a.price) - num(b.price));
  const bids = lv(result.bids).sort((a, b) => num(b.price) - num(a.price));
  const ts = num(result.timestamp);
  return {
    bestAsk: asks[0] ? [String(asks[0].price), String(asks[0].size)] : null,
    bestBid: bids[0] ? [String(bids[0].price), String(bids[0].size)] : null,
    at: ts ? new Date(ts).toISOString() : null,
  };
}

function opinionCutoff(v) {
  const n = num(v);
  return n && n > 0 ? new Date(n * 1000).toISOString() : null;
}

async function fetchOpinion(prev, deadline) {
  const fetchedAt = nowIso();
  const cands = [];
  for (const marketType of [0, 1]) {
    for (let page = 1; page <= 2; page++) {
      const q = new URLSearchParams({ status: "activated", marketType: String(marketType), sortBy: "5", limit: "20", page: String(page) });
      const r = await opinionGet(`/market?${q}`, deadline);
      const list = r.list || [];
      for (const m of list) cands.push(m);
      if (list.length < 20) break;
    }
  }
  const seen = new Set();
  const top = cands
    .filter((m) => m && m.marketId != null && !seen.has(m.marketId) && seen.add(m.marketId))
    .sort((a, b) => (num(b.volume24h) || 0) - (num(a.volume24h) || 0))
    .slice(0, OPINION_MAX);

  const items = top.map((m) => {
    const categorical = Array.isArray(m.childMarkets) && m.childMarkets.length > 0;
    const rules = String(m.rules || "");
    const item = {
      marketId: m.marketId,
      type: categorical ? "categorical" : "binary",
      title: m.marketTitle ?? null,
      slug: m.slug ?? null,
      labels: m.labels || [],
      rules: rules.length > OPINION_RULES_MAX ? rules.slice(0, OPINION_RULES_MAX) + "…" : rules,
      rulesTruncated: rules.length > OPINION_RULES_MAX,
      cutoffAt: opinionCutoff(m.cutoffAt),
      volume24h: m.volume24h ?? null, // USD (quote token USDT on BNB chain), string as sent
      volume: m.volume ?? null,
      yesTokenId: categorical ? null : m.yesTokenId ?? null,
      noTokenId: categorical ? null : m.noTokenId ?? null,
      book: null,
    };
    if (categorical) {
      item.childrenTotal = m.childMarkets.length;
      item.children = m.childMarkets
        .filter((c) => !c.statusEnum || String(c.statusEnum).toLowerCase() === "activated")
        .sort((a, b) => (num(b.volume) || 0) - (num(a.volume) || 0))
        .slice(0, OPINION_MAX_CHILDREN)
        .map((c) => ({
          marketId: c.marketId,
          title: c.marketTitle ?? null,
          yesTokenId: c.yesTokenId ?? null,
          noTokenId: c.noTokenId ?? null,
          volume: c.volume ?? null,
          book: null,
        }));
    }
    return item;
  });

  // Yes top of book; budgeted, best-effort per token.
  // Pass 1: every market (binary Yes, or the top child of a categorical); pass 2: other children.
  const targets = [];
  for (const it of items) {
    if (it.yesTokenId) targets.push(it);
    else if (it.children && it.children[0] && it.children[0].yesTokenId) targets.push(it.children[0]);
  }
  for (const it of items) for (const c of (it.children || []).slice(1)) if (c.yesTokenId) targets.push(c);
  let books = 0;
  let bookErrors = 0;
  for (const t of targets.slice(0, OPINION_MAX_BOOKS)) {
    if (Date.now() > deadline - 5_000) break;
    try {
      const r = await opinionGet(`/token/orderbook?${new URLSearchParams({ token_id: t.yesTokenId })}`, deadline);
      t.book = opinionTop(r);
      books++;
    } catch {
      bookErrors++;
    }
  }
  // Tokens left without a fresh book keep the previous one, marked stale.
  const prevBooks = new Map();
  for (const m of Array.isArray(prev?.opinion) ? prev.opinion : []) {
    if (m.yesTokenId && m.book) prevBooks.set(m.yesTokenId, m.book);
    for (const c of m.children || []) if (c.yesTokenId && c.book) prevBooks.set(c.yesTokenId, c.book);
  }
  let staleBooks = 0;
  for (const t of targets) {
    if (t.book) continue;
    const b = prevBooks.get(t.yesTokenId);
    if (b) {
      t.book = { ...b, stale: true };
      staleBooks++;
    }
  }
  const extraMeta = {
    books,
    staleBooks,
    booksSkipped: Math.max(0, targets.length - books - bookErrors),
    units: "book = Yes token top of book [price USD per share, size shares] as strings; Opinion books are mirrored (No ask = 1 - Yes bid); snapshot price, not live",
  };
  if (bookErrors) extraMeta.bookErrors = bookErrors;
  return { name: "opinion", ok: true, fetchedAt, error: null, httpStatus: 200, extraMeta, items };
}

/* ------------------------------ assembly ------------------------------ */

const LEGACY_SOURCES = ["kalshi", "limitless", "predictit"];
const NEW_SOURCES = ["hyperliquid", "predictfun", "opinion"];
const SOURCE_EXTRAS = { hyperliquid: ["hyperliquidQuestions"] };

async function loadPrevious() {
  try {
    const text = await readFile(OUT, "utf8");
    const json = JSON.parse(text);
    if (!json || json.schemaVersion !== 1) return null;
    return json;
  } catch {
    return null;
  }
}

function staleFromPrevious(prev, name, failed) {
  const prevItems = prev && Array.isArray(prev[name]) ? prev[name] : [];
  const prevSrc = prev && prev.sources && prev.sources[name] ? prev.sources[name] : {};
  const extras = {};
  for (const key of SOURCE_EXTRAS[name] || []) extras[key] = prev && Array.isArray(prev[key]) ? prev[key] : [];
  return {
    name,
    ok: false,
    stale: true,
    fetchedAt: failed.fetchedAt,
    previousFetchedAt: prevSrc.fetchedAt || prevSrc.previousFetchedAt || null,
    error: failed.error,
    httpStatus: failed.httpStatus,
    count: prevItems.length,
    // Keep the previous book meta so the app can still tell how old kalshi[].book is.
    extraMeta: prevSrc.book ? { book: { ...prevSrc.book, stale: true } } : undefined,
    extras,
    items: prevItems,
  };
}

function sourceMeta(src) {
  const meta = {
    ok: src.ok,
    fetchedAt: src.fetchedAt,
    count: Array.isArray(src.items) ? src.items.length : 0,
  };
  if (src.error) meta.error = src.error;
  if (src.httpStatus != null) meta.httpStatus = src.httpStatus;
  if (src.stale) meta.stale = true;
  if (src.previousFetchedAt) meta.previousFetchedAt = src.previousFetchedAt;
  if (src.truncated) meta.truncated = true;
  if (src.extraMeta) Object.assign(meta, src.extraMeta);
  return meta;
}

function byteSize(obj) {
  // Measure what is written (pretty-printed), not the compact form.
  return Buffer.byteLength(JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function shrink(snapshot) {
  const notes = [];
  const over = () => byteSize(snapshot) > TARGET_MAX_BYTES;
  const mark = (name, note) => {
    if (snapshot.sources[name]) snapshot.sources[name].truncated = true;
    notes.push(note);
  };
  // 1. New blocks first: they are lists for discovery, the app does not depend on full text.
  if (over() && snapshot.opinion.length) {
    for (const m of snapshot.opinion) {
      if (m.rules && m.rules.length > 500) {
        m.rules = m.rules.slice(0, 500) + "…";
        m.rulesTruncated = true;
      }
      if (m.children && m.children.length > 4) m.children = m.children.slice(0, 4);
    }
    mark("opinion", "trimmed opinion rules/children");
  }
  if (over() && snapshot.hyperliquid.length) {
    for (const o of snapshot.hyperliquid) o.description = "";
    for (const q of snapshot.hyperliquidQuestions || []) q.description = "";
    mark("hyperliquid", "dropped hyperliquid descriptions");
  }
  if (over() && snapshot.kalshi.length) {
    for (const m of snapshot.kalshi) {
      if (m.book) {
        m.book.yesBids = (m.book.yesBids || []).slice(0, 2);
        m.book.noBids = (m.book.noBids || []).slice(0, 2);
      }
    }
    if (snapshot.sources.kalshi.book) snapshot.sources.kalshi.book.levelsMax = 2;
    notes.push("trimmed kalshi book levels to 2");
  }
  // 2. Original order: Limitless rows, then PredictIt contracts.
  while (over() && snapshot.limitless.length > 20) {
    snapshot.limitless = snapshot.limitless.slice(0, Math.max(20, snapshot.limitless.length - 20));
    snapshot.sources.limitless.count = snapshot.limitless.length;
    snapshot.sources.limitless.truncated = true;
    notes.push("trimmed limitless to stay under size cap");
  }
  if (over() && snapshot.predictit.length) {
    const order = (c) => (Number.isFinite(c.displayOrder) ? c.displayOrder : Number.MAX_SAFE_INTEGER);
    for (const m of snapshot.predictit) {
      if (m.contracts && m.contracts.length > 6) {
        // Keep the first 6 by PredictIt's displayOrder and mark the market as truncated.
        m.contracts = [...m.contracts].sort((a, b) => order(a) - order(b)).slice(0, 6);
        m.truncated = true;
      }
    }
    snapshot.sources.predictit.truncated = true;
    notes.push("trimmed predictit contracts");
  }
  // 3. Last resort: shorter new lists (hyperliquid keeps the most traded outcomes).
  if (over()) {
    snapshot.hyperliquid = [...snapshot.hyperliquid]
      .sort((a, b) => (b.volume24hUsd || 0) - (a.volume24hUsd || 0))
      .slice(0, 100)
      .sort((a, b) => a.outcome - b.outcome);
    snapshot.opinion = snapshot.opinion.slice(0, 20);
    snapshot.predictfun = snapshot.predictfun.slice(0, 15);
    for (const n of NEW_SOURCES) snapshot.sources[n].count = snapshot[n].length;
    for (const n of NEW_SOURCES) mark(n, `cut ${n} list`);
  }
  if (notes.length) snapshot.sources.sizeNotes = notes;
  return snapshot;
}

function buildSnapshot(generatedAt, results) {
  const by = Object.fromEntries(results.map((r) => [r.name, r]));
  const snapshot = {
    schemaVersion: 1,
    generatedAt,
    sources: {},
  };
  for (const n of [...LEGACY_SOURCES, ...NEW_SOURCES]) snapshot.sources[n] = sourceMeta(by[n]);
  for (const n of [...LEGACY_SOURCES, ...NEW_SOURCES]) snapshot[n] = by[n].items || [];
  for (const n of NEW_SOURCES) {
    for (const key of SOURCE_EXTRAS[n] || []) snapshot[key] = (by[n].extras && by[n].extras[key]) || [];
  }
  return shrink(snapshot);
}

function withDeadline(promise, ms, name) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, rej) => {
      timer = setTimeout(() => rej(new Error(`${name}: deadline ${ms / 1000}s exceeded`)), ms);
    }),
  ]).finally(() => clearTimeout(timer));
}

async function runSource(name, fn, prev) {
  const t0 = Date.now();
  if (ONLY.length && !ONLY.includes(name)) {
    const failed = failSource(name, new Error("skipped (SNAPSHOT_ONLY)"), nowIso());
    return prev ? staleFromPrevious(prev, name, failed) : { ...failed, items: [], stale: false };
  }
  try {
    const r = await fn();
    process.stderr.write(`${name}: ok ${r.items.length} (${((Date.now() - t0) / 1000).toFixed(1)}s)\n`);
    return r;
  } catch (err) {
    const failed = failSource(name, err, nowIso());
    process.stderr.write(`${name}: FAIL ${failed.error} (${((Date.now() - t0) / 1000).toFixed(1)}s)\n`);
    if (prev) return staleFromPrevious(prev, name, failed);
    return { ...failed, items: [], stale: false };
  }
}

async function main() {
  const generatedAt = nowIso();
  const prev = await loadPrevious();
  const deadline = Date.now() + SOURCE_DEADLINE_MS;

  // Legacy sources stay sequential (as before); new sources run beside them, each with a hard deadline.
  const legacy = (async () => {
    const out = [];
    out.push(await runSource("kalshi", () => fetchKalshi(prev), prev));
    out.push(await runSource("limitless", fetchLimitless, prev));
    out.push(await runSource("predictit", fetchPredictIt, prev));
    return out;
  })();
  const fresh = [
    ["hyperliquid", fetchHyperliquid],
    ["predictfun", fetchPredictfun],
    ["opinion", fetchOpinion],
  ].map(([name, fn]) => runSource(name, () => withDeadline(fn(prev, deadline), SOURCE_DEADLINE_MS, name), prev));

  const results = [...(await legacy), ...(await Promise.all(fresh))];

  const anyOk = results.some((r) => r.ok);
  const hadPrev = Boolean(prev);
  if (!anyOk && !hadPrev) {
    console.error("all sources failed and no previous snapshot exists");
    process.exit(1);
  }
  if (!anyOk && hadPrev) {
    // Still write: all stale, new generatedAt (freshness of the *attempt*).
    process.stderr.write("all sources failed; writing stale snapshot from previous file\n");
  }

  const snapshot = buildSnapshot(generatedAt, results);
  await mkdir(dirname(OUT), { recursive: true });
  const text = JSON.stringify(snapshot, null, 2) + "\n";
  await writeFile(OUT, text);
  process.stderr.write(`wrote ${OUT} (${Buffer.byteLength(text)} bytes)\n`);
}

function canonical(v) {
  if (Array.isArray(v)) return v.map(canonical);
  if (v && typeof v === "object") {
    return Object.fromEntries(Object.keys(v).sort().map((k) => [k, canonical(v[k])]));
  }
  return v;
}

function dropTimeKeys(meta) {
  if (!meta || typeof meta !== "object") return;
  for (const k of Object.keys(meta)) {
    if (k === "fetchedAt" || k === "previousFetchedAt" || /FetchedAt$/.test(k)) delete meta[k];
    else if (meta[k] && typeof meta[k] === "object" && !Array.isArray(meta[k])) dropTimeKeys(meta[k]);
  }
}

export function fingerprint(snapshot) {
  const s = JSON.parse(JSON.stringify(snapshot));
  delete s.generatedAt;
  // sources.*.fetchedAt/previousFetchedAt and nested ones (sources.kalshi.book.fetchedAt).
  for (const meta of Object.values(s.sources || {})) dropTimeKeys(meta);
  // PredictIt feed-wide clock, ticks every minute even when prices do not move.
  for (const m of Array.isArray(s.predictit) ? s.predictit : []) delete m.timeStamp;
  // Opinion order book server clock.
  for (const m of Array.isArray(s.opinion) ? s.opinion : []) {
    if (m.book) delete m.book.at;
    for (const c of m.children || []) if (c.book) delete c.book.at;
  }
  return createHash("sha256").update(JSON.stringify(canonical(s))).digest("hex");
}

async function fingerprintCli(file) {
  try {
    const json = JSON.parse(await readFile(file, "utf8"));
    process.stdout.write(fingerprint(json) + "\n");
  } catch (err) {
    process.stderr.write(`fingerprint: ${err.message}\n`);
    process.stdout.write("none\n");
  }
}

const fpIdx = process.argv.indexOf("--fingerprint");
const run = fpIdx !== -1 ? () => fingerprintCli(process.argv[fpIdx + 1]) : main;

run()
  .then(() => {
    // A source cut by its deadline may still hold a socket open; do not let it delay exit.
    if (fpIdx === -1) process.exit(0);
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
