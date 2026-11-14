#!/usr/bin/env node
/**
 * Official public-API snapshot for predictradar-data.
 * Pure Node 20+: global fetch, no npm deps, no API keys.
 *
 * Env (for tests / overrides):
 *   SNAPSHOT_OUT                 default: <repo>/data/snapshot.json
 *   SNAPSHOT_KALSHI_BASE         default: https://api.elections.kalshi.com/trade-api/v2
 *   SNAPSHOT_LIMITLESS_ACTIVE    default: https://api.limitless.exchange/markets/active
 *   SNAPSHOT_PREDICTIT_URL       default: https://www.predictit.org/api/marketdata/all/
 */
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const OUT = resolve(process.env.SNAPSHOT_OUT || `${ROOT}/data/snapshot.json`);

const KALSHI_BASE = (process.env.SNAPSHOT_KALSHI_BASE || "https://api.elections.kalshi.com/trade-api/v2").replace(/\/$/, "");
const LIMITLESS_ACTIVE = process.env.SNAPSHOT_LIMITLESS_ACTIVE || "https://api.limitless.exchange/markets/active";
const PREDICTIT_URL = process.env.SNAPSHOT_PREDICTIT_URL || "https://www.predictit.org/api/marketdata/all/";

const TIMEOUT_MS = 20_000;
const RETRIES = 1; // one retry = two attempts
const RETRY_WAIT_MS = 1_000;
const MAX_KALSHI = 80;
const MAX_LIMITLESS = 150;
const LIMITLESS_PAGE_SIZE = 25;
const TARGET_MAX_BYTES = 1_800_000;
const UA = "predictradar-data-snapshot/1 (+https://github.com/Makoso74/predictradar-data)";

// Seed from /workspace/kalshi-research/catalog.json plus the Dec 2026 Fed event.
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

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function httpError(status, message) {
  const e = new Error(message || `HTTP ${status}`);
  e.httpStatus = status;
  return e;
}

async function fetchJson(url) {
  let lastErr;
  for (let attempt = 0; attempt <= RETRIES; attempt++) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
    try {
      const res = await fetch(url, {
        signal: ctrl.signal,
        headers: { Accept: "application/json", "User-Agent": UA },
      });
      if (!res.ok) {
        const retryAfter = res.headers.get("retry-after");
        lastErr = httpError(res.status, `HTTP ${res.status} for ${url}`);
        const retryable = res.status === 429 || res.status >= 500;
        if (retryable && attempt < RETRIES) {
          const wait = retryAfter && /^\d+$/.test(retryAfter) ? Number(retryAfter) * 1000 : RETRY_WAIT_MS * (attempt + 1);
          await sleep(wait);
          continue;
        }
        throw lastErr;
      }
      return await res.json();
    } catch (e) {
      lastErr = e.name === "AbortError" ? new Error(`timeout ${TIMEOUT_MS}ms for ${url}`) : e;
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
    rules_primary: m.rules_primary ?? null,
    rules_secondary: m.rules_secondary ?? null,
  };
}

async function fetchKalshi() {
  const fetchedAt = nowIso();
  const byTicker = new Map();

  for (const eventTicker of KALSHI_EVENT_TICKERS) {
    let cursor = "";
    for (let page = 0; page < 10; page++) {
      const q = new URLSearchParams({ event_ticker: eventTicker, limit: "200" });
      if (cursor) q.set("cursor", cursor);
      const data = await fetchJson(`${KALSHI_BASE}/markets?${q}`);
      for (const m of data.markets || []) {
        if (m && m.ticker) byTicker.set(m.ticker, m);
      }
      cursor = data.cursor || "";
      if (!cursor || !(data.markets || []).length) break;
    }
  }

  for (const ticker of KALSHI_SEED_TICKERS) {
    if (byTicker.has(ticker)) continue;
    try {
      const data = await fetchJson(`${KALSHI_BASE}/markets/${encodeURIComponent(ticker)}`);
      const m = data.market || data;
      if (m && m.ticker) byTicker.set(m.ticker, m);
    } catch (e) {
      // Seed ticker may have expired or 404; skip, don't fail the whole source.
      if (e.httpStatus === 404) continue;
      throw e;
    }
  }

  const items = [...byTicker.values()].map(pickKalshi).sort((a, b) => String(a.ticker).localeCompare(String(b.ticker)));
  const truncated = items.length > MAX_KALSHI;
  return {
    name: "kalshi",
    ok: true,
    fetchedAt,
    error: null,
    httpStatus: 200,
    truncated,
    items: truncated ? items.slice(0, MAX_KALSHI) : items,
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
      status: c.status ?? null,
      lastTradePrice: c.lastTradePrice ?? null,
      bestBuyYesCost: c.bestBuyYesCost ?? null,
      bestBuyNoCost: c.bestBuyNoCost ?? null,
      bestSellYesCost: c.bestSellYesCost ?? null,
      bestSellNoCost: c.bestSellNoCost ?? null,
    }))
    .sort((a, b) => String(a.id).localeCompare(String(b.id), undefined, { numeric: true }));
  return {
    id: m.id ?? null,
    name: m.name ?? null,
    url: m.url ?? null,
    timeStamp: m.timeStamp ?? null,
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
  return {
    name,
    ok: false,
    stale: true,
    fetchedAt: failed.fetchedAt,
    previousFetchedAt: prevSrc.fetchedAt || null,
    error: failed.error,
    httpStatus: failed.httpStatus,
    count: prevItems.length,
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
  return meta;
}

function byteSize(obj) {
  return Buffer.byteLength(JSON.stringify(obj), "utf8");
}

function shrink(snapshot) {
  // Prefer dropping lower-priority Limitless rows (already not in high_value tail if truncated earlier).
  let notes = [];
  while (byteSize(snapshot) > TARGET_MAX_BYTES && snapshot.limitless.length > 20) {
    snapshot.limitless = snapshot.limitless.slice(0, Math.max(20, snapshot.limitless.length - 20));
    snapshot.sources.limitless.count = snapshot.limitless.length;
    snapshot.sources.limitless.truncated = true;
    notes.push("trimmed limitless to stay under size cap");
  }
  if (byteSize(snapshot) > TARGET_MAX_BYTES && snapshot.predictit.length) {
    for (const m of snapshot.predictit) {
      if (m.contracts && m.contracts.length > 6) m.contracts = m.contracts.slice(0, 6);
    }
    notes.push("trimmed predictit contracts");
  }
  if (notes.length) snapshot.sources.sizeNotes = notes;
  return snapshot;
}

function buildSnapshot(generatedAt, results) {
  const by = Object.fromEntries(results.map((r) => [r.name, r]));
  const snapshot = {
    schemaVersion: 1,
    generatedAt,
    sources: {
      kalshi: sourceMeta(by.kalshi),
      limitless: sourceMeta(by.limitless),
      predictit: sourceMeta(by.predictit),
    },
    kalshi: by.kalshi.items || [],
    limitless: by.limitless.items || [],
    predictit: by.predictit.items || [],
  };
  return shrink(snapshot);
}

async function main() {
  const generatedAt = nowIso();
  const prev = await loadPrevious();

  const jobs = [
    ["kalshi", fetchKalshi],
    ["limitless", fetchLimitless],
    ["predictit", fetchPredictIt],
  ];
  const results = [];
  for (const [name, fn] of jobs) {
    try {
      results.push(await fn());
      process.stderr.write(`${name}: ok ${results.at(-1).items.length}\n`);
    } catch (err) {
      const failed = failSource(name, err, nowIso());
      process.stderr.write(`${name}: FAIL ${failed.error}\n`);
      if (prev) results.push(staleFromPrevious(prev, name, failed));
      else results.push({ ...failed, items: [], stale: false });
    }
  }

  const anyOk = results.some((r) => r.ok);
  const hadPrev = Boolean(prev);
  if (!anyOk && !hadPrev) {
    console.error("all three sources failed and no previous snapshot exists");
    process.exit(1);
  }
  if (!anyOk && hadPrev) {
    // Still write: all stale, new generatedAt (freshness of the *attempt*).
    process.stderr.write("all three sources failed; writing stale snapshot from previous file\n");
  }

  const snapshot = buildSnapshot(generatedAt, results);
  await mkdir(dirname(OUT), { recursive: true });
  const text = JSON.stringify(snapshot, null, 2) + "\n";
  await writeFile(OUT, text);
  process.stderr.write(`wrote ${OUT} (${Buffer.byteLength(text)} bytes)\n`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
