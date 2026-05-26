import { createWriteStream, mkdirSync, type WriteStream } from "node:fs";
import { dirname } from "node:path";

import type { DashboardBroadcaster } from "../dashboard/server.js";
import type { ExchangeName, FeedUpdate, Quote } from "./types.js";
import { nowUnixMs } from "./types.js";

const UPDATE_DELAY_MS = 300.0;
const DROP_DELAY_MS = 500.0;
const RESTORE_DELAY_MS = 250.0;
const WINDOW_SECONDS = 300;

const EXCHANGES: readonly { name: ExchangeName; weight: number }[] = [
  { name: "bybit", weight: 0.2 },
  { name: "gemini", weight: 0.2 },
  { name: "binance", weight: 0.25 },
  { name: "bitstamp", weight: 0.05 },
  { name: "coinbase", weight: 0.3 },
];

export interface AggregateConfig {
  logPath: string;
}

type PriceMap = Partial<Record<ExchangeName, number | null>>;

export class Aggregator {
  private readonly state = new LatestQuotes();
  private readonly writer: WriteStream;
  private currentWindowStart: number | null = null;
  private baseline: PriceMap | null = null;
  private baselineComposite: number | null = null;

  constructor(
    config: AggregateConfig,
    private readonly dashboard: DashboardBroadcaster,
  ) {
    mkdirSync(dirname(config.logPath), { recursive: true });
    this.writer = createWriteStream(config.logPath, { flags: "a" });
  }

  onUpdate(update: FeedUpdate): void {
    const localTimestampMs = nowUnixMs();
    const window = Window.fromTimestampMs(localTimestampMs);

    this.state.update(update);
    this.writeLine(this.state.logLine(localTimestampMs));

    let shouldLogWindow = false;
    if (this.currentWindowStart === null) {
      this.currentWindowStart = window.startSeconds;
      if (window.containsStartSecond(localTimestampMs)) {
        this.baseline = this.state.prices();
        this.baselineComposite = this.state.compositePrice();
        shouldLogWindow = true;
      }
    } else if (this.currentWindowStart !== window.startSeconds) {
      this.currentWindowStart = window.startSeconds;
      this.baseline = this.state.prices();
      this.baselineComposite = this.state.compositePrice();
      shouldLogWindow = true;
    }

    if (shouldLogWindow) {
      this.writeLine(this.state.windowLogLine(window, localTimestampMs));
    }

    const snapshot = this.state.dashboardSnapshot(
      localTimestampMs,
      window,
      this.baseline,
      this.baselineComposite,
    );
    this.dashboard.broadcast(JSON.stringify(snapshot));
  }

  close(): Promise<void> {
    return new Promise((resolve) => this.writer.end(resolve));
  }

  private writeLine(line: string): void {
    this.writer.write(line + "\n");
  }
}

class LatestQuotes {
  private readonly acceptedQuotes = new Map<ExchangeName, Quote>();
  private readonly rawQuotes = new Map<ExchangeName, Quote>();
  private readonly enabled = new Map<ExchangeName, boolean>();

  update(update: FeedUpdate): void {
    const { exchange, quote } = update;
    const delayMs = quote.delayMs;
    const wasEnabled = this.enabled.get(exchange) ?? false;
    const hasAcceptedQuote = this.acceptedQuotes.has(exchange);

    this.rawQuotes.set(exchange, quote);

    if (delayMs !== null && wasEnabled && delayMs > DROP_DELAY_MS) {
      this.enabled.set(exchange, false);
    } else if (delayMs !== null && wasEnabled && delayMs <= UPDATE_DELAY_MS) {
      this.acceptedQuotes.set(exchange, quote);
      this.enabled.set(exchange, true);
    } else if (delayMs !== null && wasEnabled) {
      this.enabled.set(exchange, true);
    } else if (
      delayMs !== null &&
      (delayMs <= RESTORE_DELAY_MS || (!hasAcceptedQuote && delayMs <= UPDATE_DELAY_MS))
    ) {
      this.acceptedQuotes.set(exchange, quote);
      this.enabled.set(exchange, true);
    } else {
      this.enabled.set(exchange, false);
    }
  }

  get(exchange: ExchangeName): Quote | undefined {
    return this.rawQuotes.get(exchange);
  }

  acceptedQuote(exchange: ExchangeName): Quote | undefined {
    return this.acceptedQuotes.get(exchange);
  }

  compositePrice(): number | null {
    let totalWeight = 0;
    let weightedSum = 0;

    for (const ex of EXCHANGES) {
      const quote = this.eligibleQuote(ex.name);
      if (quote !== undefined) {
        totalWeight += ex.weight;
        weightedSum += quote.mid() * ex.weight;
      }
    }

    return totalWeight === 0 ? null : weightedSum / totalWeight;
  }

  prices(): PriceMap {
    const prices: PriceMap = {};
    for (const ex of EXCHANGES) {
      prices[ex.name] = this.get(ex.name)?.mid() ?? null;
    }
    return prices;
  }

  logLine(localTimestampMs: number): string {
    const composite = formatPrice8(this.compositePrice());
    const exchanges = EXCHANGES.map((ex) => this.formatExchange(ex.name));
    return `${localTimestampMs.toFixed(0)} -> ${exchanges.join(", ")} -> ${composite}`;
  }

  windowLogLine(window: Window, localTimestampMs: number): string {
    const composite = formatPrice8(this.compositePrice());
    const exchanges = EXCHANGES.map((ex) => this.formatExchange(ex.name));
    return `WINDOW start=${window.startSeconds} end=${window.endSeconds} frozen_at=${localTimestampMs.toFixed(0)} -> ${exchanges.join(", ")} -> ${composite}`;
  }

  dashboardSnapshot(
    localTimestampMs: number,
    window: Window,
    baseline: PriceMap | null,
    baselineComposite: number | null,
  ): Record<string, unknown> {
    const price = this.compositePrice();
    const compositeChange =
      price !== null && baselineComposite !== null ? price - baselineComposite : null;

    return {
      timestamp: Math.trunc(localTimestampMs),
      window_start: window.startSeconds,
      window_end: window.endSeconds,
      price: roundedOrNull(price, 4),
      change: roundedOrNull(compositeChange, 4),
      baseline: roundedOrNull(baselineComposite, 4),
      exchanges: this.exchangesSnapshot(baseline),
    };
  }

  private eligibleQuote(exchange: ExchangeName): Quote | undefined {
    return this.enabled.get(exchange) === true ? this.acceptedQuote(exchange) : undefined;
  }

  private formatExchange(exchange: ExchangeName): string {
    const quote = this.get(exchange);
    if (quote === undefined) {
      return `${exchange} NA (NA)`;
    }

    const delay = quote.delayMs !== null ? `${quote.delayMs.toFixed(2)}ms` : "NA";
    return `${exchange} ${quote.mid().toFixed(8)} (${delay})`;
  }

  private exchangesSnapshot(baseline: PriceMap | null): Record<ExchangeName, unknown> {
    const exchanges = {} as Record<ExchangeName, unknown>;
    for (const ex of EXCHANGES) {
      const quote = this.get(ex.name);
      const mid = quote?.mid() ?? null;
      const baselineMid = baseline?.[ex.name] ?? null;
      const change = mid !== null && baselineMid !== null ? mid - baselineMid : null;
      const delay = quote?.delayMs ?? null;

      exchanges[ex.name] = {
        price: roundedOrNull(mid, 4),
        delay: roundedOrNull(delay, 2),
        change: roundedOrNull(change, 4),
        baseline: roundedOrNull(baselineMid, 4),
      };
    }
    return exchanges;
  }
}

class Window {
  private constructor(
    readonly startSeconds: number,
    readonly endSeconds: number,
  ) {}

  static fromTimestampMs(timestampMs: number): Window {
    const timestampSeconds = Math.floor(timestampMs / 1000);
    const startSeconds = timestampSeconds - (timestampSeconds % WINDOW_SECONDS);
    return new Window(startSeconds, startSeconds + WINDOW_SECONDS);
  }

  containsStartSecond(timestampMs: number): boolean {
    return Math.floor(timestampMs / 1000) === this.startSeconds;
  }
}

function formatPrice8(value: number | null): string {
  return value === null ? "NA" : value.toFixed(8);
}

function roundedOrNull(value: number | null, decimals: number): number | null {
  if (value === null || !Number.isFinite(value)) {
    return null;
  }
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}
