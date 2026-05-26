export type ExchangeName = "bybit" | "gemini" | "binance" | "bitstamp" | "coinbase";

export class Quote {
  constructor(
    public readonly bestBid: number,
    public readonly bestAsk: number,
    public readonly delayMs: number | null,
  ) {}

  static new(bestBid: number, bestAsk: number, delayMs: number): Quote {
    return new Quote(bestBid, bestAsk, finiteOrNull(delayMs));
  }

  static withDelay(
    timestampMs: number,
    bestBid: number,
    bestAsk: number,
    receivedAtMs: number,
  ): Quote {
    return new Quote(bestBid, bestAsk, finiteOrNull(receivedAtMs - timestampMs));
  }

  mid(): number {
    return (this.bestBid + this.bestAsk) / 2;
  }
}

export interface FeedUpdate {
  exchange: ExchangeName;
  quote: Quote;
}

export type FeedEvent =
  | { type: "quote"; quote: Quote }
  | { type: "info"; message: string }
  | { type: "error"; message: string }
  | { type: "ignore" };

export interface ExchangeFeed {
  readonly name: ExchangeName;
  readonly url: string;
  subscriptions(): string[];
  heartbeatIntervalMs?(): number | null;
  heartbeatMessage?(): string | null;
  handleText(raw: string, receivedAtMs: number): FeedEvent;
}

export function nowUnixMs(): number {
  return Date.now();
}

export function parseNumberText(text: unknown): number | null {
  if (typeof text !== "string") {
    return null;
  }
  const value = Number(text);
  return finiteOrNull(value);
}

export function finiteOrNull(value: number): number | null {
  return Number.isFinite(value) ? value : null;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function parseJsonRecord(raw: string): Record<string, unknown> | null {
  try {
    const parsed: unknown = JSON.parse(raw);
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

export function firstString(rows: unknown): string | null {
  if (!Array.isArray(rows) || rows.length === 0) {
    return null;
  }
  const first = rows[0];
  if (!Array.isArray(first) || typeof first[0] !== "string") {
    return null;
  }
  return first[0];
}
