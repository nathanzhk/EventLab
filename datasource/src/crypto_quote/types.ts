export type ExchangeName = "bybit" | "gemini" | "binance" | "bitstamp" | "coinbase";

export class Quote {
  constructor(
    public readonly bid: number,
    public readonly ask: number,
    public readonly exchTsMs: number,
    public readonly recvTsMs: number,
  ) {}

  static new(bid: number, ask: number, exchTsMs: number, recvTsMs: number): Quote {
    return new Quote(bid, ask, exchTsMs, recvTsMs);
  }

  mid(): number {
    return (this.bid + this.ask) / 2;
  }

  delayMs(): number | null {
    return finiteOrNull(this.recvTsMs - this.exchTsMs);
  }
}

export interface FeedUpdate {
  exchange: ExchangeName;
  quote: Quote;
}

export interface ExchangeFeed {
  readonly name: ExchangeName;
  readonly url: string;
  subscriptions(): Record<string, unknown>;
  heartbeatIntervalMs?(): number | null;
  heartbeatMessage?(): string | null;
  parseMessage(message: Record<string, unknown>, recvTsMs: number): Quote | null;
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
