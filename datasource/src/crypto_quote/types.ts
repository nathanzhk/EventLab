export type ExchangeName = "bybit" | "gemini" | "binance" | "bitstamp" | "coinbase";

export class Quote {
  constructor(
    public readonly bid: number,
    public readonly ask: number,
    public readonly exchTsMs: number,
    public readonly recvTsMs: number,
  ) {}

  mid(): number {
    return (this.bid + this.ask) / 2;
  }

  delayMs(): number {
    return this.recvTsMs - this.exchTsMs;
  }
}

export interface FeedUpdate {
  exchange: ExchangeName;
  quote: Quote;
}

export interface ExchangeFeed {
  readonly name: ExchangeName;
  readonly url: string;
  subscribe(): Record<string, unknown>;
  heartbeatIntervalMs?(): number | null;
  heartbeatMessage?(): string | null;
  parseMessage(message: Record<string, unknown>, recvTsMs: number): Quote | null;
}
