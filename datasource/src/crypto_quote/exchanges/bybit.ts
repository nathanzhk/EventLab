import type { ExchangeFeed } from "../types.js";
import { firstString, parseJsonRecord, parseNumberText, Quote } from "../types.js";

export class Bybit implements ExchangeFeed {
  readonly name = "bybit";
  readonly url = "wss://stream.bybit.com/v5/public/spot";

  constructor(private readonly topic = "orderbook.1.BTCUSDT") {}

  subscriptions(): Record<string, unknown> {
    return {
      op: "subscribe",
      args: [this.topic],
    };
  }

  heartbeatIntervalMs(): number {
    return 20_000;
  }

  heartbeatMessage(): string {
    return JSON.stringify({ op: "ping" });
  }

  handleText(raw: string, receivedAtMs: number): Quote | null {
    const msg = parseJsonRecord(raw);
    if (msg === null) {
      return null;
    }

    if (msg.topic === this.topic) {
      return parseOrderbookTop(msg, receivedAtMs);
    }

    return null;
  }
}

function parseOrderbookTop(msg: Record<string, unknown>, receivedAtMs: number): Quote | null {
  const timestampMs = typeof msg.ts === "number" ? msg.ts : null;
  const data = msg.data;
  if (timestampMs === null || typeof data !== "object" || data === null || Array.isArray(data)) {
    return null;
  }

  const bestBid = parseNumberText(firstString((data as Record<string, unknown>).b));
  const bestAsk = parseNumberText(firstString((data as Record<string, unknown>).a));
  if (bestBid === null || bestAsk === null) {
    return null;
  }

  return Quote.withDelay(timestampMs, bestBid, bestAsk, receivedAtMs);
}
