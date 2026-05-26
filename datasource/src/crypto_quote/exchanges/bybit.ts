import type { ExchangeFeed } from "../types.js";
import { firstString, parseNumberText, Quote } from "../types.js";

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

  parseMessage(message: Record<string, unknown>, recvTsMs: number): Quote | null {
    if (message.topic === this.topic) {
      return parseOrderbookTop(message, recvTsMs);
    }

    return null;
  }
}

function parseOrderbookTop(msg: Record<string, unknown>, recvTsMs: number): Quote | null {
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

  return Quote.new(bestBid, bestAsk, timestampMs, recvTsMs);
}
