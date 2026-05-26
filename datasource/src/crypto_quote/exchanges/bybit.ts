import type { ExchangeFeed, FeedEvent } from "../types.js";
import { firstString, parseJsonRecord, parseNumberText, Quote } from "../types.js";

export class Bybit implements ExchangeFeed {
  readonly name = "bybit";
  readonly url = "wss://stream.bybit.com/v5/public/spot";

  constructor(private readonly topic = "orderbook.1.BTCUSDT") {}

  subscriptions(): string[] {
    return [
      JSON.stringify({
        op: "subscribe",
        args: [this.topic],
      }),
    ];
  }

  heartbeatIntervalMs(): number {
    return 20_000;
  }

  heartbeatMessage(): string {
    return JSON.stringify({ op: "ping" });
  }

  handleText(raw: string, receivedAtMs: number): FeedEvent {
    const msg = parseJsonRecord(raw);
    if (msg === null) {
      return { type: "error", message: "bybit failed to parse message" };
    }

    if (msg.topic === this.topic) {
      const quote = parseOrderbookTop(msg, receivedAtMs);
      return quote === null ? { type: "ignore" } : { type: "quote", quote };
    }

    if (msg.op === "subscribe" && msg.success === false) {
      const retMsg = typeof msg.ret_msg === "string" ? msg.ret_msg : "unknown error";
      return { type: "error", message: `bybit subscribe error: ${retMsg}` };
    }

    if (msg.op === "pong" || msg.ret_msg === "pong") {
      return { type: "ignore" };
    }

    return { type: "ignore" };
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
