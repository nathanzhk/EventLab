import type { ExchangeFeed, FeedEvent } from "../types.js";
import { parseJsonRecord, parseNumberText, Quote } from "../types.js";

export class Gemini implements ExchangeFeed {
  readonly name = "gemini";
  readonly url = "wss://ws.gemini.com";

  constructor(
    private readonly symbol = "btcusd",
    private readonly stream = "btcusd@bookTicker",
  ) {}

  subscriptions(): string[] {
    return [
      JSON.stringify({
        id: "1",
        method: "subscribe",
        params: [this.stream],
      }),
    ];
  }

  handleText(raw: string, receivedAtMs: number): FeedEvent {
    const msg = parseJsonRecord(raw);
    if (msg === null) {
      return { type: "error", message: "gemini failed to parse message" };
    }

    if (msg.s === this.symbol && msg.b !== undefined && msg.a !== undefined) {
      const quote = parseBookTicker(msg, receivedAtMs);
      return quote === null ? { type: "ignore" } : { type: "quote", quote };
    }

    if (msg.error !== undefined) {
      return { type: "error", message: `gemini error: ${JSON.stringify(msg.error)}` };
    }

    return { type: "ignore" };
  }
}

function parseBookTicker(msg: Record<string, unknown>, receivedAtMs: number): Quote | null {
  const eventTime = typeof msg.E === "number" ? msg.E : null;
  const bestBid = parseNumberText(msg.b);
  const bestAsk = parseNumberText(msg.a);

  if (eventTime === null || bestBid === null || bestAsk === null) {
    return null;
  }

  return Quote.withDelay(eventTime / 1_000_000, bestBid, bestAsk, receivedAtMs);
}
