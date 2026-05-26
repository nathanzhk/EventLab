import type { ExchangeFeed } from "../types.js";
import { parseJsonRecord, parseNumberText, Quote } from "../types.js";

export class Gemini implements ExchangeFeed {
  readonly name = "gemini";
  readonly url = "wss://ws.gemini.com";

  constructor(
    private readonly symbol = "btcusd",
    private readonly stream = "btcusd@bookTicker",
  ) {}

  subscriptions(): Record<string, unknown> {
    return {
      id: "1",
      method: "subscribe",
      params: [this.stream],
    };
  }

  handleText(raw: string, receivedAtMs: number): Quote | null {
    const msg = parseJsonRecord(raw);
    if (msg === null) {
      return null;
    }

    if (msg.s === this.symbol && msg.b !== undefined && msg.a !== undefined) {
      return parseBookTicker(msg, receivedAtMs);
    }

    return null;
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
