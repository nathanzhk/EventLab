import type { ExchangeFeed } from "../types.js";
import { parseNumberText, Quote } from "../types.js";

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

  parseMessage(message: Record<string, unknown>, recvTsMs: number): Quote | null {
    if (message.s === this.symbol && message.b !== undefined && message.a !== undefined) {
      return parseBookTicker(message, recvTsMs);
    }

    return null;
  }
}

function parseBookTicker(msg: Record<string, unknown>, recvTsMs: number): Quote | null {
  const eventTime = typeof msg.E === "number" ? msg.E : null;
  const bestBid = parseNumberText(msg.b);
  const bestAsk = parseNumberText(msg.a);

  if (eventTime === null || bestBid === null || bestAsk === null) {
    return null;
  }

  return Quote.new(bestBid, bestAsk, eventTime / 1_000_000, recvTsMs);
}
