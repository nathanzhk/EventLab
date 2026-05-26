import type { ExchangeFeed, FeedEvent } from "../types.js";
import { finiteOrNull, parseJsonRecord, parseNumberText, Quote } from "../types.js";

export class Binance implements ExchangeFeed {
  readonly name = "binance";
  readonly url = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker";

  subscriptions(): string[] {
    return [];
  }

  handleText(raw: string): FeedEvent {
    const msg = parseJsonRecord(raw);
    if (msg === null) {
      return { type: "error", message: "binance failed to parse message" };
    }

    const bestBid = parseNumberText(msg.b);
    const bestAsk = parseNumberText(msg.a);
    if (
      bestBid !== null &&
      bestAsk !== null &&
      finiteOrNull(bestBid) !== null &&
      finiteOrNull(bestAsk) !== null
    ) {
      return { type: "quote", quote: Quote.new(bestBid, bestAsk, 0) };
    }

    return { type: "ignore" };
  }
}
