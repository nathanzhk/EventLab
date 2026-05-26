import type { ExchangeFeed } from "../types.js";
import { finiteOrNull, parseJsonRecord, parseNumberText, Quote } from "../types.js";

export class Binance implements ExchangeFeed {
  readonly name = "binance";
  readonly url = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker";

  subscriptions(): Record<string, unknown> {
    return {};
  }

  handleText(raw: string): Quote | null {
    const msg = parseJsonRecord(raw);
    if (msg === null) {
      return null;
    }

    const bestBid = parseNumberText(msg.b);
    const bestAsk = parseNumberText(msg.a);
    if (
      bestBid !== null &&
      bestAsk !== null &&
      finiteOrNull(bestBid) !== null &&
      finiteOrNull(bestAsk) !== null
    ) {
      return Quote.new(bestBid, bestAsk, 0);
    }

    return null;
  }
}
