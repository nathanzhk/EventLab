import type { ExchangeFeed } from "../types.js";
import { Quote } from "../types.js";

export class Bitstamp implements ExchangeFeed {
  readonly name = "bitstamp";
  readonly url = "wss://ws.bitstamp.net";

  subscribe(): Record<string, unknown> {
    return {
      event: "bts:subscribe",
      data: { channel: "order_book_btcusd" },
    };
  }

  parseMessage(message: Record<string, unknown>, recvTsMs: number): Quote | null {
    if (message.event !== "data") {
      return null;
    }
    const orderBook = message.data as Record<string, unknown>;

    const microTs = Number(orderBook.microtimestamp);
    const secondTs = Number(orderBook.timestamp);
    const tsMs = Number.isFinite(microTs) ? microTs / 1000 : secondTs * 1000;

    const bids = orderBook.bids as string[][];
    const asks = orderBook.asks as string[][];
    const bestBid = Number(bids[0]?.[0]);
    const bestAsk = Number(asks[0]?.[0]);

    if (!Number.isFinite(tsMs) || !Number.isFinite(bestBid) || !Number.isFinite(bestAsk)) {
      return null;
    }
    return new Quote(bestBid, bestAsk, tsMs, recvTsMs);
  }
}
