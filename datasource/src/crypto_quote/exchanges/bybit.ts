import type { ExchangeFeed } from "../types.js";
import { Quote } from "../types.js";

export class Bybit implements ExchangeFeed {
  readonly name = "bybit";
  readonly url = "wss://stream.bybit.com/v5/public/spot";

  subscriptions(): Record<string, unknown> {
    return {
      op: "subscribe",
      args: ["orderbook.1.BTCUSDT"],
    };
  }

  heartbeatIntervalMs(): number {
    return 20_000;
  }

  heartbeatMessage(): string {
    return JSON.stringify({ op: "ping" });
  }

  parseMessage(message: Record<string, unknown>, recvTsMs: number): Quote | null {
    if (message.data === undefined) {
      return null;
    }

    const data = message.data as Record<string, unknown>;
    const bids = data.b as string[][];
    const asks = data.a as string[][];
    const tsMs = Number(message.ts);
    const bestBid = Number(bids[0]?.[0]);
    const bestAsk = Number(asks[0]?.[0]);

    if (!Number.isFinite(tsMs) || !Number.isFinite(bestBid) || !Number.isFinite(bestAsk)) {
      return null;
    }
    return Quote.new(bestBid, bestAsk, tsMs, recvTsMs);
  }
}
