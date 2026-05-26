import WebSocket from "ws";

import { error, info } from "../logger.js";
import type { ExchangeFeed, FeedUpdate } from "./types.js";
import { nowUnixMs, parseJsonRecord } from "./types.js";

export interface FeedConfig {
  reconnectMinDelayMs: number;
  reconnectMaxDelayMs: number;
}

export const defaultFeedConfig: FeedConfig = {
  reconnectMinDelayMs: 1_000,
  reconnectMaxDelayMs: 30_000,
};

export type UpdateHandler = (update: FeedUpdate) => void;

export async function runFeed(
  feed: ExchangeFeed,
  config: FeedConfig,
  onUpdate: UpdateHandler,
  signal: AbortSignal,
): Promise<void> {
  let reconnectDelayMs = config.reconnectMinDelayMs;

  while (!signal.aborted) {
    try {
      await runOnce(feed, onUpdate, signal);
      info(`${feed.name} websocket closed cleanly`);
      reconnectDelayMs = config.reconnectMinDelayMs;
    } catch (err) {
      if (signal.aborted) {
        break;
      }
      error(`${feed.name} websocket error: ${formatError(err)}`);
    }

    if (signal.aborted) {
      break;
    }

    info(`${feed.name} reconnecting in ${(reconnectDelayMs / 1000).toFixed(1)}s`);
    await sleep(reconnectDelayMs, signal);
    reconnectDelayMs = Math.min(reconnectDelayMs * 2, config.reconnectMaxDelayMs);
  }
}

function runOnce(
  feed: ExchangeFeed,
  onUpdate: UpdateHandler,
  signal: AbortSignal,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(feed.url);
    let settled = false;
    let heartbeat: NodeJS.Timeout | null = null;

    const cleanup = (): void => {
      if (heartbeat !== null) {
        clearInterval(heartbeat);
        heartbeat = null;
      }
      signal.removeEventListener("abort", abort);
      ws.removeAllListeners();
    };

    const finish = (err?: unknown): void => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    };

    const abort = (): void => {
      ws.close();
      finish();
    };

    signal.addEventListener("abort", abort, { once: true });

    ws.on("open", () => {
      info(`${feed.name} connected ${feed.url}`);
      const subscription = feed.subscriptions();
      if (Object.keys(subscription).length > 0) {
        ws.send(JSON.stringify(subscription), (err) => {
          if (err) {
            finish(new Error(`send ${feed.name} subscription: ${err.message}`));
          }
        });
      }

      const intervalMs = feed.heartbeatIntervalMs?.() ?? null;
      if (intervalMs !== null) {
        heartbeat = setInterval(() => {
          const message = feed.heartbeatMessage?.() ?? null;
          if (message !== null && ws.readyState === WebSocket.OPEN) {
            ws.send(message, (err) => {
              if (err) {
                finish(new Error(`send ${feed.name} heartbeat: ${err.message}`));
              }
            });
          }
        }, intervalMs);
      }
    });

    ws.on("message", (data) => {
      const raw = typeof data === "string" ? data : data.toString("utf8");
      const msg = parseJsonRecord(raw);
      if (msg === null) {
        return;
      }

      const quote = feed.parseMessage(msg, nowUnixMs());
      if (quote !== null) {
        onUpdate({ exchange: feed.name, quote });
      }
    });

    ws.on("close", () => finish());
    ws.on("error", (err) => finish(err));
  });
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(done, ms);
    const abort = (): void => done();
    function done(): void {
      clearTimeout(timer);
      signal.removeEventListener("abort", abort);
      resolve();
    }
    signal.addEventListener("abort", abort, { once: true });
  });
}

function formatError(err: unknown): string {
  return err instanceof Error ? err.stack ?? err.message : String(err);
}
