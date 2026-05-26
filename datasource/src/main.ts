import { Aggregator } from "./crypto_quote/aggregate.js";
import { defaultFeedConfig, runFeed } from "./crypto_quote/exchanges.js";
import { Binance } from "./crypto_quote/exchanges/binance.js";
import { Bitstamp } from "./crypto_quote/exchanges/bitstamp.js";
import { Bybit } from "./crypto_quote/exchanges/bybit.js";
import { Coinbase } from "./crypto_quote/exchanges/coinbase.js";
import { Gemini } from "./crypto_quote/exchanges/gemini.js";
import type { ExchangeFeed } from "./crypto_quote/types.js";
import { DashboardServer, NoopDashboard } from "./dashboard/server.js";
import { closeLogger, error, info } from "./logger.js";

const DEFAULT_LOG_PATH = "logs/aggregate.log";
const DASHBOARD_HOST = "127.0.0.1";
const DASHBOARD_PORT = 8080;

async function main(): Promise<void> {
  const enableDashboard = process.argv.includes("--dashboard");
  const controller = new AbortController();

  const dashboard = enableDashboard
    ? new DashboardServer(DASHBOARD_HOST, DASHBOARD_PORT)
    : new NoopDashboard();

  if (dashboard instanceof DashboardServer) {
    await dashboard.listen();
  }

  const aggregator = new Aggregator({ logPath: DEFAULT_LOG_PATH }, dashboard);
  const feeds: ExchangeFeed[] = [
    new Bybit(),
    new Gemini(),
    new Binance(),
    new Bitstamp(),
    new Coinbase(),
  ];

  const feedTasks = feeds.map((feed) =>
    runFeed(feed, defaultFeedConfig, (update) => aggregator.onUpdate(update), controller.signal),
  );

  await waitForShutdown();
  info("received shutdown signal, shutting down");
  controller.abort();
  await Promise.allSettled(feedTasks);
  await aggregator.close();
  if (dashboard instanceof DashboardServer) {
    await dashboard.close();
  }
  await closeLogger();
}

function waitForShutdown(): Promise<void> {
  return new Promise((resolve) => {
    process.once("SIGINT", resolve);
    process.once("SIGTERM", resolve);
  });
}

main().catch(async (err: unknown) => {
  error(err instanceof Error ? err.stack ?? err.message : String(err));
  await closeLogger();
  process.exitCode = 1;
});
