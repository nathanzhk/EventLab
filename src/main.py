import argparse
import asyncio
import contextlib
import logging
import sys
from pathlib import Path

from app import ExecutionMode, Runtime
from dashboard.server import serve as serve_dashboard
from models import BTC5mMarket
from polymarket import get_crypto_price_result
from prediction.strategy import DefaultStrategy
from utils.env import Env
from utils.logger import configure_logging, get_logger, set_log_file
from utils.time import sleep_until

logger = get_logger("MAIN")

_MARKET_PREWARM_S = 30
_SETTLE_TIMEOUT_S = 30


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--market-start-ts",
        type=int,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--execution-mode",
        choices=("live", "mock"),
        default="mock",
        help="live uses real Polymarket clients; mock uses the local simulator",
    )
    parser.add_argument(
        "--strategy",
        choices=("none",),
        default="none",
        help="strategy to run; none only observes market data",
    )
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="start the dashboard server; disabled by default",
    )
    return parser.parse_args()


async def run(args: argparse.Namespace) -> None:
    Env.load()
    configure_logging()

    bus_logger = get_logger("BUS")
    bus_logger.setLevel(logging.INFO)
    state_logger = get_logger("STATE")
    state_logger.setLevel(logging.INFO)

    if args.market_start_ts is not None:
        await run_worker(
            args.market_start_ts,
            strategy_name=args.strategy,
            execution_mode=args.execution_mode,
            dashboard_enabled=args.dashboard,
        )
    else:
        async with asyncio.TaskGroup() as tasks:
            if args.dashboard:
                tasks.create_task(
                    serve_dashboard(),
                    name="dashboard",
                )
            tasks.create_task(
                run_supervisor(
                    strategy_name=args.strategy,
                    execution_mode=args.execution_mode,
                    dashboard_enabled=args.dashboard,
                ),
                name="supervisor",
            )


async def run_worker(
    market_start_ts: int,
    *,
    strategy_name: str,
    execution_mode: ExecutionMode,
    dashboard_enabled: bool,
) -> None:
    market = BTC5mMarket.from_start_ts(market_start_ts)
    set_log_file(market.slug)
    logger.info("start worker for %s", market.slug)
    logger.info("%s", market.title)
    logger.info("execution_mode=%s strategy=%s", execution_mode, strategy_name)

    runtime = Runtime(
        market=market,
        symbol="BTCUSDT",
        strategy=DefaultStrategy(),
        execution_mode=execution_mode,
        dashboard_enabled=dashboard_enabled,
    )
    runtime_task = asyncio.create_task(
        runtime.run(),
        name=f"runtime-{market.slug}",
    )
    settlement_task = asyncio.create_task(
        _settle_market(runtime, market),
        name=f"settle-{market.slug}",
    )

    await asyncio.wait(
        {runtime_task, settlement_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    if runtime_task.done():
        settlement_task.cancel()
        await asyncio.gather(settlement_task, return_exceptions=True)
        await runtime_task
    else:
        logger.info("stop worker for %s", market.slug)
        runtime_task.cancel()
        await asyncio.gather(runtime_task, return_exceptions=True)


async def _settle_market(runtime: Runtime, market: BTC5mMarket) -> None:
    await sleep_until(market.end_ts_s)
    for attempt in range(_SETTLE_TIMEOUT_S):
        result = await asyncio.to_thread(
            get_crypto_price_result,
            symbol="BTC",
            variant="fiveminute",
            event_start_ts_s=market.start_ts_s,
        )
        if result is not None:
            logger.info(
                "market settled %s open=%.2f close=%.2f outcome=%s",
                market.slug,
                result["open_price"],
                result["close_price"],
                result["outcome"],
            )
            await runtime.settle_market(result["outcome"])
            return
        if attempt + 1 < _SETTLE_TIMEOUT_S:
            await asyncio.sleep(1)
    logger.warning("market settlement unavailable before worker shutdown: %s", market.slug)


async def run_supervisor(
    *,
    strategy_name: str,
    execution_mode: ExecutionMode,
    dashboard_enabled: bool,
) -> None:
    set_log_file("supervisor")
    logger.info("start supervisor")
    logger.info("execution_mode=%s, strategy=%s", execution_mode, strategy_name)

    running: dict[int, asyncio.subprocess.Process] = {}
    watchers: set[asyncio.Task] = set()
    try:
        await _create_worker(
            running,
            watchers,
            BTC5mMarket.curr_market(),
            strategy_name=strategy_name,
            execution_mode=execution_mode,
            dashboard_enabled=dashboard_enabled,
        )
        while True:
            next_market = BTC5mMarket.next_market()
            await sleep_until(next_market.start_ts_s - _MARKET_PREWARM_S)
            await _create_worker(
                running,
                watchers,
                next_market,
                strategy_name=strategy_name,
                execution_mode=execution_mode,
                dashboard_enabled=dashboard_enabled,
            )
            await sleep_until(next_market.start_ts_s)
    finally:
        for watcher in watchers:
            watcher.cancel()
        await asyncio.gather(*watchers, return_exceptions=True)
        await _terminate_workers(running)


async def _create_worker(
    running: dict[int, asyncio.subprocess.Process],
    watchers: set[asyncio.Task],
    market: BTC5mMarket,
    *,
    strategy_name: str,
    execution_mode: ExecutionMode,
    dashboard_enabled: bool,
) -> None:
    proc = running.get(market.start_ts_s)
    if proc is not None and proc.returncode is None:
        return

    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--market-start-ts",
        str(market.start_ts_s),
        "--execution-mode",
        execution_mode,
        "--strategy",
        strategy_name,
    ]
    if dashboard_enabled:
        cmd.append("--dashboard")
    proc = await asyncio.create_subprocess_exec(*cmd, cwd=Path(__file__).resolve().parents[1])
    running[market.start_ts_s] = proc
    logger.info("started worker pid=%s market=%s", proc.pid, market.slug)

    watcher = asyncio.create_task(
        _watch_worker(running, market.start_ts_s, proc),
        name=f"watch-{market.slug}",
    )
    watchers.add(watcher)
    watcher.add_done_callback(watchers.discard)


async def _watch_worker(
    running: dict[int, asyncio.subprocess.Process],
    start_ts_s: int,
    proc: asyncio.subprocess.Process,
) -> None:
    returncode = await proc.wait()
    if returncode == 0:
        logger.info("worker exited start_ts=%s", start_ts_s)
    else:
        logger.error("worker exited start_ts=%s returncode=%s", start_ts_s, returncode)
    if running.get(start_ts_s) is proc:
        del running[start_ts_s]


async def _terminate_workers(running: dict[int, asyncio.subprocess.Process]) -> None:
    for proc in running.values():
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
    for proc in running.values():
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                await proc.wait()


def main() -> None:
    args = _parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        logger.info("shutdown")


if __name__ == "__main__":
    main()
