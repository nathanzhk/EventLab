import argparse
import asyncio
import contextlib
import logging
import sys
from pathlib import Path

from app import ExecutionMode, Runtime
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
        default="live",
        help="live uses real Polymarket clients; mock uses the local simulator",
    )
    parser.add_argument(
        "--strategy",
        choices=("none",),
        default="none",
        help="strategy to run; none only observes market data",
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
            execution_mode=args.execution_mode,
            strategy_name=args.strategy,
        )
    else:
        await run_supervisor(
            execution_mode=args.execution_mode,
            strategy_name=args.strategy,
        )


async def run_worker(
    market_start_ts: int,
    *,
    execution_mode: ExecutionMode,
    strategy_name: str,
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
    execution_mode: ExecutionMode,
    strategy_name: str,
) -> None:
    set_log_file("supervisor")
    logger.info("start supervisor")
    logger.info("execution_mode=%s, strategy=%s", execution_mode, strategy_name)

    running: dict[int, asyncio.subprocess.Process] = {}
    try:
        await _create_worker(
            running,
            BTC5mMarket.curr_market(),
            execution_mode=execution_mode,
            strategy_name=strategy_name,
        )
        while True:
            next_market = BTC5mMarket.next_market()
            await sleep_until(next_market.start_ts_s - _MARKET_PREWARM_S)
            await _create_worker(
                running,
                next_market,
                execution_mode=execution_mode,
                strategy_name=strategy_name,
            )
            await sleep_until(next_market.start_ts_s)
            await _cleanup_workers(running)
    finally:
        await _terminate_workers(running)


async def _create_worker(
    running: dict[int, asyncio.subprocess.Process],
    market: BTC5mMarket,
    *,
    execution_mode: ExecutionMode,
    strategy_name: str,
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
    proc = await asyncio.create_subprocess_exec(*cmd, cwd=Path(__file__).resolve().parents[1])
    running[market.start_ts_s] = proc
    logger.info("started worker pid=%s market=%s", proc.pid, market.slug)


async def _cleanup_workers(running: dict[int, asyncio.subprocess.Process]) -> None:
    for start_ts_s, proc in list(running.items()):
        if proc.returncode is None:
            continue
        returncode = await proc.wait()
        if returncode == 0:
            logger.info("worker exited start_ts=%s", start_ts_s)
        else:
            logger.error("worker exited start_ts=%s returncode=%s", start_ts_s, returncode)
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
