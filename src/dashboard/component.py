from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import orjson
import requests

from bus import EventBus, OverflowPolicy, Subscription
from runtime.events import RuntimeStateEvent
from utils.logger import get_logger

from .server import DASHBOARD_PORT, serialize_state

if TYPE_CHECKING:
    from app import ComponentFactory

logger = get_logger("DASHBOARD")


class DashboardComponent:
    def __init__(self, *, bus: EventBus) -> None:
        self._bus = bus

    def start(self, tasks: asyncio.TaskGroup) -> None:
        state_events = self._bus.subscribe(
            RuntimeStateEvent,
            name="dashboard.runtime-state",
            maxsize=200,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        tasks.create_task(self._run(state_events))

    async def _run(self, events: Subscription[RuntimeStateEvent]) -> None:
        async for state in events:
            payload = orjson.dumps(serialize_state(state))
            await asyncio.to_thread(self._forward_state, payload)

    def _forward_state(self, payload: bytes) -> None:
        try:
            response = requests.post(
                f"http://127.0.0.1:{DASHBOARD_PORT}/state",
                data=payload,
                headers={"content-type": "application/json"},
                timeout=1,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.debug("drop state forward to dashboard: %s", exc)


def dashboard_component() -> ComponentFactory:
    return lambda context: DashboardComponent(bus=context.bus)
