from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import requests

from bus import EventBus, OverflowPolicy, Subscription
from runtime.events import RuntimeStateEvent
from utils.logger import get_logger

from .server import serialize_event

if TYPE_CHECKING:
    from app import ComponentFactory

logger = get_logger("DASHBOARD")

_DEFAULT_PORT = 8177


class DashboardComponent:
    def __init__(self, *, bus: EventBus, port: int = _DEFAULT_PORT) -> None:
        self._bus = bus
        self._port = port

    def start(self, tasks: asyncio.TaskGroup) -> None:
        state_events = self._bus.subscribe(
            RuntimeStateEvent,
            name="dashboard.runtime-state",
            maxsize=200,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        tasks.create_task(self._run(state_events))

    async def _run(self, events: Subscription[RuntimeStateEvent]) -> None:
        logger.info("forwarding dashboard state to persistent dashboard on port %d", self._port)
        is_available: bool | None = None
        async for event in events:
            payload = serialize_event(event)
            forwarded = await self._forward_payload(payload)
            if forwarded and is_available is not True:
                logger.info("dashboard backend available on port %d", self._port)
            if not forwarded and is_available is not False:
                logger.warning(
                    "dashboard backend unavailable on port %d; dropping dashboard state",
                    self._port,
                )
            is_available = forwarded

    async def _forward_payload(self, payload: bytes) -> bool:
        try:
            await asyncio.to_thread(self._post_payload, payload)
            return True
        except requests.RequestException:
            return False

    def _post_payload(self, payload: bytes) -> None:
        response = requests.post(
            f"http://127.0.0.1:{self._port}/state",
            data=payload,
            headers={"content-type": "application/json"},
            timeout=1,
        )
        response.raise_for_status()


def dashboard_component() -> ComponentFactory:
    return lambda context: DashboardComponent(bus=context.bus)
