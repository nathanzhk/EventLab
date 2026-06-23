from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from bus import (
    EventBus,
    OverflowPolicy,
    Subscription,
)
from runtime.events import RuntimeStateEvent

from .engine import PredictionEngine

if TYPE_CHECKING:
    from app import ComponentFactory


class PredictionComponent:
    def __init__(self, *, bus: EventBus, engine: PredictionEngine) -> None:
        self._bus = bus
        self._engine = engine

    def start(self, tasks: asyncio.TaskGroup) -> None:
        runtime_state_events = self._bus.subscribe(
            RuntimeStateEvent,
            name="prediction.runtime-state",
            maxsize=1,
            overflow=OverflowPolicy.DROP_OLDEST,
        )
        tasks.create_task(self._prediction_loop(runtime_state_events))

    async def _prediction_loop(self, events: Subscription[RuntimeStateEvent]) -> None:
        async for runtime_state in events:
            try:
                desired_position_events = await self._engine.evaluate(runtime_state)
                for desired_position_event in desired_position_events:
                    await self._bus.publish(desired_position_event)
            except Exception:
                raise


def prediction_component() -> ComponentFactory:
    return lambda context: PredictionComponent(
        bus=context.bus,
        engine=context.prediction_engine,
    )
