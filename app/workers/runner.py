from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable

from app.core.logging import configure_logging
from app.infra.settings import get_settings

logger = logging.getLogger(__name__)

Worker = Callable[[asyncio.Event], Awaitable[None]]


def _install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda _signum, _frame: stop.set())


async def _run(name: str, worker: Worker) -> None:
    stop = asyncio.Event()
    _install_signal_handlers(stop)
    logger.info("%s starting", name)
    try:
        await worker(stop)
    finally:
        logger.info("%s stopped", name)


def run_worker(name: str, worker: Worker) -> None:
    configure_logging(get_settings().app.log_level)
    try:
        asyncio.run(_run(name, worker))
    except KeyboardInterrupt:
        logger.info("%s interrupted", name)
