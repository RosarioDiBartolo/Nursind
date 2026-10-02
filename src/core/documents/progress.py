"""Progress reporting while the extraction coordinator waits for workers."""

from concurrent.futures import FIRST_COMPLETED, Future, wait
from collections.abc import Iterable, Iterator
import logging
import time

logger = logging.getLogger(__name__)


def completed_with_progress(
    futures: Iterable[Future],
    *,
    stage: str,
    interval: float = 15.0,
) -> Iterator[Future]:
    pending = set(futures)
    started = time.monotonic()
    while pending:
        done, pending = wait(pending, timeout=interval, return_when=FIRST_COMPLETED)
        if not done:
            logger.info(
                "Waiting for %s: %s tasks outstanding (running or queued), %.0fs elapsed; "
                "no task completed in the last %.0fs",
                stage,
                len(pending),
                time.monotonic() - started,
                interval,
            )
        yield from done
