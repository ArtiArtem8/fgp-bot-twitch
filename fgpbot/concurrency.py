"""Keep blocking work owned until its thread finishes, including during cancellation."""

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


async def run_blocking[T, **P](
    function: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs
) -> T:
    """Finish a blocking operation before propagating cancellation to its caller."""
    work = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    cancellation: asyncio.CancelledError | None = None
    while not work.done():
        try:
            await asyncio.wait({work})
        except asyncio.CancelledError as exc:
            cancellation = exc
    if cancellation is not None:
        work.exception()
        raise cancellation
    return work.result()
