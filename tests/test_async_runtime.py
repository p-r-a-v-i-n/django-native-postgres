import asyncio
import multiprocessing

import pytest
from django_native_postgres import _native


async def _run_runtime_probe():
    result = await asyncio.wait_for(
        _native.runtime_probe(1),
        timeout=1,
    )
    assert result == 1


def _run_runtime_probe_in_child():
    asyncio.run(_run_runtime_probe())


@pytest.mark.asyncio
async def test_runtime_probe_round_trip():
    delay_ms = 1

    result = await _native.runtime_probe(delay_ms)

    assert result == delay_ms


@pytest.mark.asyncio
async def test_runtime_probe_yields_to_python_event_loop():
    delay_ms = 50

    probe = asyncio.create_task(_native.runtime_probe(delay_ms))
    await asyncio.sleep(0)

    assert not probe.done()
    assert await probe == delay_ms


@pytest.mark.asyncio
async def test_runtime_probes_run_concurrently():
    delay_ms = 100
    probe_count = 5
    event_loop = asyncio.get_running_loop()
    started_at = event_loop.time()

    results = await asyncio.gather(
        *(_native.runtime_probe(delay_ms) for _ in range(probe_count)),
    )

    elapsed = event_loop.time() - started_at
    sequential_duration = delay_ms * probe_count / 1_000
    assert results == [delay_ms] * probe_count
    assert elapsed < sequential_duration * 0.75


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(),
    reason="fork is not supported",
)
@pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded.*:DeprecationWarning",
)
def test_runtime_is_recreated_after_fork():
    asyncio.run(_run_runtime_probe())

    context = multiprocessing.get_context("fork")
    process = context.Process(target=_run_runtime_probe_in_child)
    process.start()
    process.join(timeout=2)

    if process.is_alive():
        process.kill()
        process.join()

    assert process.exitcode == 0
