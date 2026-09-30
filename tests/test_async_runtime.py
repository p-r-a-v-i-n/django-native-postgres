import asyncio

import pytest
from django_native_postgres import _native


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
