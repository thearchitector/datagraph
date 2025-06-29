import anyio
import pytest
from celery import shared_task
from glide import GlideClientConfiguration, NodeAddress

from datagraph import IO, Flow, IOVal, LocalExecutor, Supervisor
from datagraph.executor.celery import CeleryExecutor

from .processors import bar, consumer, first, foo, foobar, producer, second


@pytest.fixture(
    params=[LocalExecutor, CeleryExecutor], ids=("local", "celery"), scope="module"
)
async def supervisor(request):
    Supervisor._instance = None
    config = GlideClientConfiguration(addresses=[NodeAddress()], request_timeout=10000)

    if request.param == LocalExecutor:
        yield Supervisor.attach(glide_config=config, executor=LocalExecutor())
    elif request.param == CeleryExecutor:
        from .processors import (
            _bar,
            _consumer,
            _first,
            _foo,
            _foobar,
            _producer,
            _second,
        )

        shared_task(name=foo.name, ignore_result=True)(_foo)
        shared_task(name=bar.name, ignore_result=True)(_bar)
        shared_task(name=foobar.name, ignore_result=True)(_foobar)
        shared_task(name=producer.name, ignore_result=True)(_producer)
        shared_task(name=consumer.name, ignore_result=True)(_consumer)
        shared_task(name=first.name, ignore_result=True)(_first)
        shared_task(name=second.name, ignore_result=True)(_second)

        celery_app = request.getfixturevalue("celery_session_app")
        # this is necessary to spin up the worker thread
        _ = request.getfixturevalue("celery_session_worker")

        yield Supervisor.attach(
            glide_config=config, executor=CeleryExecutor(celery_app)
        )

    Supervisor.instance().shutdown()


@pytest.mark.anyio
async def test_flow_simple(supervisor):
    flow = Flow.from_processors(foo).resolve()

    with anyio.fail_after(30):
        outputs: dict[str, IO] = await supervisor.start_flow(
            flow, inputs=[IOVal(name="a", value=5)]
        )

        assert {"b", "c"} == outputs.keys()

        b_vals = [r async for r in outputs["b"].stream()]
        b_val = await outputs["b"].first()
        c_val_first = await outputs["c"].first()
        c_val_last = [r async for r in outputs["c"].stream()][-1]
        # c_val_latest = await outputs["c"].latest()

        assert b_vals == [10]
        assert b_val == 10
        assert c_val_first == 0
        assert c_val_last == 4
        # assert c_val_latest == 4


@pytest.mark.anyio
async def test_flow_dual_output(supervisor):
    flow = Flow.from_processors(foo, bar).resolve()

    with anyio.fail_after(30):
        outputs: dict[str, IO] = await supervisor.start_flow(
            flow, inputs=[IOVal(name="a", value=5)]
        )

        assert "d" in outputs.keys()

        c_d_res = [rs async for rs in outputs["c"].stream_with(outputs["d"])]
        assert c_d_res == [
            (0, 10),
            (1, 11),
            (2, 12),
            (3, 13),
            (4, 14),
        ]


@pytest.mark.anyio
async def test_flow_complex(supervisor):
    flow = Flow.from_processors(foo, bar, foobar).resolve()

    with anyio.fail_after(30):
        outputs: dict[str, IO] = await supervisor.start_flow(
            flow, inputs=[IOVal(name="a", value=5)]
        )

        assert "e" in outputs.keys()

        e_vals = [r async for r in outputs["e"].stream()]
        assert e_vals == [
            5 + 10 + 0 + 10,
            5 + 10 + 1 + 11,
            5 + 10 + 2 + 12,
            5 + 10 + 3 + 13,
            5 + 10 + 4 + 14,
        ]


@pytest.mark.anyio
async def test_flow_interlaced(supervisor):
    flow = Flow.from_processors(producer, consumer).resolve()

    with anyio.fail_after(30):
        outputs = await supervisor.start_flow(
            flow, inputs=[IOVal(name="input", value=5)], all_outputs=True
        )

        produced = [v async for v in outputs["produced"].stream()]
        consumed = [v async for v in outputs["consumed"].stream()]

        p_values = [v[0] for v in produced]
        c_values = [v[0] for v in consumed]
        p_timestamps = [v[1] for v in produced]
        c_timestamps = [v[1] for v in consumed]

        # check that both streams produced the same values in the same order
        expected_values = list(range(5))
        assert p_values == expected_values
        assert c_values == expected_values

        # the test case here is that the consumer and producer can be interlaced, aka
        # C can yield a value for immediate processing by P before C yields another.
        #
        # we record the timestamps for yielding from both C and P for each entry. if
        # they're truly interlaced, zipping those lists should produce a strictly
        # increasing series of timestamps, i.e. we should see CPCPCP. if C processed
        # all values before P, we'd see CCCPPP, and the sorted list would not be
        # identical to the raw zipped one
        timestamps = list(zip(p_timestamps, c_timestamps, strict=False))
        assert timestamps == sorted(timestamps)


@pytest.mark.anyio
async def test_flow_sequential(supervisor):
    flow = Flow.from_processors(first, second).resolve()

    with anyio.fail_after(30):
        outputs = await supervisor.start_flow(flow)

        result = await outputs["b"].first()
        assert result == sum(range(10))
