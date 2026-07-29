import asyncio
import gc
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
from jupyter_client.jsonutil import json_clean, json_default
from jupyter_client.session import Session
from tornado.httpserver import HTTPRequest

from jupyter_server.serverapp import ServerApp
from jupyter_server.services.kernels.connection.channels import ZMQChannelsWebsocketConnection
from jupyter_server.services.kernels.websocket import KernelWebsocketHandler


async def test_websocket_connection(jp_serverapp: ServerApp) -> None:
    app = jp_serverapp
    kernel_id = await app.kernel_manager.start_kernel()  # type:ignore[has-type]
    kernel = app.kernel_manager.get_kernel(kernel_id)
    request = HTTPRequest("foo", "GET")
    request.connection = MagicMock()
    handler = KernelWebsocketHandler(app.web_app, request)
    handler.ws_connection = MagicMock()
    handler.ws_connection.is_closing = lambda: False
    conn = ZMQChannelsWebsocketConnection(parent=kernel, websocket_handler=handler)
    handler.connection = conn
    await conn.prepare()
    await conn.connect()
    await conn.nudge()
    session: Session = kernel.session
    msg = session.msg("data_pub", content={"a": "b"})
    data = json.dumps(
        json_clean(msg),
        default=json_default,
        ensure_ascii=False,
        allow_nan=False,
    )
    conn.handle_incoming_message(data)
    await conn.handle_outgoing_message("iopub", session.serialize(msg))
    assert (
        conn.websocket_handler.select_subprotocol(["v1.kernel.websocket.jupyter.org"])
        == "v1.kernel.websocket.jupyter.org"
    )
    conn.write_stderr("test", {})
    conn.on_kernel_restarted()
    conn.on_restart_failed()
    conn._on_error("shell", msg, session.serialize(msg))


def _make_connection(app, kernel, session_id=None, timeout=0.01):
    """Build a ZMQChannelsWebsocketConnection with a mocked handler."""
    request = HTTPRequest("foo", "GET")
    request.connection = MagicMock()
    handler = KernelWebsocketHandler(app.web_app, request)
    handler.ws_connection = MagicMock()
    handler.ws_connection.is_closing = lambda: False
    conn = ZMQChannelsWebsocketConnection(parent=kernel, websocket_handler=handler)
    handler.connection = conn
    if session_id:
        conn.session.session = session_id
    conn.kernel_info_timeout = timeout
    return conn


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
async def test_nudge_cleanup_closes_transient_channels_when_iopub_closed(
    jp_serverapp: ServerApp,
) -> None:
    """If iopub is closed before nudge's cleanup callback fires (websocket
    teardown race), cleanup must still close the transient shell/control
    sockets it owns. Previously iopub.stop_on_recv() raised OSError inside
    the done-callback and aborted cleanup, leaving shell+control open."""
    app = jp_serverapp
    km = app.kernel_manager
    kernel_id = await km.start_kernel()
    kernel = km.get_kernel(kernel_id)
    await asyncio.sleep(1)

    conn = _make_connection(app, kernel)
    await conn.prepare()
    conn.create_stream()
    conn.kernel_info_timeout = 0.1

    # Track the transient sockets nudge() opens from this point forward.
    created: list = []

    def tracking(orig):
        def connect(*args, **kwargs):
            socket = orig(*args, **kwargs)
            created.append(socket)
            return socket

        return connect

    with (
        patch.object(kernel, "connect_shell", tracking(kernel.connect_shell)),
        patch.object(kernel, "connect_control", tracking(kernel.connect_control)),
    ):
        # Close the shared iopub channel out from under nudge, so its wait for
        # an iopub message can never be satisfied. The transient shell/control
        # sockets it owns must still be cleaned up.
        task = asyncio.ensure_future(conn.nudge())
        await asyncio.sleep(0)
        conn.channels["iopub"].close()
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except Exception:
            pass
        await asyncio.sleep(0.2)

    still_open = [s for s in created if not s.closed]
    assert not still_open, (
        f"nudge leaked {len(still_open)} transient socket(s) when iopub "
        f"was closed before cleanup ran"
    )


@pytest.mark.skipif(sys.platform != "linux", reason="Requires /proc/self/fd")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
async def test_no_fd_leak_on_buffer_restore_with_port_change(jp_serverapp: ServerApp) -> None:
    """Verify that stale buffered channels are closed when ports change on reconnect."""
    app = jp_serverapp
    km = app.kernel_manager
    kernel_id = await km.start_kernel()
    kernel = km.get_kernel(kernel_id)
    await asyncio.sleep(1)

    session_id = "fixed-session-for-test"

    # Warm up
    conn = _make_connection(app, kernel, session_id=session_id)
    conn.create_stream()
    try:
        await conn.nudge()
    except Exception:
        pass
    for s in conn.channels.values():
        if not s.closed:
            s.close()
    gc.collect()
    await asyncio.sleep(1)

    baseline_fds = len(os.listdir(f"/proc/{os.getpid()}/fd"))

    for _ in range(100):
        conn1 = _make_connection(app, kernel, session_id=session_id)
        conn1.create_stream()
        try:
            await conn1.nudge()
        except Exception:
            pass
        km._kernel_connections.setdefault(kernel_id, 0)
        km._kernel_connections[kernel_id] = 0
        km.start_buffering(kernel_id, conn1.session_key, conn1.channels)

        conn2 = _make_connection(app, kernel, session_id=session_id)
        km._kernel_connections[kernel_id] = 1
        with patch.object(km, "ports_changed", return_value=True):
            try:
                await conn2.connect()
            except Exception:
                pass
        for s in conn2.channels.values():
            if not s.closed:
                s.close()
        conn2.channels = {}

    gc.collect()
    await asyncio.sleep(2)
    gc.collect()
    final_fds = len(os.listdir(f"/proc/{os.getpid()}/fd"))
    assert final_fds - baseline_fds <= 5, (
        f"FD leak detected: {final_fds - baseline_fds} FDs leaked "
        f"after 100 buffer-restore-with-port-change cycles"
    )


@pytest.mark.skipif(sys.platform != "linux", reason="Requires /proc/self/fd")
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
async def test_no_fd_leak_on_disconnect_with_orphaned_kernel_info_channel(
    jp_serverapp: ServerApp,
) -> None:
    """When a kernel does not reply to kernel_info_request (e.g. rogue/hung),
    kernel_info_channel is left open after nudge. It must be closed on
    disconnect, including the single-tab last-connection path that triggers
    start_buffering."""
    app = jp_serverapp
    km = app.kernel_manager
    kernel_id = await km.start_kernel()
    kernel = km.get_kernel(kernel_id)
    await asyncio.sleep(1)

    # Warm up
    conn = _make_connection(app, kernel)
    conn.create_stream()
    conn.kernel_info_channel = km.connect_shell(kernel_id)
    ZMQChannelsWebsocketConnection._open_sockets.add(conn)
    km._kernel_connections[kernel_id] = 1
    conn.disconnect()
    gc.collect()
    await asyncio.sleep(1)

    baseline_fds = len(os.listdir(f"/proc/{os.getpid()}/fd"))

    for _ in range(100):
        conn = _make_connection(app, kernel)
        conn.create_stream()
        # Simulate rogue kernel: kernel_info_channel opened but reply never arrives
        conn.kernel_info_channel = km.connect_shell(kernel_id)
        ZMQChannelsWebsocketConnection._open_sockets.add(conn)
        # Natural single-tab flow: last connection disconnects -> start_buffering
        km._kernel_connections[kernel_id] = 1
        conn.disconnect()

    gc.collect()
    await asyncio.sleep(2)
    gc.collect()
    final_fds = len(os.listdir(f"/proc/{os.getpid()}/fd"))
    assert final_fds - baseline_fds <= 5, (
        f"FD leak detected: {final_fds - baseline_fds} FDs leaked after 100 "
        f"disconnects with orphaned kernel_info_channel"
    )


@pytest.mark.skipif(sys.platform == "win32", reason="Times out on Windows")
async def test_disconnect_resolves_orphaned_kernel_info_future(jp_serverapp: ServerApp) -> None:
    """Disconnecting with an orphaned kernel_info channel should not leave
    kernel_manager._kernel_info_future pending, which would block reconnects
    waiting for a never-arriving kernel_info reply."""
    app = jp_serverapp
    km = app.kernel_manager
    kernel_id = await km.start_kernel()
    kernel = km.get_kernel(kernel_id)

    conn1 = _make_connection(app, kernel, timeout=5.0)
    conn1.create_stream()
    conn1.session.key = kernel.session.key
    conn1.session_key = f"{kernel_id}:{conn1.session.session}"

    # Simulate an in-flight kernel_info request that never gets a reply.
    conn1.kernel_info_channel = km.connect_shell(kernel_id)
    km._kernel_info_future = conn1._kernel_info_future

    # Force the buffering early-return path in disconnect().
    # disconnect() first decrements the connection count via
    # notify_disconnect(), so start at 1 to reach the == 0 branch.
    km._kernel_connections[kernel_id] = 1
    ZMQChannelsWebsocketConnection._open_sockets.add(conn1)
    conn1.disconnect()

    # Regression check: pending shared future must be resolved by disconnect.
    assert conn1._kernel_info_future.done()

    # A new connection should not block waiting on the stale pending future.
    conn2 = _make_connection(app, kernel, timeout=0.2)
    conn2.create_stream()
    conn2.session.key = kernel.session.key
    conn2.kernel_info_timeout = 0.2
    await asyncio.wait_for(asyncio.wrap_future(conn2.request_kernel_info()), timeout=1.0)


async def test_iopub_is_never_blocked_by_a_slow_websocket(jp_serverapp: ServerApp) -> None:
    """iopub must keep draining even when the browser is behind.

    iopub is an XPUB/SUB channel, and libzmq silently discards messages once a
    PUB socket reaches its high-water mark. Declining to read iopub in order to
    apply backpressure would therefore lose kernel output with no error. The
    request/reply channels have no such hazard and do wait.
    """
    app = jp_serverapp
    km = app.kernel_manager
    kernel_id = await km.start_kernel()
    kernel = km.get_kernel(kernel_id)
    conn = _make_connection(app, kernel)

    # A writer that never drains, and a full queue, so any awaiting put blocks.
    conn._outgoing = asyncio.Queue(maxsize=2)
    conn._writer_task = asyncio.ensure_future(asyncio.sleep(3600))
    while not conn._outgoing.full():
        conn._outgoing.put_nowait((b"filler", True))

    try:
        # iopub returns immediately despite the full queue...
        await asyncio.wait_for(conn._enqueue("iopub", b"iopub-msg", True), timeout=1.0)

        # ...and the message is not dropped, just waiting its turn.
        assert conn._pending_puts

        # A request/reply channel does wait, which is where backpressure is safe.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(conn._enqueue("shell", b"shell-msg", True), timeout=0.25)
    finally:
        conn._stop_pumps()
        await km.shutdown_kernel(kernel_id, now=True)
