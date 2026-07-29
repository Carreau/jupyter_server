"""An implementation of a kernel connection."""

from __future__ import annotations

import asyncio
import inspect
import json
import time
import typing as t
import weakref
from concurrent.futures import Future
from textwrap import dedent

import zmq
import zmq.asyncio
from jupyter_client import protocol_version as client_protocol_version  # type:ignore[attr-defined]
from tornado import web
from tornado.ioloop import IOLoop
from tornado.websocket import WebSocketClosedError
from traitlets import Any, Bool, Dict, Float, Instance, Int, List, Unicode, default

try:
    from jupyter_client.jsonutil import json_default
except ImportError:
    from jupyter_client.jsonutil import date_default as json_default

from jupyter_core.utils import ensure_async

from jupyter_server.transutils import _i18n

from ..kernelmanager import as_awaitable_socket
from ..websocket import KernelWebsocketHandler
from .abc import KernelWebsocketConnectionABC
from .base import (
    BaseKernelWebsocketConnection,
    deserialize_binary_message,
    deserialize_msg_from_ws_v1,
    serialize_binary_message,
    serialize_msg_to_ws_v1,
)

#: How many outgoing websocket payloads may be in flight before the channel
#: pumps are made to wait. Small on purpose: the backlog belongs in zmq, where
#: the high-water mark bounds it, not in this process.
OUTGOING_QUEUE_SIZE = 100


def _ensure_future(f):
    """Wrap a concurrent future as an asyncio future if there is a running loop."""
    try:
        asyncio.get_running_loop()
        return asyncio.wrap_future(f)
    except RuntimeError:
        return f


class ZMQChannelsWebsocketConnection(BaseKernelWebsocketConnection):
    """A Jupyter Server Websocket Connection"""

    limit_rate = Bool(
        True,
        config=True,
        help=_i18n(
            "Whether to limit the rate of IOPub messages (default: True). "
            "If True, use iopub_msg_rate_limit, iopub_data_rate_limit and/or rate_limit_window "
            "to tune the rate."
        ),
    )

    iopub_msg_rate_limit = Float(
        1000,
        config=True,
        help=_i18n(
            """(msgs/sec)
        Maximum rate at which messages can be sent on iopub before they are
        limited."""
        ),
    )

    iopub_data_rate_limit = Float(
        1000000,
        config=True,
        help=_i18n(
            """(bytes/sec)
        Maximum rate at which stream output can be sent on iopub before they are
        limited."""
        ),
    )

    rate_limit_window = Float(
        3,
        config=True,
        help=_i18n(
            """(sec) Time window used to
        check the message and data rate limits."""
        ),
    )

    websocket_handler = Instance(KernelWebsocketHandler)

    @property
    def write_message(self):
        """Alias to the websocket handler's write_message method."""
        return self.websocket_handler.write_message

    # class-level registry of open sessions
    # allows checking for conflict on session-id,
    # which is used as a zmq identity and must be unique.
    _open_sessions: dict[str, KernelWebsocketHandler] = {}
    _open_sockets: t.MutableSet[ZMQChannelsWebsocketConnection] = weakref.WeakSet()

    _kernel_info_future: Future[t.Any]
    _close_future: Future[t.Any]

    channels = Dict({})
    kernel_info_channel = Any(allow_none=True)

    #: channel name -> task draining that channel into the websocket
    _pump_tasks = Dict({})
    _kernel_info_task = Any(allow_none=True)
    _outgoing = Any(allow_none=True)
    _writer_task = Any(allow_none=True)
    _pending_puts = Any()

    _kernel_info_future = Instance(klass=Future)  # type:ignore[assignment]

    @default("_kernel_info_future")
    def _default_kernel_info_future(self):
        """The default kernel info future."""
        return Future()

    _close_future = Instance(klass=Future)  # type:ignore[assignment]

    @default("_close_future")
    def _default_close_future(self):
        """The default close future."""
        return Future()

    session_key = Unicode("")

    _iopub_window_msg_count = Int()
    _iopub_window_byte_count = Int()
    _iopub_msgs_exceeded = Bool(False)
    _iopub_data_exceeded = Bool(False)
    # Queue of (time stamp, byte count)
    # Allows you to specify that the byte count should be lowered
    # by a delta amount at some point in the future.
    _iopub_window_byte_queue: List[t.Any] = List([])

    @classmethod
    async def close_all(cls):
        """Tornado does not provide a way to close open sockets, so add one."""
        for connection in list(cls._open_sockets):
            connection.disconnect()
            await _ensure_future(connection._close_future)

    @property
    def subprotocol(self):
        """The sub protocol."""
        try:
            protocol = self.websocket_handler.selected_subprotocol
        except Exception:
            protocol = None
        return protocol

    def create_stream(self):
        """Connect this session's zmq channels to the kernel."""
        identity = self.session.bsession
        for channel in ("iopub", "shell", "control", "stdin"):
            meth = getattr(self.kernel_manager, "connect_" + channel)
            self.channels[channel] = as_awaitable_socket(meth(identity=identity))

    # -- channel pumps ----------------------------------------------------
    #
    # Each channel is drained by its own task rather than by an on_recv
    # callback, and everything bound for the browser goes through one writer
    # task. That buys three things a callback could not: an error forwarding a
    # message has somewhere to surface instead of being swallowed by a callback
    # with no caller; teardown is task cancellation rather than bookkeeping;
    # and a single writer keeps channels ordered relative to each other and to
    # synthetic messages, without interleaved concurrent websocket writes.
    #
    # It does *not* mean every channel applies backpressure. Only the
    # request/reply channels wait on a slow browser; iopub must always be
    # drained promptly or libzmq will silently discard kernel output. See
    # _enqueue for the full reasoning.

    def _start_writer(self):
        """Start the single task that owns writing to the websocket."""
        if self._outgoing is None:
            self._outgoing = asyncio.Queue(maxsize=OUTGOING_QUEUE_SIZE)
        if self._writer_task is None:
            self._writer_task = asyncio.ensure_future(self._writer())

    def _start_pumps(self):
        """Start forwarding every channel to the websocket."""
        self._start_writer()
        for channel in self.channels:
            if channel not in self._pump_tasks:
                self._pump_tasks[channel] = asyncio.ensure_future(self._pump(channel))

    def _stop_pumps(self):
        """Stop forwarding channels to the websocket."""
        for task in self._pump_tasks.values():
            task.cancel()
        self._pump_tasks = {}
        if self._writer_task is not None:
            self._writer_task.cancel()
            self._writer_task = None
        # Nothing will drain the queue now, so overflow puts would stay pending
        # forever and be reported as destroyed-but-pending at teardown.
        for task in list(self._pending_puts or ()):
            task.cancel()
        self._pending_puts = set()

    async def _writer(self):
        """Write queued payloads to the websocket, one at a time.

        A single writer keeps the channels correctly ordered relative to each
        other and to synthetic messages such as the restart status, and avoids
        interleaving concurrent write_message calls from several pumps.
        """
        while True:
            payload, binary = await self._outgoing.get()
            try:
                # tornado's write_message returns a Future, and awaiting it is
                # what applies backpressure. Custom websocket handlers are not
                # obliged to, so only await when there is something to await.
                written = self.write_message(payload, binary=binary)
                if inspect.isawaitable(written):
                    await written
            except asyncio.CancelledError:
                raise
            except WebSocketClosedError as e:
                self.log.warning(str(e))
                return
            except Exception:
                self.log.exception("Error writing to websocket")

    async def _enqueue(self, channel, payload, binary):
        """Queue a payload, applying backpressure only where that is safe.

        For shell/control/stdin, waiting here is the point: it suspends the
        calling pump, which stops it reading its socket, so a slow browser is
        felt upstream instead of growing a queue in this process. Those are
        request/reply channels, so the backlog is bounded by outstanding
        requests and never approaches zmq's high-water mark.

        iopub is deliberately excluded. It is an XPUB/SUB channel, and libzmq
        *silently discards* messages once a PUB socket reaches its high-water
        mark (1000 by default; neither ipykernel nor jupyter-server raises it).
        Declining to read iopub would therefore lose kernel output with no
        error anywhere -- the failure mode behind nbconvert#1183. So iopub is
        always drained promptly, and overload stays the job of the existing
        iopub rate limiter, which drops visibly and tells the user why.
        """
        if channel == "iopub":
            self._enqueue_write_nowait(payload, binary)
        else:
            self._start_writer()
            await self._outgoing.put((payload, binary))

    def _enqueue_write_nowait(self, payload, binary):
        """Queue a payload without ever waiting.

        Used for iopub and for synchronous callers (the restart status
        messages). When the writer is behind, the payload is handed to a task
        that waits on our behalf, so the caller is never suspended. Ordering
        holds because asyncio.Queue wakes blocked putters in FIFO order.
        """
        self._start_writer()
        if self._pending_puts is None:
            self._pending_puts = set()
        try:
            self._outgoing.put_nowait((payload, binary))
        except asyncio.QueueFull:
            # Wait on a task rather than dropping the message.
            task = asyncio.ensure_future(self._outgoing.put((payload, binary)))
            self._pending_puts.add(task)
            task.add_done_callback(self._pending_puts.discard)

    async def _pump(self, channel):
        """Forward one channel's messages to the websocket until cancelled."""
        socket = self.channels.get(channel)
        if socket is None:
            return
        while True:
            try:
                msg_list = await socket.recv_multipart()
            except asyncio.CancelledError:
                raise
            except (zmq.ZMQError, RuntimeError):
                # Socket closed underneath us, e.g. by a concurrent disconnect.
                return
            try:
                await self.handle_outgoing_message(channel, msg_list)
            except asyncio.CancelledError:
                raise
            except WebSocketClosedError:
                self.log.debug("Websocket closed while forwarding %s", channel)
                return
            except Exception:
                self.log.exception("Error forwarding %s message to websocket", channel)

    async def nudge(self):
        """Nudge the zmq connections with kernel_info_requests

        Returns once we have received a shell or control reply and at least one
        iopub message, ensuring that zmq subscriptions are established, sockets
        are fully connected, and the kernel is responsive. Keeps retrying
        kernel_info_request until both are received.
        """
        # Do not nudge busy kernels as kernel info requests sent to shell are
        # queued behind execution requests.
        # nudging in this case would cause a potentially very long wait
        # before connections are opened,
        # plus it is *very* unlikely that a busy kernel will not finish
        # establishing its zmq subscriptions before processing the next request.
        if getattr(self.kernel_manager, "execution_state", None) == "busy":
            self.log.debug("Nudge: not nudging busy kernel %s", self.kernel_id)
            return
        # Use a transient shell channel to prevent leaking
        # shell responses to the front-end.
        shell_channel = self.kernel_manager.connect_shell()
        # Use a transient control channel to prevent leaking
        # control responses to the front-end.
        control_channel = self.kernel_manager.connect_control()
        # Snapshot of ports the transient channels above are bound to. If a
        # restart with newports happens mid-nudge, kernel_manager.ports will
        # change and we must abort: the channels are now connected to dead
        # peers, no reply will come, and open() would otherwise block until
        # kernel_info_timeout (default 60s), preventing on_close from firing.
        nudge_ports = list(self.kernel_manager.ports)
        # The IOPub used by the client, whose subscriptions we are verifying.
        iopub_channel = self.channels["iopub"]

        async def wait_for_activity():
            execution_state = getattr(self.kernel_manager, "execution_state", None)
            while execution_state == "starting":
                await asyncio.sleep(0.05)
                execution_state = getattr(self.kernel_manager, "execution_state", None)
            self.log.debug("Nudge: %s execution_state=%s", self.kernel_id, execution_state)

        async def wait_for_reply():
            """Resolve as soon as either transient channel answers."""
            recvs = [
                asyncio.ensure_future(shell_channel.recv_multipart()),
                asyncio.ensure_future(control_channel.recv_multipart()),
            ]
            try:
                await asyncio.wait(recvs, return_when=asyncio.FIRST_COMPLETED)
                self.log.debug("Nudge: info reply received: %s", self.kernel_id)
            finally:
                for recv in recvs:
                    recv.cancel()

        async def wait_for_iopub():
            """Resolve once the shared iopub channel proves it is subscribed.

            The message itself is discarded: it is the side effect of our own
            nudge request, and the client has not subscribed yet.
            """
            await iopub_channel.recv_multipart()
            self.log.debug("Nudge: IOPub received: %s", self.kernel_id)

        async def keep_nudging():
            """Re-send kernel_info_request until told to stop, or until moot."""
            count = 0
            while True:
                count += 1
                # check for stopped kernel
                if self.kernel_id not in self.multi_kernel_manager:
                    self.log.debug("Nudge: cancelling on stopped kernel: %s", self.kernel_id)
                    return
                # If the kernel was restarted with new ports, the transient
                # shell/control channels above are bound to dead peers and will
                # never receive a reply. Bail so connect()/open() can return.
                if list(self.kernel_manager.ports) != nudge_ports:
                    self.log.debug("Nudge: cancelling on port change: %s", self.kernel_id)
                    return
                # check for closed zmq sockets
                if shell_channel.closed or control_channel.closed:
                    self.log.debug("Nudge: cancelling on closed zmq socket: %s", self.kernel_id)
                    return

                log = self.log.warning if count % 10 == 0 else self.log.debug
                log(f"Nudge: attempt {count} on kernel {self.kernel_id}")
                self.session.send(shell_channel, "kernel_info_request")
                self.session.send(control_channel, "kernel_info_request")
                await asyncio.sleep(0.5)

        nudging = asyncio.ensure_future(keep_nudging())
        waiting = asyncio.ensure_future(
            asyncio.gather(wait_for_reply(), wait_for_iopub(), wait_for_activity())
        )
        try:
            # keep_nudging only finishes by giving up, so whichever of the two
            # lands first ends the nudge.
            done, _pending = await asyncio.wait(
                [nudging, waiting],
                timeout=self.kernel_info_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if waiting not in done:
                self.log.debug("Nudge: giving up on kernel %s", self.kernel_id)
        finally:
            nudging.cancel()
            waiting.cancel()
            # Close the transient shell/control sockets we own. The shared
            # iopub channel is left alone: it belongs to the connection, and
            # may already have been torn down by a concurrent disconnect.
            if not shell_channel.closed:
                shell_channel.close()
            if not control_channel.closed:
                control_channel.close()

    async def _register_session(self):
        """Ensure we aren't creating a duplicate session.

        If a previous identical session is still open, close it to avoid collisions.
        This is likely due to a client reconnecting from a lost network connection,
        where the socket on our side has not been cleaned up yet.
        """
        self.session_key = f"{self.kernel_id}:{self.session.session}"
        stale_handler = self._open_sessions.get(self.session_key)
        if stale_handler:
            self.log.warning("Replacing stale connection: %s", self.session_key)
            stale_handler.close()
        if (
            self.kernel_id in self.multi_kernel_manager
        ):  # only update open sessions if kernel is actively managed
            self._open_sessions[self.session_key] = self.websocket_handler

    async def prepare(self):
        """Prepare a kernel connection."""
        # check session collision:
        await self._register_session()
        # then request kernel info, waiting up to a certain time before giving up.
        # We don't want to wait forever, because browsers don't take it well when
        # servers never respond to websocket connection requests.

        if hasattr(self.kernel_manager, "ready"):
            ready = self.kernel_manager.ready
            if not isinstance(ready, asyncio.Future):
                ready = asyncio.wrap_future(ready)
            try:
                await ready
            except Exception as e:
                self.kernel_manager.execution_state = "dead"
                self.kernel_manager.reason = str(e)
                raise web.HTTPError(500, str(e)) from e

        t0 = time.time()
        while not await ensure_async(self.kernel_manager.is_alive()):
            await asyncio.sleep(0.1)
            if (time.time() - t0) > self.multi_kernel_manager.kernel_info_timeout:
                msg = "Kernel never reached an 'alive' state."
                raise TimeoutError(msg)

        self.session.key = self.kernel_manager.session.key
        future = self.request_kernel_info()

        def give_up():
            """Don't wait forever for the kernel to reply"""
            if future.done():
                return
            self.log.warning("Timeout waiting for kernel_info reply from %s", self.kernel_id)
            future.set_result({})

        loop = IOLoop.current()
        loop.add_timeout(loop.time() + self.kernel_info_timeout, give_up)
        # actually wait for it
        await asyncio.wrap_future(future)

    async def connect(self) -> None:
        """Handle a connection.

        Returns once the kernel is responsive and the channels are being
        forwarded to the websocket, or immediately if the connection failed
        and was disconnected.
        """
        self.multi_kernel_manager.notify_connect(self.kernel_id)

        # on new connections, flush the message buffer
        buffer_info = self.multi_kernel_manager.get_buffer(self.kernel_id, self.session_key)
        replay_buffer: list[t.Any] = []
        if buffer_info and buffer_info["session_key"] == self.session_key:
            self.log.info("Restoring connection for %s", self.session_key)
            if self.multi_kernel_manager.ports_changed(self.kernel_id):
                # If the kernel's ports have changed (some restarts trigger this)
                # then reset the channels so nudge() is using the correct iopub channel.
                # Close the stale buffered channels first to avoid leaking FDs.
                for socket in buffer_info["channels"].values():
                    if not socket.closed:
                        socket.close()
                self.create_stream()
            else:
                # The kernel's ports have not changed; use the channels captured in the buffer
                self.channels = buffer_info["channels"]

            replay_buffer = buffer_info["buffer"]
            await self.nudge()
        else:
            try:
                self.create_stream()
                await self.nudge()
            except web.HTTPError as e:
                # Do not log error if the kernel is already shutdown,
                # as it's normal that it's not responding
                try:
                    self.multi_kernel_manager.get_kernel(self.kernel_id)
                    self.log.error("Error opening stream: %s", e)
                except KeyError:
                    pass
                # WebSockets don't respond to traditional error codes so we
                # close the connection.
                for socket in self.channels.values():
                    if not socket.closed:
                        socket.close()
                self.disconnect()
                return

        self.multi_kernel_manager.add_restart_callback(self.kernel_id, self.on_kernel_restarted)
        self.multi_kernel_manager.add_restart_callback(
            self.kernel_id, self.on_restart_failed, "dead"
        )

        if replay_buffer:
            self.log.info("Replaying %s buffered messages", len(replay_buffer))
            for channel, msg_list in replay_buffer:
                await self.handle_outgoing_message(channel, msg_list)

        self._start_pumps()
        ZMQChannelsWebsocketConnection._open_sockets.add(self)

    def close(self):
        """Close the connection."""
        return self.disconnect()

    def disconnect(self):
        """Handle a disconnect."""
        # Stop reading the channels before anything else: the pumps must not
        # race the teardown below for messages on sockets we are closing.
        self._stop_pumps()
        # Decrement the connection counter first, before any work that can
        # block the event loop (zmq channel close can stall on LINGER when
        # the peer is gone, especially on Windows). The counter conceptually
        # drops the moment the websocket closes, not when teardown finishes.
        # notify_disconnect is internally guarded on _kernel_connections, so
        # it is safe to call even when the kernel was transiently removed
        # from the mkm (port-changing restart window).
        self.multi_kernel_manager.notify_disconnect(self.kernel_id)
        self.log.debug("Websocket closed %s", self.session_key)
        # unregister myself as an open session (only if it's really me)
        if self._open_sessions.get(self.session_key) is self.websocket_handler:
            self._open_sessions.pop(self.session_key)

        # Close any pending kernel_info_channel. If the kernel never replied to
        # the kernel_info_request (e.g. hung/rogue), _handle_kernel_info_reply
        # will not have fired to close it. This must run before the
        # start_buffering early-return below, otherwise the channel leaks.
        if self._kernel_info_task is not None:
            self._kernel_info_task.cancel()
            self._kernel_info_task = None
        if self.kernel_info_channel is not None and not self.kernel_info_channel.closed:
            self.kernel_info_channel.close()
            # If this connection owned the shared kernel_info future and we
            # are closing its channel before a reply arrives, unblock any
            # reconnect path waiting on that pending future.
            if not self._kernel_info_future.done():
                self._kernel_info_future.set_result({})
            # Allow a future connection to issue a fresh kernel_info request
            # rather than inheriting an orphaned/empty cached future.
            if (
                getattr(self.kernel_manager, "_kernel_info_future", None)
                is self._kernel_info_future
            ):
                del self.kernel_manager._kernel_info_future
        self.kernel_info_channel = None

        if self.kernel_id in self.multi_kernel_manager:
            self.multi_kernel_manager.remove_restart_callback(
                self.kernel_id,
                self.on_kernel_restarted,
            )
            self.multi_kernel_manager.remove_restart_callback(
                self.kernel_id,
                self.on_restart_failed,
                "dead",
            )

            # start buffering instead of closing if this was the last connection
            if (
                self.kernel_id in self.multi_kernel_manager._kernel_connections
                and self.multi_kernel_manager._kernel_connections[self.kernel_id] == 0
            ):
                self.multi_kernel_manager.start_buffering(
                    self.kernel_id, self.session_key, self.channels
                )
                ZMQChannelsWebsocketConnection._open_sockets.remove(self)
                self._close_future.set_result(None)
                return

        # This method can be called twice, once by self.kernel_died and once
        # from the WebSocket close event. If the WebSocket connection is
        # closed before the ZMQ sockets are setup, they could be None.
        for socket in self.channels.values():
            if socket is not None and not socket.closed:
                socket.close()

        self.channels = {}
        try:
            ZMQChannelsWebsocketConnection._open_sockets.remove(self)
            self._close_future.set_result(None)
        except Exception:
            pass

    def handle_incoming_message(self, incoming_msg: str) -> None:
        """Handle incoming messages from Websocket to ZMQ Sockets."""
        ws_msg = incoming_msg
        if not self.channels:
            # already closed, ignore the message
            self.log.debug("Received message on closed websocket %r", ws_msg)
            return

        if self.subprotocol == "v1.kernel.websocket.jupyter.org":
            channel, msg_list = deserialize_msg_from_ws_v1(ws_msg)
            msg = {
                "header": None,
            }
        else:
            if isinstance(ws_msg, bytes):  # type:ignore[unreachable]
                msg = deserialize_binary_message(ws_msg)  # type:ignore[unreachable]
            else:
                msg = json.loads(ws_msg)
            msg_list = []
            channel = msg.pop("channel", None)

        if channel is None:
            self.log.warning("No channel specified, assuming shell: %s", msg)
            channel = "shell"
        if channel not in self.channels:
            self.log.warning("No such channel: %r", channel)
            return
        am = self.multi_kernel_manager.allowed_message_types
        ignore_msg = False
        if am:
            msg["header"] = self.get_part("header", msg["header"], msg_list)
            assert msg["header"] is not None
            if msg["header"]["msg_type"] not in am:  # type:ignore[unreachable]
                self.log.warning(
                    'Received message of type "%s", which is not allowed. Ignoring.'
                    % msg["header"]["msg_type"]
                )
                ignore_msg = True
        if not ignore_msg:
            socket = self.channels[channel]
            if self.subprotocol == "v1.kernel.websocket.jupyter.org":
                self.session.send_raw(socket, msg_list)
            else:
                self.session.send(socket, msg)

    async def handle_outgoing_message(self, channel: str, outgoing_msg: list[t.Any]) -> None:
        """Handle the outgoing messages from ZMQ sockets to Websocket."""
        msg_list = outgoing_msg
        _, fed_msg_list = self.session.feed_identities(msg_list)

        if self.subprotocol == "v1.kernel.websocket.jupyter.org":
            msg = {"header": None, "parent_header": None, "content": None}
        else:
            msg = self.session.deserialize(fed_msg_list)

        parts = fed_msg_list[1:]

        self._on_error(channel, msg, parts)

        if self._limit_rate(channel, msg, parts):
            return

        if self.subprotocol == "v1.kernel.websocket.jupyter.org":
            await self._on_zmq_reply(channel, parts)
        else:
            await self._on_zmq_reply(channel, msg)

    def get_part(self, field, value, msg_list):
        """Get a part of a message."""
        if value is None:
            field2idx = {
                "header": 0,
                "parent_header": 1,
                "content": 3,
            }
            value = self.session.unpack(msg_list[field2idx[field]])
        return value

    def _reserialize_reply(self, msg_or_list, channel=None):
        """Reserialize a reply message using JSON.

        msg_or_list can be an already-deserialized msg dict or the zmq buffer list.
        If it is the zmq list, it will be deserialized with self.session.

        This takes the msg list from the ZMQ socket and serializes the result for the websocket.
        This method should be used by self._on_zmq_reply to build messages that can
        be sent back to the browser.

        """
        if isinstance(msg_or_list, dict):
            # already unpacked
            msg = msg_or_list
        else:
            _, msg_list = self.session.feed_identities(msg_or_list)
            msg = self.session.deserialize(msg_list)
        if channel:
            msg["channel"] = channel
        if msg["buffers"]:
            buf = serialize_binary_message(msg)
            return buf
        else:
            return json.dumps(msg, default=json_default)

    async def _on_zmq_reply(self, channel, msg_list):
        """Handle a zmq reply.

        The websocket write is awaited: that is what makes backpressure real.
        While the browser is slow to drain, this coroutine is suspended, so its
        channel pump stops calling recv_multipart and the messages stay in
        zmq's buffer rather than accumulating in ours.
        """
        # Sometimes this gets triggered when the on_close method is scheduled in the
        # eventloop but hasn't been called.
        socket = self.channels.get(channel)
        if socket is None or socket.closed:
            self.log.warning("zmq message arrived on closed channel")
            self.disconnect()
            return
        if self.subprotocol == "v1.kernel.websocket.jupyter.org":
            bin_msg = serialize_msg_to_ws_v1(msg_list, channel)
            await self._enqueue(channel, bin_msg, True)
        else:
            try:
                msg = self._reserialize_reply(msg_list, channel=channel)
            except Exception:
                self.log.critical("Malformed message: %r" % msg_list, exc_info=True)
            else:
                await self._enqueue(channel, msg, isinstance(msg, bytes))

    def request_kernel_info(self):
        """send a request for kernel_info"""
        try:
            # check for previous request
            future = self.kernel_manager._kernel_info_future
        except AttributeError:
            self.log.debug("Requesting kernel info from %s", self.kernel_id)
            # Create a kernel_info channel to query the kernel protocol version.
            # This channel will be closed after the kernel_info reply is received.
            if self.kernel_info_channel is None:
                self.kernel_info_channel = self.multi_kernel_manager.connect_shell(self.kernel_id)
            assert self.kernel_info_channel is not None
            self.session.send(self.kernel_info_channel, "kernel_info_request")
            self._kernel_info_task = asyncio.ensure_future(self._await_kernel_info_reply())
            # store the future on the kernel, so only one request is sent
            self.kernel_manager._kernel_info_future = self._kernel_info_future
        else:
            if not future.done():
                self.log.debug("Waiting for pending kernel_info request")
            future.add_done_callback(lambda f: self._finish_kernel_info(f.result()))
        return _ensure_future(self._kernel_info_future)

    async def _await_kernel_info_reply(self):
        """Wait for the reply on the transient kernel_info channel."""
        channel = self.kernel_info_channel
        if channel is None:
            return
        try:
            msg = await channel.recv_multipart()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.log.debug("kernel_info channel closed before a reply arrived")
            # Nothing else will settle the future now, and prepare() waits on
            # it; leaving it pending would stall the connection until the full
            # kernel_info_timeout elapsed. Continue with default protocol
            # assumptions, exactly as the timeout path does.
            self._finish_kernel_info({})
            return
        self._handle_kernel_info_reply(msg)

    def _handle_kernel_info_reply(self, msg):
        """process the kernel_info_reply

        enabling msg spec adaptation, if necessary
        """
        _idents, msg = self.session.feed_identities(msg)
        try:
            msg = self.session.deserialize(msg)
        except BaseException:
            self.log.error("Bad kernel_info reply", exc_info=True)
            self._kernel_info_future.set_result({})
            return
        else:
            info = msg["content"]
            self.log.debug("Received kernel info: %s", info)
            if msg["msg_type"] != "kernel_info_reply" or "protocol_version" not in info:
                self.log.error("Kernel info request failed, assuming current %s", info)
                info = {}
            self._finish_kernel_info(info)

        # close the kernel_info channel, we don't need it anymore
        if self.kernel_info_channel:
            self.kernel_info_channel.close()
        self.kernel_info_channel = None
        self._kernel_info_task = None

    def _finish_kernel_info(self, info):
        """Finish handling kernel_info reply

        Set up protocol adaptation, if needed,
        and signal that connection can continue.
        """
        protocol_version = info.get("protocol_version", client_protocol_version)
        if protocol_version != client_protocol_version:
            self.session.adapt_version = int(protocol_version.split(".")[0])
            self.log.info(
                f"Adapting from protocol version {protocol_version} (kernel {self.kernel_id}) to {client_protocol_version} (client)."
            )
        if not self._kernel_info_future.done():
            self._kernel_info_future.set_result(info)

    def write_stderr(self, error_message, parent_header):
        """Write a message to stderr."""
        self.log.warning(error_message)
        err_msg = self.session.msg(
            "stream",
            content={"text": error_message + "\n", "name": "stderr"},
            parent=parent_header,
        )
        if self.subprotocol == "v1.kernel.websocket.jupyter.org":
            bin_msg = serialize_msg_to_ws_v1(err_msg, "iopub", self.session.pack)
            self.write_message(bin_msg, binary=True)
        else:
            err_msg["channel"] = "iopub"
            self.write_message(json.dumps(err_msg, default=json_default))

    def _limit_rate(self, channel, msg, msg_list):
        """Limit the message rate on a channel."""
        if not (self.limit_rate and channel == "iopub"):
            return False

        msg["header"] = self.get_part("header", msg["header"], msg_list)

        msg_type = msg["header"]["msg_type"]
        if msg_type == "status":
            msg["content"] = self.get_part("content", msg["content"], msg_list)
            if msg["content"].get("execution_state") == "idle":
                # reset rate limit counter on status=idle,
                # to avoid 'Run All' hitting limits prematurely.
                self._iopub_window_byte_queue = []
                self._iopub_window_msg_count = 0
                self._iopub_window_byte_count = 0
                self._iopub_msgs_exceeded = False
                self._iopub_data_exceeded = False

        if msg_type not in {"status", "comm_open", "execute_input"}:
            # Remove the counts queued for removal.
            now = IOLoop.current().time()
            while len(self._iopub_window_byte_queue) > 0:
                queued = self._iopub_window_byte_queue[0]
                if now >= queued[0]:
                    self._iopub_window_byte_count -= queued[1]
                    self._iopub_window_msg_count -= 1
                    del self._iopub_window_byte_queue[0]
                else:
                    # This part of the queue hasn't be reached yet, so we can
                    # abort the loop.
                    break

            # Increment the bytes and message count
            self._iopub_window_msg_count += 1
            byte_count = sum(len(x) for x in msg_list) if msg_type == "stream" else 0
            self._iopub_window_byte_count += byte_count

            # Queue a removal of the byte and message count for a time in the
            # future, when we are no longer interested in it.
            self._iopub_window_byte_queue.append((now + self.rate_limit_window, byte_count))

            # Check the limits, set the limit flags, and reset the
            # message and data counts.
            msg_rate = float(self._iopub_window_msg_count) / self.rate_limit_window
            data_rate = float(self._iopub_window_byte_count) / self.rate_limit_window

            # Check the msg rate
            if self.iopub_msg_rate_limit > 0 and msg_rate > self.iopub_msg_rate_limit:
                if not self._iopub_msgs_exceeded:
                    self._iopub_msgs_exceeded = True
                    msg["parent_header"] = self.get_part(
                        "parent_header", msg["parent_header"], msg_list
                    )
                    self.write_stderr(
                        dedent(
                            f"""\
                    IOPub message rate exceeded.
                    The Jupyter server will temporarily stop sending output
                    to the client in order to avoid crashing it.
                    To change this limit, set the config variable
                    `--ServerApp.iopub_msg_rate_limit`.

                    Current values:
                    ServerApp.iopub_msg_rate_limit={self.iopub_msg_rate_limit} (msgs/sec)
                    ServerApp.rate_limit_window={self.rate_limit_window} (secs)
                    """
                        ),
                        msg["parent_header"],
                    )
            # resume once we've got some headroom below the limit
            elif self._iopub_msgs_exceeded and msg_rate < (0.8 * self.iopub_msg_rate_limit):
                self._iopub_msgs_exceeded = False
                if not self._iopub_data_exceeded:
                    self.log.warning("iopub messages resumed")

            # Check the data rate
            if self.iopub_data_rate_limit > 0 and data_rate > self.iopub_data_rate_limit:
                if not self._iopub_data_exceeded:
                    self._iopub_data_exceeded = True
                    msg["parent_header"] = self.get_part(
                        "parent_header", msg["parent_header"], msg_list
                    )
                    self.write_stderr(
                        dedent(
                            f"""\
                    IOPub data rate exceeded.
                    The Jupyter server will temporarily stop sending output
                    to the client in order to avoid crashing it.
                    To change this limit, set the config variable
                    `--ServerApp.iopub_data_rate_limit`.

                    Current values:
                    ServerApp.iopub_data_rate_limit={self.iopub_data_rate_limit} (bytes/sec)
                    ServerApp.rate_limit_window={self.rate_limit_window} (secs)
                    """
                        ),
                        msg["parent_header"],
                    )
            # resume once we've got some headroom below the limit
            elif self._iopub_data_exceeded and data_rate < (0.8 * self.iopub_data_rate_limit):
                self._iopub_data_exceeded = False
                if not self._iopub_msgs_exceeded:
                    self.log.warning("iopub messages resumed")

            # If either of the limit flags are set, do not send the message.
            if self._iopub_msgs_exceeded or self._iopub_data_exceeded:
                # we didn't send it, remove the current message from the calculus
                self._iopub_window_msg_count -= 1
                self._iopub_window_byte_count -= byte_count
                self._iopub_window_byte_queue.pop(-1)
                return True

            return False

    def _send_status_message(self, status):
        """Send a status message.

        No explicit iopub flush is needed: this payload goes through the same
        queue as the channel traffic, so everything the stopped kernel already
        sent is written first.
        """
        msg = self.session.msg("status", {"execution_state": status})
        if self.subprotocol == "v1.kernel.websocket.jupyter.org":
            bin_msg = serialize_msg_to_ws_v1(msg, "iopub", self.session.pack)
            self._enqueue_write_nowait(bin_msg, True)
        else:
            msg["channel"] = "iopub"
            self._enqueue_write_nowait(json.dumps(msg, default=json_default), False)

    def on_kernel_restarted(self):
        """Handle a kernel restart."""
        self.log.warning("kernel %s restarted", self.kernel_id)
        self._send_status_message("restarting")

    def on_restart_failed(self):
        """Handle a kernel restart failure."""
        self.log.error("kernel %s restarted failed!", self.kernel_id)
        self._send_status_message("dead")

    def _on_error(self, channel, msg, msg_list):
        """Handle an error message."""
        if self.multi_kernel_manager.allow_tracebacks:
            return

        if channel == "iopub":
            msg["header"] = self.get_part("header", msg["header"], msg_list)
            if msg["header"]["msg_type"] == "error":
                msg["content"] = self.get_part("content", msg["content"], msg_list)
                msg["content"]["ename"] = "ExecutionError"
                msg["content"]["evalue"] = "Execution error"
                msg["content"]["traceback"] = [self.kernel_manager.traceback_replacement_message]
                if self.subprotocol == "v1.kernel.websocket.jupyter.org":
                    msg_list[3] = self.session.pack(msg["content"])


KernelWebsocketConnectionABC.register(ZMQChannelsWebsocketConnection)
