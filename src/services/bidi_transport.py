"""Pure-Python WebDriver BiDi WebSocket client and local SOCKS5 upstream proxy relay."""

from __future__ import annotations

import asyncio
import base64
from collections import deque
import json
import re
import socket
import struct
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

try:
    import websockets
except ImportError:
    websockets = None

try:
    import aiohttp
except ImportError:
    aiohttp = None


BIDI_ENDPOINT_PATTERN = re.compile(r"WebDriver BiDi listening on (ws://[^\s]+)")


async def wait_for_bidi_endpoint(
    proc: asyncio.subprocess.Process, timeout: float = 45.0
) -> str:
    """Read subprocess stdout lines until the WebDriver BiDi ws:// endpoint appears."""
    recent_lines: deque[str] = deque(maxlen=25)
    deadline = time.monotonic() + timeout
    assert proc.stdout is not None
    while time.monotonic() < deadline:
        remaining = max(0.5, deadline - time.monotonic())
        line_bytes = await asyncio.wait_for(
            proc.stdout.readline(), timeout=remaining
        )
        if not line_bytes:
            break
        line = line_bytes.decode("utf-8", errors="replace").strip()
        if line:
            recent_lines.append(line)
        match = BIDI_ENDPOINT_PATTERN.search(line)
        if match:
            return match.group(1)

    tail = " | ".join(recent_lines)
    raise RuntimeError(f"Camoufox 未输出 WebDriver BiDi 端点: {tail}")


class LocalSocks5Relay:
    """Local `127.0.0.1:0` SOCKS5 server forwarding connections to upstream HTTP/SOCKS5 proxy."""

    def __init__(self, upstream_proxy_url: str):
        self.upstream_proxy_url = upstream_proxy_url.strip()
        self.parsed = urlparse(
            self.upstream_proxy_url
            if "://" in self.upstream_proxy_url
            else f"http://{self.upstream_proxy_url}"
        )
        self._server: Optional[asyncio.AbstractServer] = None
        self._client_tasks: Set[asyncio.Task] = set()
        self._client_writers: Set[asyncio.StreamWriter] = set()
        self.host = "127.0.0.1"
        self.port = 0

    async def start(self) -> Tuple[str, int]:
        self._server = await asyncio.start_server(
            self._handle_client, self.host, 0
        )
        sockets = self._server.sockets or []
        if sockets:
            self.port = sockets[0].getsockname()[1]
        return self.host, self.port

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None
        writers = list(self._client_writers)
        self._client_writers.clear()
        for w in writers:
            try:
                w.close()
            except Exception:
                pass
        tasks = list(self._client_tasks)
        self._client_tasks.clear()
        for t in tasks:
            if not t.done():
                t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        current_task = asyncio.current_task()
        if current_task is not None:
            self._client_tasks.add(current_task)
        self._client_writers.add(writer)
        upstream_writer: Optional[asyncio.StreamWriter] = None
        try:
            header = await asyncio.wait_for(reader.readexactly(2), timeout=10.0)
            if header[0] != 5:
                return
            methods = await asyncio.wait_for(
                reader.readexactly(header[1]), timeout=10.0
            )
            if 0 not in methods:
                writer.write(b"\x05\xff")
                await writer.drain()
                return
            writer.write(b"\x05\x00")
            await writer.drain()

            req = await asyncio.wait_for(reader.readexactly(4), timeout=10.0)
            if req[0] != 5 or req[1] != 1:
                writer.write(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00")
                await writer.drain()
                return
            atyp = req[3]
            if atyp == 1:
                raw_ip = await asyncio.wait_for(reader.readexactly(4), timeout=10.0)
                target_host = socket.inet_ntop(socket.AF_INET, raw_ip)
            elif atyp == 4:
                raw_ip = await asyncio.wait_for(reader.readexactly(16), timeout=10.0)
                target_host = socket.inet_ntop(socket.AF_INET6, raw_ip)
            elif atyp == 3:
                length = (
                    await asyncio.wait_for(reader.readexactly(1), timeout=10.0)
                )[0]
                target_host = (
                    await asyncio.wait_for(reader.readexactly(length), timeout=10.0)
                ).decode("utf-8", errors="replace")
            else:
                writer.write(b"\x05\x08\x00\x01\x00\x00\x00\x00\x00\x00")
                await writer.drain()
                return
            target_port = struct.unpack(
                "!H", await asyncio.wait_for(reader.readexactly(2), timeout=10.0)
            )[0]

            upstream_reader, upstream_writer = await self._dial_upstream(
                target_host, target_port
            )
            self._client_writers.add(upstream_writer)
            writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
            await writer.drain()

            async def _pipe(
                r: asyncio.StreamReader, w: asyncio.StreamWriter
            ) -> None:
                try:
                    while True:
                        chunk = await r.read(32768)
                        if not chunk:
                            break
                        w.write(chunk)
                        await w.drain()
                except Exception:
                    pass
                finally:
                    try:
                        w.close()
                    except Exception:
                        pass

            await asyncio.gather(
                _pipe(reader, upstream_writer), _pipe(upstream_reader, writer)
            )
        except Exception:
            try:
                writer.write(b"\x05\x01\x00\x01\x00\x00\x00\x00\x00\x00")
                await writer.drain()
            except Exception:
                pass
        finally:
            if current_task is not None:
                self._client_tasks.discard(current_task)
            for w in (upstream_writer, writer):
                if w is not None:
                    self._client_writers.discard(w)
                    try:
                        w.close()
                    except Exception:
                        pass

    async def _dial_upstream(
        self, target_host: str, target_port: int
    ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        scheme = (self.parsed.scheme or "http").lower()
        proxy_host = self.parsed.hostname or "127.0.0.1"
        proxy_port = self.parsed.port or (1080 if "socks" in scheme else 8080)
        username = self.parsed.username or ""
        password = self.parsed.password or ""

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(proxy_host, proxy_port), timeout=15.0
        )
        try:
            if scheme.startswith("socks5"):
                if username or password:
                    writer.write(b"\x05\x02\x00\x02")
                else:
                    writer.write(b"\x05\x01\x00")
                await writer.drain()
                resp = await asyncio.wait_for(reader.readexactly(2), timeout=10.0)
                if resp[1] == 2:
                    u_bytes = username.encode("utf-8")
                    p_bytes = password.encode("utf-8")
                    writer.write(
                        bytes([1, len(u_bytes)])
                        + u_bytes
                        + bytes([len(p_bytes)])
                        + p_bytes
                    )
                    await writer.drain()
                    auth_resp = await asyncio.wait_for(
                        reader.readexactly(2), timeout=10.0
                    )
                    if auth_resp[1] != 0:
                        raise RuntimeError("SOCKS5 upstream auth failed")
                elif resp[1] != 0:
                    raise RuntimeError(f"SOCKS5 upstream method rejected: {resp[1]}")

                host_bytes = target_host.encode("utf-8")
                writer.write(
                    b"\x05\x01\x00\x03"
                    + bytes([len(host_bytes)])
                    + host_bytes
                    + struct.pack("!H", target_port)
                )
                await writer.drain()
                conn_resp = await asyncio.wait_for(
                    reader.readexactly(4), timeout=15.0
                )
                if conn_resp[1] != 0:
                    raise RuntimeError(
                        f"SOCKS5 upstream connect failed: {conn_resp[1]}"
                    )
                atyp = conn_resp[3]
                if atyp == 1:
                    await asyncio.wait_for(reader.readexactly(6), timeout=15.0)
                elif atyp == 4:
                    await asyncio.wait_for(reader.readexactly(18), timeout=15.0)
                elif atyp == 3:
                    ln = (await asyncio.wait_for(reader.readexactly(1), timeout=15.0))[0]
                    await asyncio.wait_for(reader.readexactly(ln + 2), timeout=15.0)
                return reader, writer

            connect_lines = [
                f"CONNECT {target_host}:{target_port} HTTP/1.1",
                f"Host: {target_host}:{target_port}",
            ]
            if username or password:
                token = base64.b64encode(
                    f"{username}:{password}".encode("utf-8")
                ).decode("ascii")
                connect_lines.append(f"Proxy-Authorization: Basic {token}")
            connect_lines.append("\r\n")
            writer.write("\r\n".join(connect_lines).encode("utf-8"))
            await writer.drain()

            status_line = await asyncio.wait_for(reader.readline(), timeout=15.0)
            if b" 200 " not in status_line:
                raise RuntimeError(
                    f"HTTP proxy CONNECT failed: {status_line.decode('utf-8', errors='replace').strip()}"
                )
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=10.0)
                if line in (b"\r\n", b"\n", b""):
                    break
            return reader, writer
        except BaseException:
            writer.close()
            raise


def _from_bidi_remote_value(remote: Any) -> Any:
    """Recursively decode a WebDriver BiDi RemoteValue into a native Python value."""
    if not isinstance(remote, dict):
        return remote
    rtype = remote.get("type")
    if rtype in ("undefined", "null"):
        return None
    if rtype in ("string", "boolean", "bigint"):
        return remote.get("value")
    if rtype == "number":
        val = remote.get("value")
        if isinstance(val, (int, float)):
            return val
        return 0
    if rtype == "array":
        items = remote.get("value")
        if isinstance(items, list):
            return [_from_bidi_remote_value(item) for item in items]
        return []
    if rtype in ("object", "map"):
        pairs = remote.get("value")
        out: Dict[str, Any] = {}
        if isinstance(pairs, list):
            for pair in pairs:
                if isinstance(pair, list) and len(pair) == 2:
                    raw_key, raw_val = pair
                    key = (
                        _from_bidi_remote_value(raw_key)
                        if isinstance(raw_key, dict)
                        else str(raw_key)
                    )
                    out[str(key)] = _from_bidi_remote_value(raw_val)
        return out
    return remote.get("value")


def _to_bidi_local_value(value: Any) -> Dict[str, Any]:
    """Encode a native Python value into a WebDriver BiDi LocalValue."""
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "boolean", "value": value}
    if isinstance(value, (int, float)):
        return {"type": "number", "value": value}
    if isinstance(value, str):
        return {"type": "string", "value": value}
    if isinstance(value, (list, tuple)):
        return {
            "type": "array",
            "value": [_to_bidi_local_value(item) for item in value],
        }
    if isinstance(value, dict):
        return {
            "type": "object",
            "value": [
                [str(k), _to_bidi_local_value(v)] for k, v in value.items()
            ],
        }
    return {"type": "string", "value": str(value)}


class BiDiChannel:
    """Receives `script.message` events for a registered BiDi channel."""

    def __init__(self, client: "BiDiClient", name: str):
        self.client = client
        self.name = name
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self._closed = False

    def push(self, data: Any) -> None:
        if not self._closed:
            self._queue.put_nowait(data)

    def fail(self, exc: Exception) -> None:
        if not self._closed:
            self._closed = True
            self._queue.put_nowait(exc)

    async def next_text(self, timeout: float = 90.0) -> str:
        item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
        if isinstance(item, Exception):
            raise item
        val = _from_bidi_remote_value(item)
        return str(val or "")

    def close(self) -> None:
        self._closed = True
        self.client._channels.pop(self.name, None)


class BiDiClient:
    """Async WebDriver BiDi client over WebSocket (supports `websockets` or `aiohttp`)."""

    def __init__(self, ws_send: Callable[[str], Any], ws_close: Callable[[], Any]):
        self._ws_send = ws_send
        self._ws_close = ws_close
        self._send_lock = asyncio.Lock()
        self._next_id = 0
        self._pending: Dict[int, asyncio.Future] = {}
        self._channels: Dict[str, BiDiChannel] = {}
        self._event_listeners: List[Callable[[str, Dict[str, Any]], None]] = []
        self._closed = False
        self._failure: Optional[Exception] = None
        self._reader_task: Optional[asyncio.Task] = None

    @staticmethod
    def _normalize_endpoint(endpoint: str) -> str:
        parsed = urlparse(endpoint.strip())
        if parsed.path in ("", "/"):
            return endpoint.strip().rstrip("/") + "/session"
        return endpoint.strip()

    @classmethod
    async def connect(cls, endpoint: str, timeout: float = 30.0) -> "BiDiClient":
        endpoint = cls._normalize_endpoint(endpoint)
        if websockets is not None:
            ws = await asyncio.wait_for(
                websockets.connect(
                    endpoint,
                    max_size=64 * 1024 * 1024,
                    ping_interval=None,
                ),
                timeout=timeout,
            )
            client = cls(ws.send, ws.close)

            async def _read_loop() -> None:
                try:
                    async for raw in ws:
                        text = (
                            raw.decode("utf-8", errors="replace")
                            if isinstance(raw, (bytes, bytearray))
                            else str(raw)
                        )
                        client._handle_message(text)
                except Exception as exc:
                    client._fail_all(exc)
                finally:
                    client._fail_all(RuntimeError("BiDi WebSocket closed"))

            client._reader_task = asyncio.create_task(_read_loop())
            return client

        if aiohttp is not None:
            session = aiohttp.ClientSession()
            try:
                ws = await asyncio.wait_for(
                    session.ws_connect(
                        endpoint, max_msg_size=64 * 1024 * 1024, heartbeat=None
                    ),
                    timeout=timeout,
                )
            except BaseException:
                await session.close()
                raise

            async def _close_all() -> None:
                try:
                    await ws.close()
                finally:
                    await session.close()

            client = cls(ws.send_str, _close_all)

            async def _read_loop() -> None:
                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            client._handle_message(msg.data)
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            client._handle_message(
                                msg.data.decode("utf-8", errors="replace")
                            )
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
                except Exception as exc:
                    client._fail_all(exc)
                finally:
                    client._fail_all(RuntimeError("BiDi WebSocket closed"))
                    await session.close()

            client._reader_task = asyncio.create_task(_read_loop())
            return client

        raise RuntimeError(
            "缺少 WebSocket 客户端依赖：请安装 websockets 或 aiohttp"
        )

    def _fail_all(self, exc: Exception) -> None:
        if self._closed:
            return
        self._closed = True
        self._failure = exc
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(exc)
        self._pending.clear()
        for ch in list(self._channels.values()):
            ch.fail(exc)
        self._channels.clear()

    def _handle_message(self, raw_text: str) -> None:
        try:
            envelope = json.loads(raw_text)
        except Exception:
            return
        if not isinstance(envelope, dict):
            return
        msg_id = envelope.get("id")
        if msg_id is not None:
            fut = self._pending.pop(int(msg_id), None)
            if fut is not None and not fut.done():
                fut.set_result(envelope)
            return
        method = str(envelope.get("method") or "")
        params = envelope.get("params")
        if not isinstance(params, dict):
            params = {}
        if method == "script.message":
            ch_name = str(params.get("channel") or "")
            ch = self._channels.get(ch_name)
            if ch is not None:
                ch.push(params.get("data"))
        for listener in list(self._event_listeners):
            try:
                listener(method, params)
            except Exception:
                pass

    def add_listener(
        self, callback: Callable[[str, Dict[str, Any]], None]
    ) -> None:
        if callback not in self._event_listeners:
            self._event_listeners.append(callback)

    def remove_listener(
        self, callback: Callable[[str, Dict[str, Any]], None]
    ) -> None:
        if callback in self._event_listeners:
            self._event_listeners.remove(callback)

    def open_channel(self, name: str) -> BiDiChannel:
        ch = BiDiChannel(self, name)
        self._channels[name] = ch
        return ch

    async def command(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 60.0,
    ) -> Dict[str, Any]:
        if self._closed:
            raise self._failure or RuntimeError("BiDi connection is closed")
        loop = asyncio.get_running_loop()
        self._next_id += 1
        req_id = self._next_id
        fut: asyncio.Future = loop.create_future()
        self._pending[req_id] = fut
        payload = json.dumps(
            {"id": req_id, "method": method, "params": params or {}},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        try:
            async with self._send_lock:
                await asyncio.wait_for(self._ws_send(payload), timeout=timeout)
            envelope = await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending.pop(req_id, None)

        if envelope.get("type") == "error" or "error" in envelope:
            err_code = envelope.get("error") or "unknown"
            err_msg = envelope.get("message") or ""
            raise RuntimeError(f"BiDi {method} failed: {err_code} ({err_msg})")
        result = envelope.get("result")
        return result if isinstance(result, dict) else {}

    async def evaluate(
        self,
        context_id: str,
        expression: str,
        *,
        await_promise: bool = True,
        sandbox: str = "",
        user_activation: bool = False,
        timeout: float = 45.0,
    ) -> Any:
        target: Dict[str, Any] = {"context": context_id}
        if sandbox:
            target["sandbox"] = sandbox
        params: Dict[str, Any] = {
            "expression": expression,
            "target": target,
            "awaitPromise": await_promise,
            "resultOwnership": "none",
        }
        if user_activation:
            params["userActivation"] = True
        res = await self.command("script.evaluate", params, timeout=timeout)
        if res.get("type") == "exception":
            details = res.get("exceptionDetails") or {}
            text = details.get("text") or "JS evaluation exception"
            raise RuntimeError(f"BiDi script.evaluate exception: {text}")
        return _from_bidi_remote_value(res.get("result"))

    async def call_function(
        self,
        context_id: str,
        function_declaration: str,
        arguments: Optional[List[Dict[str, Any]]] = None,
        *,
        await_promise: bool = True,
        sandbox: str = "",
        user_activation: bool = False,
        timeout: float = 60.0,
    ) -> Any:
        target: Dict[str, Any] = {"context": context_id}
        if sandbox:
            target["sandbox"] = sandbox
        params: Dict[str, Any] = {
            "functionDeclaration": function_declaration,
            "target": target,
            "awaitPromise": await_promise,
            "resultOwnership": "none",
            "arguments": arguments or [],
        }
        if user_activation:
            params["userActivation"] = True
        res = await self.command("script.callFunction", params, timeout=timeout)
        if res.get("type") == "exception":
            details = res.get("exceptionDetails") or {}
            text = details.get("text") or "JS callFunction exception"
            raise RuntimeError(f"BiDi script.callFunction exception: {text}")
        return _from_bidi_remote_value(res.get("result"))

    async def close(self) -> None:
        self._fail_all(RuntimeError("BiDi client closed"))
        try:
            await self._ws_close()
        except Exception:
            pass
        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
            try:
                await self._reader_task
            except BaseException:
                pass
