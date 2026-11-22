"""UDP 探测客户端：一次测量独占一个套接字，按载荷内容精确匹配回包。

每个探测包都是 ``size`` 字节的随机载荷，回包必须与发出内容逐字节一致
才算“匹配回包”，因此串台包、迟到包、对端反射的杂包都会被忽略。
"""
from __future__ import annotations

import asyncio
import os


class _Inbox(asyncio.DatagramProtocol):
    """把收到的数据报塞进队列，由发送方按内容认领。"""

    def __init__(self) -> None:
        self.queue: asyncio.Queue[bytes] = asyncio.Queue()

    def datagram_received(self, data: bytes, addr) -> None:  # noqa: ANN001
        self.queue.put_nowait(data)

    def error_received(self, exc: Exception) -> None:
        # 例如 ICMP port unreachable：按超时处理即可，无需中断测量。
        pass


class UdpProber:
    """面向一条线路（host:port）的同步式往返探测器。"""

    def __init__(self, host: str, port: int, timeout_s: float) -> None:
        self.host = host
        self.port = port
        self.timeout_s = timeout_s
        self.sent = 0  # 已发出的探测包总数（用于核对探测次数复杂度）
        self._transport: asyncio.DatagramTransport | None = None
        self._inbox: _Inbox | None = None

    async def open(self) -> None:
        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            _Inbox, local_addr=("0.0.0.0", 0)
        )
        self._transport = transport  # type: ignore[assignment]
        self._inbox = protocol  # type: ignore[assignment]

    def close(self) -> None:
        if self._transport is not None:
            self._transport.close()
            self._transport = None

    async def roundtrip(self, size: int) -> float | None:
        """发一个 ``size`` 字节的包，等到匹配回包则返回 RTT(秒)，超时返回 None。"""
        assert self._transport is not None and self._inbox is not None
        payload = os.urandom(size)
        # 清掉上一轮的迟到回包，避免误认领。
        while not self._inbox.queue.empty():
            self._inbox.queue.get_nowait()

        loop = asyncio.get_running_loop()
        start = loop.time()
        self._transport.sendto(payload, (self.host, self.port))
        self.sent += 1
        deadline = start + self.timeout_s
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            try:
                data = await asyncio.wait_for(self._inbox.queue.get(), remaining)
            except asyncio.TimeoutError:
                return None
            if data == payload:
                return loop.time() - start
            # 内容不匹配：杂包/迟到包，丢弃并继续等，直到超过截止时间。
