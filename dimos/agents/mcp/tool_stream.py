# Copyright 2026 Dimensional Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from dataclasses import asdict, dataclass
import threading
from typing import Any, Literal
import uuid

from dimos.core.transport import pLCMTransport
from dimos.utils.logging_config import setup_logger

logger = setup_logger()

_TOOL_STREAM_TOPIC = "/tool_streams"


@dataclass(frozen=True, slots=True)
class ToolStreamEvent:
    """A single event published on a tool stream channel."""

    stream_id: str
    tool_name: str
    type: Literal["update", "close"]
    text: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dict, omitting ``None`` values."""
        return {k: v for k, v in asdict(self).items() if v is not None}


class ToolStream:
    """A streaming channel for sending updates from a running skill to the agent.

    Each `ToolStream` publishes messages on a shared LCM topic.  The agent
    (or the MCP server SSE endpoint) subscribes once and receives all updates.
    """

    def __init__(self, tool_name: str) -> None:
        self.tool_name: str = tool_name
        self.id: str = str(uuid.uuid4())
        self._closed: threading.Event = threading.Event()
        self._transport: pLCMTransport[dict[str, Any]] | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        self._transport = pLCMTransport(_TOOL_STREAM_TOPIC)
        self._transport.start()

    def send(self, message: str) -> None:
        with self._lock:
            if self._closed.is_set():
                logger.error("Attempted to send on closed ToolStream", stream_id=self.id)
                return
            if self._transport is None:
                logger.error("ToolStream transport not initialized", stream_id=self.id)
                return
            self._transport.publish(
                ToolStreamEvent(
                    stream_id=self.id,
                    tool_name=self.tool_name,
                    type="update",
                    text=message,
                ).to_dict(),
            )

    def stop(self) -> None:
        with self._lock:
            if self._closed.is_set():
                return
            self._closed.set()
            if self._transport is not None:
                try:
                    self._transport.publish(
                        ToolStreamEvent(
                            stream_id=self.id,
                            tool_name=self.tool_name,
                            type="close",
                        ).to_dict(),
                    )
                finally:
                    self._transport.stop()
                    self._transport = None

    @property
    def is_closed(self) -> bool:
        return self._closed.is_set()
