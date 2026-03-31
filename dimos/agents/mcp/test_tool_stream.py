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

import json
from threading import Thread
import time

import httpx
from langchain_core.messages import HumanMessage
import pytest

from dimos.agents.annotation import skill
from dimos.agents.mcp.mcp_adapter import McpAdapter
from dimos.agents.mcp.mcp_server import McpServer
from dimos.agents.mcp.tool_stream import ToolStream
from dimos.core.blueprints import autoconnect
from dimos.core.global_config import global_config
from dimos.core.module import Module


class StreamingModule(Module):
    """Test module that uses ToolStream to send multiple updates."""

    @skill
    def start_streaming(self, count: int) -> None:
        """Starts streaming count updates back to the agent."""
        stream = ToolStream("start_streaming")
        stream.start()

        def _stream_loop() -> None:
            try:
                for i in range(count):
                    time.sleep(0.1)
                    stream.send(f"Update {i + 1} of {count}")
            finally:
                stream.stop()

        Thread(target=_stream_loop, daemon=True).start()


@pytest.fixture
def mcp_server():
    """Start a blueprint with StreamingModule + McpServer, wait for readiness."""
    global_config.update(viewer="none")
    blueprint = autoconnect(StreamingModule.blueprint(), McpServer.blueprint())
    coordinator = blueprint.build()

    adapter = McpAdapter()
    if not adapter.wait_for_ready(timeout=15):
        coordinator.stop()
        pytest.fail("MCP server did not become ready")

    yield adapter

    coordinator.stop()


@pytest.mark.slow
def test_tool_stream_inline_sse(mcp_server: McpAdapter) -> None:
    """Streaming tool returns inline SSE notifications when client accepts SSE."""
    adapter = mcp_server
    adapter.initialize()

    body = {
        "jsonrpc": "2.0",
        "id": 42,
        "method": "tools/call",
        "params": {"name": "start_streaming", "arguments": {"count": 3}},
    }

    events = []
    with httpx.Client(timeout=30.0) as client:
        with client.stream(
            "POST",
            adapter.url,
            json=body,
            headers={"Accept": "application/json, text/event-stream"},
        ) as response:
            assert response.headers["content-type"].startswith("text/event-stream")
            for line in response.iter_lines():
                if line.startswith("data: "):
                    events.append(json.loads(line[6:]))

    notifications = [e for e in events if e.get("method") == "notifications/message"]
    results = [e for e in events if "result" in e]

    assert len(notifications) == 3
    assert notifications[0]["params"]["data"] == "Update 1 of 3"
    assert notifications[1]["params"]["data"] == "Update 2 of 3"
    assert notifications[2]["params"]["data"] == "Update 3 of 3"

    assert len(results) == 1
    assert results[0]["id"] == 42
    content_text = results[0]["result"]["content"][0]["text"]
    assert "Update 1 of 3" in content_text
    assert "Update 3 of 3" in content_text


@pytest.mark.slow
def test_tool_stream_agent(agent_setup) -> None:  # type: ignore[no-untyped-def]
    """Tool stream updates arrive at the agent as HumanMessages."""
    history = agent_setup(
        blueprints=[StreamingModule.blueprint()],
        messages=[
            HumanMessage("Start streaming 3 updates using the start_streaming tool with count=3.")
        ],
    )

    # agent_setup returns after the initial tool call round-trip.  The tool
    # stream updates arrive asynchronously afterwards — poll the history list
    # (which is still being mutated by the /agent transport callback).
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        stream_updates = [
            m
            for m in history
            if isinstance(m, HumanMessage) and "[Tool stream update" in str(m.content)
        ]
        if len(stream_updates) >= 3:
            break
        time.sleep(0.2)

    stream_updates = [
        m
        for m in history
        if isinstance(m, HumanMessage) and "[Tool stream update" in str(m.content)
    ]
    assert len(stream_updates) == 3
    assert "Update 1 of 3" in stream_updates[0].content
    assert "Update 2 of 3" in stream_updates[1].content
    assert "Update 3 of 3" in stream_updates[2].content


@pytest.fixture()
def make_stream(mocker):
    """Create a ToolStream with a mocked transport."""
    mock_transport = mocker.MagicMock()
    mocker.patch("dimos.agents.mcp.tool_stream.pLCMTransport", return_value=mock_transport)
    stream = ToolStream("test_tool")
    stream.start()
    return stream, mock_transport


def test_send_after_stop_does_not_raise(make_stream) -> None:
    stream, _ = make_stream
    stream.stop()
    # Must not raise even though transport is None after stop.
    stream.send("should be ignored")


def test_double_stop_is_safe(make_stream) -> None:
    stream, mock_transport = make_stream
    stream.stop()
    stream.stop()
    # Transport stop called only once.
    mock_transport.stop.assert_called_once()
