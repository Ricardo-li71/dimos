# Copyright 2025-2026 Dimensional Inc.
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
from queue import Empty, Queue
from threading import Event, RLock, Thread
import time
from typing import Any
import uuid

import httpx
from langchain.agents import create_agent
from langchain_core.messages import HumanMessage
from langchain_core.messages.base import BaseMessage
from langchain_core.tools import StructuredTool
from langgraph.graph.state import CompiledStateGraph
from reactivex.disposable import Disposable

from dimos.agents.mcp.tool_stream import ToolStreamEvent
from dimos.agents.system_prompt import SYSTEM_PROMPT
from dimos.agents.utils import pretty_print_langchain_message
from dimos.core.core import rpc
from dimos.core.module import Module, ModuleConfig
from dimos.core.rpc_client import RPCClient
from dimos.core.stream import In, Out
from dimos.utils.logging_config import setup_logger
from dimos.utils.sequential_ids import SequentialIds

logger = setup_logger()


class McpClientConfig(ModuleConfig):
    system_prompt: str | None = SYSTEM_PROMPT
    model: str = "gpt-4o"
    model_fixture: str | None = None
    mcp_server_url: str = "http://localhost:9990/mcp"


class McpClient(Module[McpClientConfig]):
    default_config = McpClientConfig
    agent: Out[BaseMessage]
    human_input: In[str]
    agent_idle: Out[bool]

    _lock: RLock
    _state_graph: CompiledStateGraph[Any, Any, Any, Any] | None
    _message_queue: Queue[BaseMessage]
    _tool_registry: dict[str, dict[str, Any]]
    _history: list[BaseMessage]
    _thread: Thread
    _stop_event: Event
    _http_client: httpx.Client
    _seq_ids: SequentialIds
    _sse_thread: Thread | None
    _sse_client: httpx.Client | None

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._lock = RLock()
        self._state_graph = None
        self._message_queue = Queue()
        self._tool_registry = {}
        self._history = []
        self._thread = Thread(
            target=self._thread_loop,
            name=f"{self.__class__.__name__}-thread",
            daemon=True,
        )
        self._stop_event = Event()
        self._http_client = httpx.Client(timeout=120.0)
        self._seq_ids = SequentialIds()
        self._sse_thread = None
        self._sse_client = None

    def __reduce__(self) -> Any:
        return (self.__class__, (), {})

    def _mcp_request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": self._seq_ids.next(),
            "method": method,
        }
        if params is not None:
            body["params"] = params

        resp = self._http_client.post(self.config.mcp_server_url, json=body)
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            raise RuntimeError(f"MCP error {data['error']['code']}: {data['error']['message']}")

        result: dict[str, Any] = data.get("result")
        return result

    def _fetch_tools(self, timeout: float = 60.0, interval: float = 1.0) -> list[StructuredTool]:
        result = self._try_fetch_tools(timeout=timeout, interval=interval)
        if result is None:
            raise RuntimeError(
                f"Failed to fetch tools from MCP server {self.config.mcp_server_url}"
            )

        raw_tools = result.get("tools", [])
        self._tool_registry = {t["name"]: t for t in raw_tools}
        tools = [self._mcp_tool_to_langchain(t) for t in raw_tools]

        if not tools:
            logger.warning("No tools found from MCP server.")
        else:
            tool_names = [t.name for t in tools]
            logger.info("Discovered tools from MCP server.", tools=tool_names, n_tools=len(tools))

        return tools

    def _try_fetch_tools(self, timeout: float, interval: float) -> dict[str, Any] | None:
        deadline = time.monotonic() + timeout

        while True:
            try:
                self._mcp_request("initialize")
                break
            except (httpx.ConnectError, httpx.RemoteProtocolError):
                if time.monotonic() >= deadline:
                    return None
                time.sleep(interval)

        return self._mcp_request("tools/list")

    def _mcp_tool_to_langchain(self, mcp_tool: dict[str, Any]) -> StructuredTool:
        name = mcp_tool["name"]
        description = mcp_tool.get("description", "")
        input_schema = mcp_tool.get("inputSchema", {"type": "object", "properties": {}})

        def call_tool(**kwargs: Any) -> str:
            result = self._mcp_request("tools/call", {"name": name, "arguments": kwargs})
            content = result.get("content", [])
            parts = [c.get("text", "") for c in content if c.get("type") == "text"]
            text = "\n".join(parts)

            # Images need to be added to the history separately because they
            # cannot be included in the tool response for OpenAI models and
            # probably others.
            for item in content:
                if item.get("type") != "text":
                    uuid_ = str(uuid.uuid4())
                    text += f"Tool call started with UUID: {uuid_}. You will be updated with the result soon."
                    _append_image_to_history(self, name, uuid_, item)

            return text

        return StructuredTool(
            name=name,
            description=description,
            func=call_tool,
            args_schema=input_schema,
        )

    @rpc
    def start(self) -> None:
        super().start()

        def _on_human_input(string: str) -> None:
            self._message_queue.put(HumanMessage(content=string))

        self._disposables.add(Disposable(self.human_input.subscribe(_on_human_input)))

    @rpc
    def on_system_modules(self, _modules: list[RPCClient]) -> None:
        tools = self._fetch_tools()

        model: str | Any = self.config.model
        if self.config.model_fixture is not None:
            from dimos.agents.testing import MockModel

            model = MockModel(json_path=self.config.model_fixture)

        with self._lock:
            self._state_graph = create_agent(
                model=model,
                tools=tools,
                system_prompt=self.config.system_prompt,
            )
            self._thread.start()

        self._start_sse_listener()

    @rpc
    def stop(self) -> None:
        self._stop_event.set()
        with self._lock:
            if self._sse_client is not None:
                self._sse_client.close()
        if self._thread.is_alive():
            self._thread.join(timeout=2.0)
        if self._sse_thread is not None and self._sse_thread.is_alive():
            self._sse_thread.join(timeout=2.0)
        self._http_client.close()
        super().stop()

    @rpc
    def add_message(self, message: BaseMessage) -> None:
        self._message_queue.put(message)

    @rpc
    def dispatch_continuation(
        self, continuation: dict[str, Any], continuation_context: dict[str, Any]
    ) -> None:
        """Execute a tool continuation with detection data, bypassing the LLM.

        Called by trigger tools (e.g. look_out_for) to immediately invoke a
        follow-up tool when a detection fires, without waiting for the LLM to
        reason about the next action.

        Args:
            continuation: ``{"tool": "<name>", "args": {…}}`` — the tool to
                call and its arguments.  Argument values that are strings
                starting with ``$`` are treated as template variables and
                resolved against *continuation_context* (e.g. ``"$bbox"``).
            continuation_context: runtime detection data, e.g.
                ``{"bbox": [x1, y1, x2, y2], "label": "person"}``.
        """
        tool_name = continuation.get("tool")
        if not tool_name:
            self._message_queue.put(
                HumanMessage(f"Continuation failed: missing 'tool' key in {continuation}")
            )
            return

        if tool_name not in self._tool_registry:
            self._message_queue.put(
                HumanMessage(f"Continuation failed: tool '{tool_name}' not found")
            )
            return

        tool_args: dict[str, Any] = dict(continuation.get("args", {}))

        # Substitute $-prefixed template variables from continuation_context
        for key, value in tool_args.items():
            if isinstance(value, str) and value.startswith("$"):
                context_key = value[1:]
                if context_key in continuation_context:
                    tool_args[key] = continuation_context[context_key]

        try:
            result = self._mcp_request("tools/call", {"name": tool_name, "arguments": tool_args})
            content = result.get("content", [])
            parts = [c.get("text", "") for c in content if c.get("type") == "text"]
            text = "\n".join(parts)
        except Exception as e:
            self._message_queue.put(
                HumanMessage(f"Continuation '{tool_name}' failed with error: {e}")
            )
            return

        label = continuation_context.get("label", "unknown")
        self._message_queue.put(
            HumanMessage(
                f"Automatically executed '{tool_name}' as a continuation of lookout "
                f"detection (detected: {label}). Result: {text or 'started'}"
            )
        )

    def _thread_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                message = self._message_queue.get(timeout=0.5)
            except Empty:
                continue

            with self._lock:
                if not self._state_graph:
                    raise ValueError("No state graph initialized")
                self._process_message(self._state_graph, message)

    def _process_message(
        self, state_graph: CompiledStateGraph[Any, Any, Any, Any], message: BaseMessage
    ) -> None:
        self.agent_idle.publish(False)
        self._history.append(message)
        pretty_print_langchain_message(message)
        self.agent.publish(message)

        for update in state_graph.stream({"messages": self._history}, stream_mode="updates"):
            for node_output in update.values():
                for msg in node_output.get("messages", []):
                    self._history.append(msg)
                    pretty_print_langchain_message(msg)
                    self.agent.publish(msg)

        if self._message_queue.empty():
            self.agent_idle.publish(True)

    def _start_sse_listener(self) -> None:
        """Connect to the MCP server SSE endpoint to receive tool stream updates."""
        self._sse_thread = Thread(target=self._sse_loop, name="McpClient-SSE", daemon=True)
        self._sse_thread.start()

    def _sse_loop(self) -> None:
        base_url = self.config.mcp_server_url.rsplit("/mcp", 1)[0]
        sse_url = f"{base_url}/mcp/streams"

        while not self._stop_event.is_set():
            try:
                self._sse_connect(sse_url)
            except Exception:
                if not self._stop_event.is_set():
                    # Try reconnecting after a short delay
                    time.sleep(1.0)

    def _sse_connect(self, sse_url: str) -> None:
        client = httpx.Client(timeout=None)
        with self._lock:
            self._sse_client = client
        try:
            with client.stream("GET", sse_url) as response:
                self._sse_consume(response)
        finally:
            with self._lock:
                self._sse_client = None
            client.close()

    def _sse_consume(self, response: httpx.Response) -> None:
        for line in response.iter_lines():
            if self._stop_event.is_set():
                return
            if not line.startswith("data: "):
                continue
            try:
                data = json.loads(line[6:])
            except json.JSONDecodeError:
                continue
            event = ToolStreamEvent(**data)
            if event.type == "update":
                self._message_queue.put(
                    HumanMessage(
                        content=f"[Tool stream update from '{event.tool_name}']: {event.text}"
                    )
                )


def _append_image_to_history(
    mcp_client: McpClient, func_name: str, uuid_: str, result: Any
) -> None:
    mcp_client.add_message(
        HumanMessage(
            content=[
                {
                    "type": "text",
                    "text": f"This is the artefact for the '{func_name}' tool with UUID:={uuid_}.",
                },
                result,
            ]
        )
    )
