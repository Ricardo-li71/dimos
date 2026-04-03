#!/usr/bin/env python3

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

"""DimSim simulator connection.

Launches the dimsim CLI subprocess and communicates via LCM.
Implements Go2ConnectionProtocol so it can be used as a drop-in
replacement for MujocoConnection or UnitreeWebRTCConnection.
"""

import atexit
from collections.abc import Callable
import functools
import math
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time
from typing import Any, TypeVar

import lcm as lcm_mod
from reactivex import Observable
from reactivex.abc import ObserverBase, SchedulerBase
from reactivex.disposable import Disposable

from dimos.core.global_config import GlobalConfig
from dimos.msgs.geometry_msgs.PoseStamped import PoseStamped
from dimos.msgs.geometry_msgs.Twist import Twist
from dimos.msgs.geometry_msgs.Vector3 import Vector3
from dimos.msgs.sensor_msgs.CameraInfo import CameraInfo
from dimos.msgs.sensor_msgs.Image import Image
from dimos.msgs.sensor_msgs.PointCloud2 import PointCloud2
from dimos.utils.logging_config import setup_logger

logger = setup_logger()

T = TypeVar("T")

# DimSim virtual camera parameters.
_WIDTH = 960
_HEIGHT = 432
_FOV_DEG = 80

# Stream polling frequencies.
_VIDEO_FPS = 20
_ODOM_FPS = 50
_LIDAR_FPS = 2


def _find_dimsim() -> str:
    """Find the dimsim binary: ~/.dimsim/bin/dimsim then PATH."""
    home_bin = Path.home() / ".dimsim" / "bin" / "dimsim"
    if home_bin.exists():
        return str(home_bin)
    path_bin = shutil.which("dimsim")
    if path_bin:
        return path_bin
    raise FileNotFoundError(
        "dimsim not found. Install it from https://github.com/Antim-Labs/DimSim "
        "or place the binary in ~/.dimsim/bin/dimsim"
    )


class DimSimConnection:
    """DimSim simulator connection that runs as a subprocess and communicates via LCM."""

    def __init__(self, global_config: GlobalConfig) -> None:
        self.global_config = global_config
        self.process: subprocess.Popen[bytes] | None = None

        self._lcm: lcm_mod.LCM | None = None
        self._lcm_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._is_cleaned_up = False

        self._stream_threads: list[threading.Thread] = []
        self._stop_events: list[threading.Event] = []
        self._stop_timer: threading.Timer | None = None

        # Latest messages from LCM callbacks + sequence counters.
        self._latest_odom: PoseStamped | None = None
        self._latest_image: Image | None = None
        self._latest_lidar: PointCloud2 | None = None
        self._odom_seq = 0
        self._image_seq = 0
        self._lidar_seq = 0
        self._last_odom_seq = 0
        self._last_image_seq = 0
        self._last_lidar_seq = 0

    @staticmethod
    def _compute_camera_info() -> CameraInfo:
        fov_rad = math.radians(_FOV_DEG)
        fx = (_WIDTH / 2) / math.tan(fov_rad / 2)
        fy = fx
        cx = _WIDTH / 2.0
        cy = _HEIGHT / 2.0

        return CameraInfo(
            frame_id="camera_optical",
            height=_HEIGHT,
            width=_WIDTH,
            distortion_model="plumb_bob",
            D=[0.0, 0.0, 0.0, 0.0, 0.0],
            K=[fx, 0.0, cx, 0.0, fy, cy, 0.0, 0.0, 1.0],
            R=[1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0],
            P=[fx, 0.0, cx, 0.0, 0.0, fy, cy, 0.0, 0.0, 0.0, 1.0, 0.0],
            binning_x=0,
            binning_y=0,
        )

    camera_info_static: CameraInfo = _compute_camera_info()

    # ── Lifecycle ──────────────────────────────────────────────────────

    @staticmethod
    def _kill_port_holder(port: int) -> None:
        """Kill any process listening on the given port."""
        try:
            result = subprocess.run(
                ["lsof", "-ti", f":{port}"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            pids = result.stdout.strip()
            if pids:
                for pid in pids.splitlines():
                    logger.info(f"Killing stale process {pid} on port {port}")
                    subprocess.run(["kill", pid], timeout=5)
                time.sleep(0.5)
        except Exception as e:
            logger.warning(f"Failed to check/kill port {port}: {e}")

    @staticmethod
    def _ensure_scene(dimsim_bin: str, scene: str) -> None:
        """Run dimsim setup and scene install (skips if already cached)."""
        logger.info("Checking dimsim core assets...")
        subprocess.run([dimsim_bin, "setup"], check=True)
        logger.info(f"Checking dimsim scene '{scene}'...")
        subprocess.run([dimsim_bin, "scene", "install", scene], check=True)

    def _start_log_reader(self) -> None:
        """Read subprocess stdout/stderr and log them."""
        assert self.process is not None

        def _reader(stream: subprocess.PIPE, label: str) -> None:  # type: ignore[valid-type]
            if stream is None:
                return
            for raw in stream:
                line = raw.decode("utf-8", errors="replace").rstrip()
                if line:
                    logger.info(f"[dimsim {label}] {line}")

        for stream, label in [
            (self.process.stdout, "out"),
            (self.process.stderr, "err"),
        ]:
            t = threading.Thread(target=_reader, args=(stream, label), daemon=True)
            t.start()

    def start(self) -> None:
        dimsim_bin = _find_dimsim()
        scene = self.global_config.dimsim_scene
        port = self.global_config.dimsim_port

        self._ensure_scene(dimsim_bin, scene)
        self._kill_port_holder(port)

        render = os.environ.get("DIMSIM_RENDER", "gpu").strip()
        cmd = [
            dimsim_bin,
            "dev",
            "--scene",
            scene,
            "--port",
            str(port),
            "--headless",
            "--render",
            render,
        ]

        logger.info(f"Starting DimSim: {' '.join(cmd)}")
        try:
            self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except Exception as e:
            raise RuntimeError(f"Failed to start DimSim subprocess: {e}") from e

        self._start_log_reader()
        self._start_lcm_listener()

        # Wait for first odom message as readiness signal.
        timeout = 60.0
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.process.poll() is not None:
                exit_code = self.process.returncode
                stderr = ""
                if self.process.stderr:
                    stderr = self.process.stderr.read().decode(errors="replace")
                self.stop()
                raise RuntimeError(f"DimSim process exited early (code {exit_code})\n{stderr}")
            if self._odom_seq > 0:
                logger.info("DimSim process started successfully")
                atexit.register(self._atexit_cleanup)
                return
            time.sleep(0.1)

        self.stop()
        raise RuntimeError("DimSim process failed to start (timeout waiting for odom)")

    def _atexit_cleanup(self) -> None:
        self.stop()

    def stop(self) -> None:
        if self._is_cleaned_up:
            return
        self._is_cleaned_up = True

        if self._stop_timer:
            self._stop_timer.cancel()
            self._stop_timer = None

        self._stop_event.set()

        for ev in self._stop_events:
            ev.set()
        for t in self._stream_threads:
            if t.is_alive():
                t.join(timeout=2.0)

        if self._lcm_thread and self._lcm_thread.is_alive():
            self._lcm_thread.join(timeout=2.0)

        if self.process:
            if self.process.stderr:
                self.process.stderr.close()
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning("DimSim process did not stop gracefully, killing")
                self.process.kill()
                self.process.wait(timeout=2)
            except Exception as e:
                logger.error(f"Error stopping DimSim process: {e}")
            self.process = None

        self._lcm = None
        self._stream_threads.clear()
        self._stop_events.clear()

        self.lidar_stream.cache_clear()
        self.odom_stream.cache_clear()
        self.video_stream.cache_clear()

    # ── LCM listener ──────────────────────────────────────────────────

    def _start_lcm_listener(self) -> None:
        from dimos.protocol.service.lcmservice import autoconf

        autoconf()

        lc = lcm_mod.LCM()
        self._lcm = lc

        # Subscribe to everything and dispatch by channel name. This avoids
        # issues with exact-match vs regex and with type-suffixed channel names
        # (e.g. "/odom#geometry_msgs.PoseStamped").
        lc.subscribe(".*", self._on_lcm_message)

        def loop() -> None:
            while not self._stop_event.is_set():
                try:
                    if self._lcm is not None:
                        self._lcm.handle_timeout(50)
                except Exception as e:
                    logger.error(f"LCM handle error: {e}")

        self._lcm_thread = threading.Thread(target=loop, daemon=True)
        self._lcm_thread.start()

    def _on_lcm_message(self, channel: str, data: bytes) -> None:
        # Strip type suffix if present (e.g. "/odom#geometry_msgs.PoseStamped" → "/odom")
        base = channel.split("#")[0]

        try:
            if base == "/odom":
                self._latest_odom = PoseStamped.lcm_decode(data)
                self._odom_seq += 1
            elif base == "/color_image":
                self._latest_image = Image.lcm_decode(data)
                self._image_seq += 1
            elif base == "/lidar":
                self._latest_lidar = PointCloud2.lcm_decode(data)
                self._lidar_seq += 1
            elif self._odom_seq == 0:
                # Debug: log channels we see before odom arrives
                logger.info(f"LCM '{channel}' ({len(data)} bytes)")
        except Exception as e:
            logger.error(f"Failed to decode {channel}: {e}")

    # ── Observable streams ────────────────────────────────────────────

    def _create_stream(
        self,
        getter: Callable[[], T | None],
        frequency: float,
        stream_name: str,
    ) -> Observable[T]:
        def on_subscribe(observer: ObserverBase[T], _scheduler: SchedulerBase | None) -> Disposable:
            if self._is_cleaned_up:
                observer.on_completed()
                return Disposable(lambda: None)

            stop_event = threading.Event()
            self._stop_events.append(stop_event)

            def run() -> None:
                try:
                    while not stop_event.is_set() and not self._is_cleaned_up:
                        data = getter()
                        if data is not None:
                            observer.on_next(data)
                        time.sleep(1 / frequency)
                except Exception as e:
                    logger.error(f"{stream_name} stream error: {e}")
                finally:
                    observer.on_completed()

            thread = threading.Thread(target=run, daemon=True)
            self._stream_threads.append(thread)
            thread.start()

            def dispose() -> None:
                stop_event.set()

            return Disposable(dispose)

        return Observable(on_subscribe)

    @functools.cache
    def lidar_stream(self) -> Observable[PointCloud2]:
        def getter() -> PointCloud2 | None:
            if self._lidar_seq > self._last_lidar_seq:
                self._last_lidar_seq = self._lidar_seq
                return self._latest_lidar
            return None

        return self._create_stream(getter, _LIDAR_FPS, "Lidar")

    @functools.cache
    def odom_stream(self) -> Observable[PoseStamped]:
        def getter() -> PoseStamped | None:
            if self._odom_seq > self._last_odom_seq:
                self._last_odom_seq = self._odom_seq
                return self._latest_odom
            return None

        return self._create_stream(getter, _ODOM_FPS, "Odom")

    @functools.cache
    def video_stream(self) -> Observable[Image]:
        def getter() -> Image | None:
            if self._image_seq > self._last_image_seq:
                self._last_image_seq = self._image_seq
                return self._latest_image
            return None

        return self._create_stream(getter, _VIDEO_FPS, "Video")

    # ── Movement ──────────────────────────────────────────────────────

    def move(self, twist: Twist, duration: float = 0.0) -> bool:
        if self._is_cleaned_up or self._lcm is None:
            return True

        self._lcm.publish("/cmd_vel", twist.lcm_encode())

        if duration > 0:
            if self._stop_timer:
                self._stop_timer.cancel()

            def stop_movement() -> None:
                if self._lcm is not None:
                    stop_twist = Twist(
                        linear=Vector3(0, 0, 0),
                        angular=Vector3(0, 0, 0),
                    )
                    self._lcm.publish("/cmd_vel", stop_twist.lcm_encode())
                self._stop_timer = None

            self._stop_timer = threading.Timer(duration, stop_movement)
            self._stop_timer.daemon = True
            self._stop_timer.start()

        return True

    # ── Stubs ─────────────────────────────────────────────────────────

    def standup(self) -> bool:
        return True

    def liedown(self) -> bool:
        return True

    def balance_stand(self) -> bool:
        return True

    def set_obstacle_avoidance(self, enabled: bool = True) -> None:
        pass

    def publish_request(self, topic: str, data: dict[str, Any]) -> dict[Any, Any]:
        return {}
