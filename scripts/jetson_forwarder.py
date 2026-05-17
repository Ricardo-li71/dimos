#!/usr/bin/env python3
"""
TCP port forwarder for the Unitree Go2 deployment.

It maps a Jetson-accessible local port to the motion board WebRTC port.
Adjust TARGET_HOST and FORWARDS for the actual robot network.
"""

import socket
import sys
import threading


TARGET_HOST = "192.168.123.161"

FORWARDS = [
    (8080, 9991),
]


def forward(src: socket.socket, dst: socket.socket) -> None:
    try:
        while True:
            data = src.recv(4096)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        src.close()
        dst.close()


def handle(client_sock: socket.socket, target_host: str, target_port: int) -> None:
    try:
        remote_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        remote_sock.connect((target_host, target_port))
    except OSError as exc:
        print(f"[forwarder] Cannot connect to {target_host}:{target_port}: {exc}")
        client_sock.close()
        return

    threading.Thread(target=forward, args=(client_sock, remote_sock), daemon=True).start()
    threading.Thread(target=forward, args=(remote_sock, client_sock), daemon=True).start()


def listen(local_port: int, target_port: int) -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", local_port))
    server.listen(50)
    print(f"[forwarder] 0.0.0.0:{local_port} -> {TARGET_HOST}:{target_port}")

    while True:
        client, _addr = server.accept()
        threading.Thread(target=handle, args=(client, TARGET_HOST, target_port), daemon=True).start()


def main() -> int:
    threads = []
    for local_port, target_port in FORWARDS:
        thread = threading.Thread(target=listen, args=(local_port, target_port), daemon=False)
        thread.start()
        threads.append(thread)

    print("[forwarder] running, press Ctrl+C to stop")
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("[forwarder] stopped")
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
