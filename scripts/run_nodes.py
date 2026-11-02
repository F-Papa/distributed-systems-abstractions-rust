#!/usr/bin/env python3
"""Launch multiple instances of the distributed-systems Rust binary."""

from __future__ import annotations

import argparse
import multiprocessing
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


BINARY = "distributed-systems-abstractions-rust"


def run_node(
    node_id: int,
    node_count: int,
    base_port: int,
    output_dir: str,
    project_dir: str,
) -> None:
    """Run one Rust node and send both output streams to its log file."""
    log_path = Path(output_dir) / f"node_{node_id}.log"
    proposal_value = node_id
    command = [
        "cargo",
        "run",
        "--bin",
        BINARY,
        "--",
        str(base_port),
        str(node_id),
        str(node_count),
    ]
    environment = os.environ.copy()
    environment["RUST_LOG"] = "debug"

    with log_path.open("w", encoding="utf-8", buffering=1) as log_file:
        log_file.write(f"proposal_value={proposal_value}\n")
        child = subprocess.Popen(
            command,
            cwd=project_dir,
            env=environment,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE,
            start_new_session=True,
        )

        assert child.stdin is not None
        child.stdin.write(f"{proposal_value}\n".encode())
        child.stdin.flush()

        def stop_child(_signum: int, _frame: object) -> None:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
            raise SystemExit(128 + _signum)

        signal.signal(signal.SIGTERM, stop_child)
        signal.signal(signal.SIGINT, stop_child)

        try:
            return_code = child.wait()
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)

    if return_code != 0:
        raise SystemExit(return_code)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "node_count",
        type=int,
        help="number of nodes to launch",
    )
    parser.add_argument(
        "--base-port",
        type=int,
        default=4000,
        help="base UDP port passed to every node (default: 4000)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="directory for node logs (default: output)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.node_count < 1:
        raise SystemExit("node_count must be at least 1")
    if not 0 <= args.base_port <= 65535:
        raise SystemExit("base_port must be between 0 and 65535")
    if args.base_port + 1000 + args.node_count > 65535:
        raise SystemExit("base_port is too high for the requested number of nodes")

    project_dir = Path(__file__).resolve().parent.parent
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    processes = [
        multiprocessing.Process(
            target=run_node,
            name=f"node-{node_id}",
            args=(
                node_id,
                args.node_count,
                args.base_port,
                str(output_dir),
                str(project_dir),
            ),
        )
        for node_id in range(1, args.node_count + 1)
    ]

    for process in processes:
        process.start()

    try:
        while any(process.is_alive() for process in processes):
            time.sleep(0.2)
    except KeyboardInterrupt:
        print("Stopping nodes...", file=sys.stderr)
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
        for process in processes:
            process.join()

    return max((process.exitcode or 0) for process in processes)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    raise SystemExit(main())
