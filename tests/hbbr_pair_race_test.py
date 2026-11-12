#!/usr/bin/env python3
"""A/B harness for the hbbr relay pairing race (rustdesk-server#708).

Opens two TCP legs per pair, sends the same RequestRelay uuid on both (back to back, or `gap_s`
apart), then checks that a byte written on one leg arrives on the other. A build with the race
loses some simultaneous pairs; a fixed build relays all of them. Adapted from the harness in
https://github.com/rustdesk/rustdesk-server/issues/708 (2026).

Usage: hbbr_pair_race_test.py <port> <pairs> <gap_s> <non-loopback-host-ip>
Loopback connections reach hbbr's admin console instead of the relay, hence the host IP.
"""
import socket
import sys
import time
import uuid

LEG_SETTLE_S = 0.02
PAIR_SETTLE_S = 0.1
RECV_TIMEOUT_S = 1.0
PROBE = b"ping-from-a"


def request_relay_frame(pair_id: bytes) -> bytes:
    """Encode RendezvousMessage{request_relay{uuid}} with hbb_common's 1-byte length header."""
    inner = b"\x12" + bytes([len(pair_id)]) + pair_id
    msg = b"\x92\x01" + bytes([len(inner)]) + inner
    return bytes([len(msg) << 2]) + msg


def pair_relays(host: str, port: int, gap_s: float) -> bool:
    """Return True when one pair of legs gets paired and relays a probe from a to b."""
    frame = request_relay_frame(str(uuid.uuid4()).encode())
    leg_a = socket.create_connection((host, port))
    leg_b = socket.create_connection((host, port))
    try:
        for leg in (leg_a, leg_b):
            leg.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        time.sleep(LEG_SETTLE_S)
        leg_a.send(frame)
        if gap_s:
            time.sleep(gap_s)
        leg_b.send(frame)
        time.sleep(PAIR_SETTLE_S)
        leg_a.send(PROBE)
        leg_b.settimeout(RECV_TIMEOUT_S)
        return leg_b.recv(64) == PROBE
    except OSError:
        return False
    finally:
        leg_a.close()
        leg_b.close()


def main() -> None:
    port, pairs, gap_s, host = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]), sys.argv[4]
    relayed = sum(pair_relays(host, port, gap_s) for _ in range(pairs))
    print(f"port {port} gap={gap_s}: paired+relayed {relayed}/{pairs}", flush=True)


if __name__ == "__main__":
    main()
