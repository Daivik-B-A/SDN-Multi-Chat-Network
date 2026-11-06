#!/usr/bin/env python3
"""
Flooding / abusive client used for the "challenging scenario".
Registers, moves to room 'spam' (so it does not spam other rooms) and sends
large messages as fast as possible. Writes the timestamp of the first
flood message to --stamp so the experiment can compute controller reaction time.
"""
import argparse
import socket
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('server')
    ap.add_argument('--port', type=int, default=6666)
    ap.add_argument('--user', default='flooder')
    ap.add_argument('--room', default='spam')
    ap.add_argument('--size', type=int, default=900)
    ap.add_argument('--duration', type=float, default=30)
    ap.add_argument('--stamp', default='/tmp/flood_start')
    a = ap.parse_args()

    s = socket.create_connection((a.server, a.port), timeout=5)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.recv(4096)                                   # REGISTER prompt
    s.sendall(f'{a.user}\n'.encode())
    time.sleep(0.3)
    s.sendall(f'/join {a.room}\n'.encode())
    time.sleep(0.3)
    try:
        s.recv(4096)
    except OSError:
        pass

    payload = (b'X' * a.size) + b'\n'
    s.settimeout(1.0)
    started = time.time()
    with open(a.stamp, 'w') as f:
        f.write(f'{started:.6f}')
    sent = 0
    try:
        while time.time() - started < a.duration:
            try:
                sent += s.send(payload)
            except socket.timeout:
                pass                                # blocked by the SDN controller
            except OSError:
                break
    except KeyboardInterrupt:
        pass
    print(f'flood finished, {sent} bytes queued')
    s.close()


if __name__ == '__main__':
    main()
