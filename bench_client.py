#!/usr/bin/env python3
"""
Benchmark client: opens N concurrent TCP chat connections, registers each,
sends /ping <token> M times and measures the application-level RTT.

Appends rows to --out CSV:  kind,host,client,seq,value_ms
   kind=setup -> connection + registration time (-1 = failed)
   kind=ping  -> RTT in ms (-1 = lost / timeout)
Prints a one-line JSON summary on stdout.
"""
import argparse
import json
import socket
import statistics
import threading
import time

perf = time.perf_counter


class LineReader:
    def __init__(self, sock):
        self.s, self.buf = sock, b''

    def readline(self, timeout):
        deadline = perf() + timeout
        while b'\n' not in self.buf:
            remaining = deadline - perf()
            if remaining <= 0:
                return None
            self.s.settimeout(remaining)
            try:
                d = self.s.recv(4096)
            except socket.timeout:
                return None
            if not d:
                raise ConnectionError('closed')
            self.buf += d
        line, self.buf = self.buf.split(b'\n', 1)
        return line.decode('utf-8', 'replace').strip()


def wait_for(reader, predicate, timeout):
    """Read lines until predicate(line) is true; return seconds taken or None."""
    t0 = perf()
    while True:
        remaining = timeout - (perf() - t0)
        if remaining <= 0:
            return None
        line = reader.readline(remaining)
        if line is None:
            return None
        if predicate(line):
            return perf() - t0


def client_worker(idx, a, rows, rlock):
    name = f'{a.prefix}_{idx}'
    t0 = perf()
    try:
        s = socket.create_connection((a.server, a.port), timeout=5)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        r = LineReader(s)
        if wait_for(r, lambda l: l.startswith('REGISTER'), 5) is None:
            raise ConnectionError('no prompt')
        s.sendall(f'{name}\n'.encode())
        if wait_for(r, lambda l: l.startswith('WELCOME'), 5) is None:
            raise ConnectionError('no welcome')
    except (OSError, ConnectionError):
        with rlock:
            rows.append(('setup', idx, 0, -1))
        return
    with rlock:
        rows.append(('setup', idx, 0, (perf() - t0) * 1000))

    for seq in range(-3, a.pings):            # first 3 pings are warm-up
        token = f'{idx}-{seq}'
        t = perf()
        try:
            s.sendall(f'/ping {token}\n'.encode())
            dt = wait_for(r, lambda l: l == f'PONG {token}', a.timeout)
        except (OSError, ConnectionError):
            dt = None
        if seq >= 0:
            with rlock:
                rows.append(('ping', idx, seq, -1 if dt is None else dt * 1000))
        time.sleep(a.interval)
    try:
        s.sendall(b'/quit\n')
        s.close()
    except OSError:
        pass


def pct(vals, p):
    vals = sorted(vals)
    return vals[min(len(vals) - 1, int(round(p / 100 * (len(vals) - 1))))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('server')
    ap.add_argument('--port', type=int, default=6666)
    ap.add_argument('--clients', type=int, default=5)
    ap.add_argument('--pings', type=int, default=50)
    ap.add_argument('--interval', type=float, default=0.1)
    ap.add_argument('--timeout', type=float, default=3.0)
    ap.add_argument('--prefix', default='bench')
    ap.add_argument('--label', default='run')
    ap.add_argument('--out')
    a = ap.parse_args()

    rows, rlock = [], threading.Lock()
    start = perf()
    threads = []
    for i in range(a.clients):
        t = threading.Thread(target=client_worker, args=(i, a, rows, rlock))
        t.start()
        threads.append(t)
        time.sleep(0.02)
    for t in threads:
        t.join()
    duration = perf() - start

    if a.out:
        text = ''.join(f'{k},{a.prefix},{c},{s},{v:.3f}\n' for k, c, s, v in rows)
        with open(a.out, 'a') as f:
            f.write(text)  # single write -> safe with parallel hosts

    pings = [v for k, _, _, v in rows if k == 'ping']
    ok = [v for v in pings if v >= 0]
    setup = [v for k, _, _, v in rows if k == 'setup' and v >= 0]
    summary = {
        'label': a.label, 'clients': a.clients, 'sent': len(pings), 'ok': len(ok),
        'lost': len(pings) - len(ok),
        'mean_ms': round(statistics.mean(ok), 3) if ok else None,
        'p95_ms': round(pct(ok, 95), 3) if ok else None,
        'max_ms': round(max(ok), 3) if ok else None,
        'setup_ms': round(statistics.mean(setup), 3) if setup else None,
        'msgs_per_s': round(len(ok) / duration, 2),
    }
    print(json.dumps(summary))


if __name__ == '__main__':
    main()
