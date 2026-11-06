#!/usr/bin/env python3
"""
Runtime packet-loss control.

topo.py starts a LossControl server (Unix stream socket /tmp/chat_loss.sock).
Anyone can change the loss percentage while the app is running:
  * the web page (slider/number box -> gateway.py -> this socket)
  * the command line:  python3 topology/set_loss.py 25
Protocol (one line each way):
  GET          -> OK <pct>
  LOSS <pct>   -> OK <pct>   (0 = no loss ... 100 = every packet dropped)
  anything bad -> ERR <reason>
"""
import os
import socket
import threading

LOSS_SOCK = '/tmp/chat_loss.sock'


def parse_pct(text):
    """Accepts any number 0..100 (e.g. 0, 5, 12.5, 100). Raises ValueError otherwise."""
    v = float(str(text).strip().rstrip('%'))
    if v != v or v < 0 or v > 100:
        raise ValueError('loss must be a number between 0 and 100')
    return round(v, 3)


def netem_cmd(dev, pct):
    """tc command that makes <dev> drop pct% of the packets it transmits."""
    if pct <= 0:
        return ['tc', 'qdisc', 'del', 'dev', dev, 'root']            # remove impairment
    return ['tc', 'qdisc', 'replace', 'dev', dev, 'root', 'netem', 'loss', '%g%%' % pct]


class LossControl(object):
    def __init__(self, apply_fn, path=LOSS_SOCK):
        self.apply_fn = apply_fn      # apply_fn(pct) performs the real change
        self.path = path
        self.pct = 0.0
        self.lock = threading.Lock()
        self.srv = None

    def set(self, pct):
        pct = parse_pct(pct)
        with self.lock:
            self.apply_fn(pct)
            self.pct = pct
        return pct

    def _handle(self, conn):
        with conn:
            try:
                line = conn.makefile('rb').readline(200).decode('utf-8', 'replace').strip()
                cmd, _, arg = line.partition(' ')
                if cmd.upper() == 'GET':
                    reply = 'OK %g' % self.pct
                elif cmd.upper() == 'LOSS':
                    reply = 'OK %g' % self.set(arg)
                else:
                    reply = 'ERR unknown command'
            except ValueError as e:
                reply = 'ERR %s' % e
            except Exception as e:                      # keep the server alive
                reply = 'ERR %s' % e
            try:
                conn.sendall((reply + '\n').encode())
            except OSError:
                pass

    def start(self):
        try:
            os.unlink(self.path)
        except OSError:
            pass
        self.srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.srv.bind(self.path)
        os.chmod(self.path, 0o777)
        self.srv.listen(8)

        def loop():
            while True:
                try:
                    conn, _ = self.srv.accept()
                except OSError:
                    return
                threading.Thread(target=self._handle, args=(conn,), daemon=True).start()
        threading.Thread(target=loop, daemon=True).start()

    def stop(self):
        try:
            if self.srv:
                self.srv.close()
            os.unlink(self.path)
        except OSError:
            pass


def request(cmd, path=LOSS_SOCK, timeout=15):
    """Client helper: send one command, return the reply line."""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect(path)
    s.sendall((cmd + '\n').encode())
    data = s.makefile('rb').readline().decode().strip()
    s.close()
    return data
