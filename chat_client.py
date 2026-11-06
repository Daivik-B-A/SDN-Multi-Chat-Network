#!/usr/bin/env python3
"""
Interactive chat client.

  python3 chat_client.py 10.0.0.1 --user alice [--reconnect]

--reconnect : if the server dies/restarts, keep retrying, re-register
              and rejoin the last room (service-failure scenario).
"""
import argparse
import os
import socket
import threading
import time


class ChatClient:
    def __init__(self, host, port, user, reconnect):
        self.host, self.port, self.user, self.reconnect = host, port, user, reconnect
        self.sock = None
        self.room = None
        self.registered = False

    def connect(self):
        while True:
            try:
                s = socket.create_connection((self.host, self.port), timeout=5)
                s.settimeout(None)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.sock = s
                self.registered = False
                return
            except OSError:
                if not self.reconnect:
                    print('[client] cannot connect to server')
                    os._exit(1)
                print('[client] server unreachable, retrying in 2s...')
                time.sleep(2)

    def send(self, text):
        try:
            self.sock.sendall((text + '\n').encode())
        except OSError:
            pass

    def handle(self, line):
        if line.startswith('REGISTER:'):
            self.send(self.user)
        elif line.startswith('ERROR:') and not self.registered:
            print(line)
            os._exit(1)
        elif line.startswith('WELCOME'):
            self.registered = True
            print(line)
            if self.room and self.room != 'Lobby':
                self.send(f'/join {self.room}')
        else:
            print(f'\r{line}\n> ', end='', flush=True)

    def receiver(self):
        buf = b''
        while True:
            try:
                data = self.sock.recv(4096)
            except OSError:
                data = b''
            if not data:
                if not self.reconnect:
                    print('\n[client] disconnected from server.')
                    os._exit(0)
                print('\n[client] connection lost, reconnecting...')
                buf = b''
                self.connect()
                continue
            buf += data
            while b'\n' in buf:
                line, buf = buf.split(b'\n', 1)
                self.handle(line.decode('utf-8', 'replace').strip())

    def run(self):
        self.connect()
        threading.Thread(target=self.receiver, daemon=True).start()
        try:
            while True:
                msg = input('> ').strip()
                if not msg:
                    continue
                if msg.startswith('/join '):
                    self.room = msg.split(' ', 1)[1].strip()
                self.send(msg)
                if msg == '/quit':
                    break
        except (EOFError, KeyboardInterrupt):
            pass
        finally:
            try:
                self.sock.close()
            except OSError:
                pass


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('host', nargs='?', default='10.0.0.1')
    ap.add_argument('--port', type=int, default=6666)
    ap.add_argument('--user')
    ap.add_argument('--reconnect', action='store_true')
    a = ap.parse_args()
    user = a.user or input('Username: ').strip()
    ChatClient(a.host, a.port, user, a.reconnect).run()
