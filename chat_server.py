#!/usr/bin/env python3
"""
Multi-client TCP chat server (thread per client).

Protocol: newline-terminated text lines over TCP (framing handled by read_lines).
Commands: /help /users /rooms /join <room> /msg <user> <text> /ping [token] /quit
Application events (JOIN / ROOM_CHANGE / LEAVE) are pushed to the SDN controller
over a Unix datagram socket so the controller can apply user/room policies.
"""
import argparse
import json
import re
import socket
import threading
import time

CTRL_SOCK = '/tmp/chat_ctrl.sock'
EVENT_LOG = '/tmp/chat_server_events.csv'
STATS_LOG = '/tmp/chat_server_stats.csv'
NAME_RE = re.compile(r'^[A-Za-z0-9_\-]{1,24}$')
MAX_LINE = 65536

lock = threading.RLock()
log_lock = threading.Lock()
clients = {}          # socket -> {'name', 'room', 'ip'}
lines_total = 0       # every processed line (for msgs/sec)

_ctl = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)


def notify_controller(event, user, ip, room):
    """Tell the SDN controller about an application-level event."""
    try:
        _ctl.sendto(json.dumps({'event': event, 'user': user, 'ip': ip,
                                'room': room}).encode(), CTRL_SOCK)
    except OSError:
        pass  # controller not running (e.g. plain baseline without POX)


def log_event(kind, user, ip, room):
    with log_lock:
        with open(EVENT_LOG, 'a') as f:
            f.write(f'{time.time():.6f},{kind},{user},{ip},{room}\n')


def send_line(sock, text):
    try:
        sock.sendall((text + '\n').encode('utf-8'))
        return True
    except OSError:
        return False


def read_lines(sock):
    """Generator that yields complete newline-terminated lines (TCP framing)."""
    buf = b''
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            return
        buf += chunk
        while b'\n' in buf:
            line, buf = buf.split(b'\n', 1)
            yield line.decode('utf-8', 'replace').strip()
        if len(buf) > MAX_LINE:
            return  # protect against clients that never send '\n'


def kill_socket(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)  # wakes the handler thread blocked in recv
    except OSError:
        pass


def broadcast(text, room, exclude=None):
    with lock:
        targets = [s for s, c in clients.items() if c['room'] == room and s is not exclude]
    for s in targets:
        if not send_line(s, text):
            kill_socket(s)


def remove_client(sock):
    with lock:
        info = clients.pop(sock, None)
    if info:
        broadcast(f"SYSTEM: {info['name']} left the chat.", info['room'])
        notify_controller('LEAVE', info['name'], info['ip'], info['room'])
        log_event('LEAVE', info['name'], info['ip'], info['room'])
        print(f"[-] {info['name']} ({info['ip']}) disconnected")
    try:
        sock.close()
    except OSError:
        pass


def handle_command(sock, info, line):
    """Returns False if the client asked to quit."""
    name, room = info['name'], info['room']
    cmd, _, rest = line.partition(' ')
    rest = rest.strip()

    if cmd == '/quit':
        return False
    if cmd == '/ping':
        send_line(sock, f'PONG {rest}'.rstrip())
    elif cmd == '/help':
        send_line(sock, 'Commands: /users /rooms /join <room> /msg <user> <text> /ping /quit')
    elif cmd == '/users':
        with lock:
            lst = ', '.join(f"{c['name']}({c['room']})" for c in clients.values())
        send_line(sock, f'SYSTEM: Online: {lst}')
    elif cmd == '/rooms':
        with lock:
            counts = {}
            for c in clients.values():
                counts[c['room']] = counts.get(c['room'], 0) + 1
        send_line(sock, 'SYSTEM: Rooms: ' + ', '.join(f'{r}({n})' for r, n in sorted(counts.items())))
    elif cmd == '/join':
        if not NAME_RE.match(rest):
            send_line(sock, 'SYSTEM: usage: /join <room> (letters/digits/_/-)')
        else:
            broadcast(f'SYSTEM: {name} left for {rest}.', room, exclude=sock)
            with lock:
                info['room'] = rest
            send_line(sock, f"SYSTEM: Joined room '{rest}'")
            broadcast(f'SYSTEM: {name} joined.', rest, exclude=sock)
            notify_controller('ROOM_CHANGE', name, info['ip'], rest)
            log_event('ROOM_CHANGE', name, info['ip'], rest)
    elif cmd == '/msg':
        target, _, text = rest.partition(' ')
        if not target or not text.strip():
            send_line(sock, 'SYSTEM: usage: /msg <user> <text>')
        else:
            with lock:
                tsock = next((s for s, c in clients.items() if c['name'] == target), None)
            if tsock is None:
                send_line(sock, f"SYSTEM: User '{target}' not found.")
            elif send_line(tsock, f'[PM from {name}]: {text}'):
                send_line(sock, f'[PM to {target}]: {text}')
            else:
                send_line(sock, 'SYSTEM: Error sending private message.')
    else:
        send_line(sock, 'SYSTEM: unknown command, try /help')
    return True


def handle_client(sock, addr):
    global lines_total
    ip = addr[0]
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    send_line(sock, 'REGISTER: Enter username:')
    name = None
    try:
        lines = read_lines(sock)
        for line in lines:                       # --- registration phase
            if line == '/quit':
                return
            if not NAME_RE.match(line):
                send_line(sock, 'ERROR: invalid username (1-24 chars: letters, digits, _ -). Try again:')
                continue
            with lock:
                taken = any(c['name'] == line for c in clients.values())
                if not taken:
                    clients[sock] = {'name': line, 'room': 'Lobby', 'ip': ip}
            if taken:
                send_line(sock, 'ERROR: username taken. Try again:')
                continue
            name = line
            break
        if name is None:
            return

        info = clients[sock]
        send_line(sock, f'WELCOME {name}')
        print(f'[+] {name} connected from {ip}')
        notify_controller('JOIN', name, ip, 'Lobby')
        log_event('JOIN', name, ip, 'Lobby')
        broadcast(f'SYSTEM: {name} joined.', 'Lobby', exclude=sock)

        for line in lines:                       # --- chat phase
            if not line:
                continue
            with lock:
                lines_total += 1
            if line.startswith('/'):
                if not handle_command(sock, info, line):
                    break
            else:
                broadcast(f'[{name}]: {line}', info['room'], exclude=sock)
    except OSError:
        pass
    finally:
        remove_client(sock)


def stats_loop(period=5):
    last = 0
    with open(STATS_LOG, 'w') as f:
        f.write('ts,clients,lines_total,lines_per_s\n')
    while True:
        time.sleep(period)
        with lock:
            n, total = len(clients), lines_total
        rate = (total - last) / period
        last = total
        with log_lock:
            with open(STATS_LOG, 'a') as f:
                f.write(f'{time.time():.3f},{n},{total},{rate:.1f}\n')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default='0.0.0.0')
    ap.add_argument('--port', type=int, default=6666)
    args = ap.parse_args()

    with open(EVENT_LOG, 'w') as f:
        f.write('ts,event,user,ip,room\n')

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((args.host, args.port))
    srv.listen(128)
    threading.Thread(target=stats_loop, daemon=True).start()
    print(f'[*] Chat server listening on {args.host}:{args.port}')
    try:
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        print('\n[*] shutting down')


if __name__ == '__main__':
    main()
