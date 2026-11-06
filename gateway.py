#!/usr/bin/env python3
"""
Browser gateway for the SDN chat app (Python standard library only).

  * serves web/index.html on  http://<host>:8080/
  * bridges each browser WebSocket (/ws?user=NAME) to one real TCP connection
    to the chat server, speaking the normal line protocol.

Because every browser user gets a real TCP connection that enters the Mininet
switch, the SDN controller classifies/prioritises/blocks web users exactly like
terminal clients.  With --pool every web user is bound to its own source IP
(10.0.0.200-249 by default when topo.py is started with --web), so per-user
policies and the flood monitor work per person.

Run (after `sudo python3 topology/topo.py --web --auto-server`):
    python3 web/gateway.py --server 10.0.0.1 --pool 10.0.0.200-10.0.0.249
Local test without Mininet:
    python3 server/chat_server.py &   python3 web/gateway.py --server 127.0.0.1
"""
import argparse
import asyncio
import base64
import hashlib
import ipaddress
import os
import re
import socket
from urllib.parse import parse_qs, urlparse

GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
HERE = os.path.dirname(os.path.abspath(__file__))
MAX_FRAME = 1 << 20
NAME_RE = re.compile(r'^[A-Za-z0-9_\-]{1,24}$')
SESSIONS = set()          # writers of all open browser sessions (for loss broadcasts)


class IpPool:
    def __init__(self, spec):
        self.free = []
        if spec:
            a, b = spec.split('-')
            lo, hi = int(ipaddress.ip_address(a)), int(ipaddress.ip_address(b))
            self.free = [str(ipaddress.ip_address(i)) for i in range(lo, hi + 1)]

    def get(self):
        return self.free.pop(0) if self.free else None

    def put(self, ip):
        if ip:
            self.free.append(ip)


async def loss_request(path, cmd):
    """Ask topo.py's loss-control socket to GET or set the packet loss."""
    try:
        r, w = await asyncio.wait_for(asyncio.open_unix_connection(path), 3)
        w.write((cmd + '\n').encode())
        await w.drain()
        line = await asyncio.wait_for(r.readline(), 20)
        w.close()
        return line.decode().strip() or 'ERR empty reply'
    except (OSError, asyncio.TimeoutError):
        return 'ERR not available (start the topology with topology/topo.py)'


async def handle_loss_command(text, writer, args):
    """Browser sent '!loss <pct>' (set) or '!loss?' (get). Never forwarded to the chat server."""
    arg = text[5:].strip()
    setting = arg not in ('', '?')
    reply = await loss_request(args.loss_sock, 'LOSS ' + arg if setting else 'GET')
    out = 'LOSS ' + reply[3:].strip() if reply.startswith('OK ') else 'LOSSERR ' + reply[4:].strip()
    targets = list(SESSIONS) if (setting and reply.startswith('OK ')) else [writer]
    for w in targets:                      # a successful change is shown in every open tab
        try:
            await ws_send_text(w, out)
        except OSError:
            pass


# ---------------------------------------------------------------- WebSocket
def ws_frame(opcode, payload=b''):
    n = len(payload)
    head = bytes([0x80 | opcode])
    if n < 126:
        head += bytes([n])
    elif n < 65536:
        head += bytes([126]) + n.to_bytes(2, 'big')
    else:
        head += bytes([127]) + n.to_bytes(8, 'big')
    return head + payload


async def ws_read_frame(reader):
    b1, b2 = await reader.readexactly(2)
    fin, opcode = b1 & 0x80, b1 & 0x0F
    masked, length = b2 & 0x80, b2 & 0x7F
    if length == 126:
        length = int.from_bytes(await reader.readexactly(2), 'big')
    elif length == 127:
        length = int.from_bytes(await reader.readexactly(8), 'big')
    if length > MAX_FRAME:
        raise ValueError('frame too large')
    mask = await reader.readexactly(4) if masked else None
    payload = await reader.readexactly(length)
    if mask:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload, bool(fin)


async def ws_send_text(writer, text):
    writer.write(ws_frame(0x1, text.encode('utf-8')))
    await writer.drain()


async def ws_session(reader, writer, headers, url, args, pool):
    key = headers.get('sec-websocket-key')
    user = parse_qs(url.query).get('user', [''])[0]
    if not key:
        writer.write(b'HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n')
        return
    accept = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
    writer.write(('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n'
                  'Connection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n' % accept).encode())
    await writer.drain()

    if not NAME_RE.match(user):
        await ws_send_text(writer, 'ERROR: invalid username (1-24 chars: letters, digits, _ -)')
        writer.write(ws_frame(0x8))
        return

    src = pool.get()
    loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setblocking(False)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    try:
        if src:
            sock.bind((src, 0))
        await asyncio.wait_for(loop.sock_connect(sock, (args.server, args.port)), 12)
        treader, twriter = await asyncio.open_connection(sock=sock)
    except (OSError, asyncio.TimeoutError):
        sock.close()
        pool.put(src)
        await ws_send_text(writer, 'ERROR: cannot reach chat server %s:%d' % (args.server, args.port))
        writer.write(ws_frame(0x8))
        return
    SESSIONS.add(writer)
    print('[gw] %s connected (source ip %s)' % (user, src or 'default'))

    async def tcp_to_ws():
        while True:
            try:
                line = await treader.readline()
            except (ValueError, asyncio.LimitOverrunError):
                break
            if not line:
                break
            text = line.decode('utf-8', 'replace').rstrip('\r\n')
            if text.startswith('REGISTER:'):
                twriter.write((user + '\n').encode())      # auto-register with the chosen name
                continue
            await ws_send_text(writer, text)

    async def ws_to_tcp():
        buf = b''
        while True:
            op, payload, fin = await ws_read_frame(reader)
            if op == 0x8:
                break
            if op == 0x9:
                writer.write(ws_frame(0xA, payload))
                continue
            if op == 0xA:
                continue
            buf = payload if op in (0x1, 0x2) else buf + payload
            if fin:
                text = buf.decode('utf-8', 'replace').replace('\r', ' ').replace('\n', ' ')[:2000]
                buf = b''
                if text.startswith('!loss'):
                    await handle_loss_command(text, writer, args)
                    continue
                twriter.write((text + '\n').encode('utf-8'))
                await twriter.drain()

    tasks = [asyncio.ensure_future(tcp_to_ws()), asyncio.ensure_future(ws_to_tcp())]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in tasks:
            t.cancel()
        try:
            twriter.close()
        except OSError:
            pass
        try:
            writer.write(ws_frame(0x8))
        except OSError:
            pass
        SESSIONS.discard(writer)
        pool.put(src)
        print('[gw] %s disconnected' % user)


# --------------------------------------------------------------------- HTTP
async def handle(reader, writer, args, pool):
    try:
        head = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), 10)
        lines = head.decode('latin-1').split('\r\n')
        method, target, _ = lines[0].split(' ', 2)
        headers = {}
        for l in lines[1:]:
            if ':' in l:
                k, v = l.split(':', 1)
                headers[k.strip().lower()] = v.strip()
        url = urlparse(target)
        if headers.get('upgrade', '').lower() == 'websocket' and url.path == '/ws':
            await ws_session(reader, writer, headers, url, args, pool)
        elif method == 'GET' and url.path in ('/', '/index.html'):
            with open(os.path.join(HERE, 'index.html'), 'rb') as f:
                body = f.read()
            writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n'
                         b'Cache-Control: no-store\r\nContent-Length: ' + str(len(body)).encode() +
                         b'\r\nConnection: close\r\n\r\n' + body)
        else:
            writer.write(b'HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
        await writer.drain()
    except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError,
            ValueError, asyncio.LimitOverrunError, OSError):
        pass
    finally:
        try:
            writer.close()
        except OSError:
            pass


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--server', default='10.0.0.1', help='chat server IP')
    ap.add_argument('--port', type=int, default=6666, help='chat server TCP port')
    ap.add_argument('--listen', default='0.0.0.0')
    ap.add_argument('--http-port', type=int, default=8080)
    ap.add_argument('--loss-sock', default='/tmp/chat_loss.sock', help='packet-loss control socket (made by topo.py)')
    ap.add_argument('--pool', default='', help='source-IP pool, e.g. 10.0.0.200-10.0.0.249')
    args = ap.parse_args()
    pool = IpPool(args.pool)
    srv = await asyncio.start_server(lambda r, w: handle(r, w, args, pool),
                                     args.listen, args.http_port)
    print('[gw] open http://localhost:%d  ->  chat server %s:%d  (source pool: %s)' %
          (args.http_port, args.server, args.port, args.pool or 'none'))
    async with srv:
        await srv.serve_forever()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
