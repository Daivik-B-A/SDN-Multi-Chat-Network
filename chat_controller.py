"""
POX controller for the SDN-managed multi-client chat network (OpenFlow 1.0).

Launch:
  ./pox.py log.level --INFO openflow.of_01 chat_controller --mode=sdn
  ./pox.py log.level --INFO openflow.of_01 chat_controller --mode=baseline

Modes
  baseline : plain POX L2 learning switch (no queues, no policy)  -> comparison baseline
  sdn      : own L2 switch + dynamic behaviours:
     1. Classification  : TCP port 6666 = chat traffic, everything else = background
     2. Prioritisation  : chat -> OVS queue 1, background -> queue 0, VIP-room users -> queue 2
     3. User/room policy: updated live from the chat server (Unix datagram socket)
                          --vip=<rooms>      rooms that get the premium queue
                          --blocked=<users>  users whose chat traffic is dropped
     4. Monitoring      : every <interval> s poll flow stats; a host sending more than
                          <threshold> bytes of chat traffic per interval is blocked
                          for <block_time> s (drop rule).
"""
import json
import os
import socket
import threading
import time

from pox.core import core
import pox.openflow.libopenflow_01 as of
from pox.lib.packet.ethernet import ethernet
from pox.lib.addresses import IPAddr
from pox.lib.recoco import Timer
from pox.forwarding.l2_learning import LearningSwitch

log = core.getLogger()

CHAT_PORT = 6666
CTRL_SOCK = '/tmp/chat_ctrl.sock'
EVENT_LOG = '/tmp/sdn_events.csv'

Q_BG, Q_CHAT, Q_VIP = 0, 1, 2                 # OVS queue ids (see topology/topo.py)
PRIO_BG, PRIO_CHAT, PRIO_DROP = 10, 100, 200  # flow priorities


class ChatController(object):
    def __init__(self, mode, threshold, interval, block_time, blocked_users, vip_rooms):
        self.mode = mode
        self.threshold = threshold
        self.interval = interval
        self.block_time = block_time
        self.blocked_users = blocked_users
        self.vip_rooms = vip_rooms

        self.switches = {}       # dpid -> connection
        self.mac_to_port = {}    # dpid -> {mac: port}
        self.policy = {}         # ip -> {'user':..., 'room':...}
        self.blocked = {}        # ip -> expiry (epoch, inf = permanent)
        self.prev_bytes = {}     # flow key -> last byte_count

        self._init_log()
        core.openflow.addListeners(self)
        threading.Thread(target=self._ctrl_listener, daemon=True).start()
        if mode == 'sdn':
            Timer(self.interval, self._request_stats, recurring=True)
        log.info('=== Chat SDN controller started: mode=%s threshold=%dB/%ds block=%ds vip=%s blocked=%s ===',
                 mode, threshold, interval, block_time, sorted(vip_rooms), sorted(blocked_users))

    # ------------------------------------------------------------------ logging
    def _init_log(self):
        with open(EVENT_LOG, 'w') as f:
            f.write('ts,event,ip,detail\n')

    def _log_event(self, kind, ip, detail=''):
        with open(EVENT_LOG, 'a') as f:
            f.write('%.6f,%s,%s,%s\n' % (time.time(), kind, ip, detail.replace(',', ';')))

    # ------------------------------------------------- app -> controller channel
    def _ctrl_listener(self):
        try:
            os.unlink(CTRL_SOCK)
        except OSError:
            pass
        s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        s.bind(CTRL_SOCK)
        os.chmod(CTRL_SOCK, 0o777)
        while True:
            data = s.recv(4096)
            try:
                msg = json.loads(data.decode())
            except ValueError:
                continue
            core.callLater(self._on_app_event, msg)   # hop back onto the POX thread

    def _on_app_event(self, msg):
        ev, ip, user, room = msg.get('event'), msg.get('ip'), msg.get('user'), msg.get('room')
        if ev == 'RESET':
            self.blocked.clear()
            self.prev_bytes.clear()
            self.policy.clear()
            for con in self.switches.values():
                con.send(of.ofp_flow_mod(command=of.OFPFC_DELETE))
            self._init_log()
            log.info('controller state reset')
        elif ev in ('JOIN', 'ROOM_CHANGE') and ip:
            old = self.policy.get(ip)
            self.policy[ip] = {'user': user, 'room': room}
            log.info('[APP EVENT] %s user=%s ip=%s room=%s', ev, user, ip, room)
            if user in self.blocked_users and ip not in self.blocked:
                self._block(ip, 'policy: user %s is blocked' % user, 0)
            was_vip = bool(old and old['room'] in self.vip_rooms)
            if self.mode == 'sdn' and was_vip != (room in self.vip_rooms):
                log.info('[POLICY] %s queue class changed -> reinstalling flows', ip)
                self._flush_host(ip)
        elif ev == 'LEAVE' and ip:
            old = self.policy.pop(ip, None)
            log.info('[APP EVENT] LEAVE user=%s ip=%s', user, ip)
            if self.mode == 'sdn' and old and old['room'] in self.vip_rooms:
                self._flush_host(ip)

    # ---------------------------------------------------------- switch events
    def _handle_ConnectionUp(self, event):
        self.switches[event.dpid] = event.connection
        self.mac_to_port[event.dpid] = {}
        if self.mode == 'baseline':
            LearningSwitch(event.connection, False)
            log.info('Switch %s connected -> BASELINE L2 learning', event.dpid)
        else:
            event.connection.send(of.ofp_flow_mod(command=of.OFPFC_DELETE))
            log.info('Switch %s connected -> SDN priority/policy mode', event.dpid)

    def _handle_ConnectionDown(self, event):
        self.switches.pop(event.dpid, None)

    def _handle_PacketIn(self, event):
        if self.mode != 'sdn':
            return
        packet = event.parsed
        if not packet.parsed or packet.effective_ethertype == ethernet.LLDP_TYPE:
            return
        table = self.mac_to_port.setdefault(event.dpid, {})
        table[packet.src] = event.port
        if packet.dst.is_multicast:
            return self._flood(event)
        out = table.get(packet.dst)
        if out is None:
            return self._flood(event)
        if out == event.port:
            return
        self._install(event, packet, out)

    def _flood(self, event):
        msg = of.ofp_packet_out(data=event.ofp, in_port=event.port)
        msg.actions.append(of.ofp_action_output(port=of.OFPP_FLOOD))
        event.connection.send(msg)

    # ---------------------------------------- classification + prioritisation
    def _install(self, event, packet, out_port):
        ip = packet.find('ipv4')
        tcp = packet.find('tcp')
        is_chat = bool(tcp and CHAT_PORT in (tcp.srcport, tcp.dstport))

        if ip and is_chat and str(ip.srcip) in self.blocked:
            self._install_drop(event.connection, str(ip.srcip), self._remaining(str(ip.srcip)))
            return

        if is_chat:
            qid = Q_CHAT
            for addr in (str(ip.srcip), str(ip.dstip)):        # client side of the flow
                p = self.policy.get(addr)
                if p and p['room'] in self.vip_rooms:
                    qid = Q_VIP
        elif ip:
            qid = Q_BG
        else:
            qid = Q_CHAT                                      # ARP etc. = control traffic

        fm = of.ofp_flow_mod()
        fm.match = of.ofp_match.from_packet(packet, event.port)
        fm.priority = PRIO_CHAT if is_chat else PRIO_BG
        fm.idle_timeout = 10
        fm.actions.append(of.ofp_action_enqueue(port=out_port, queue_id=qid))
        fm.data = event.ofp                                   # also forwards the buffered packet
        event.connection.send(fm)

    # ------------------------------------------------ blocking / flow helpers
    def _remaining(self, ip):
        exp = self.blocked.get(ip, 0)
        if exp == float('inf'):
            return 0
        return max(1, int(exp - time.time()))

    def _install_drop(self, con, ip, timeout):
        fm = of.ofp_flow_mod()                                # no actions = drop
        fm.priority = PRIO_DROP
        fm.hard_timeout = timeout
        fm.match.dl_type = 0x0800
        fm.match.nw_proto = 6
        fm.match.nw_src = IPAddr(ip)
        fm.match.tp_dst = CHAT_PORT
        con.send(fm)

    def _block(self, ip, reason, duration):
        self.blocked[ip] = time.time() + duration if duration else float('inf')
        for con in self.switches.values():
            con.send(of.ofp_flow_mod(command=of.OFPFC_DELETE,
                                     match=of.ofp_match(dl_type=0x0800, nw_src=IPAddr(ip))))
            self._install_drop(con, ip, int(duration))
        self._log_event('BLOCK', ip, reason)
        log.warning('[SDN BLOCK] %s blocked for %s: %s', ip,
                    ('%ds' % duration) if duration else 'ever', reason)

    def _flush_host(self, ip):
        """Delete all flows of a host so they are re-installed with the new queue."""
        for con in self.switches.values():
            for field in ('nw_src', 'nw_dst'):
                m = of.ofp_match(dl_type=0x0800, **{field: IPAddr(ip)})
                con.send(of.ofp_flow_mod(command=of.OFPFC_DELETE, match=m))
            if ip in self.blocked:
                self._install_drop(con, ip, self._remaining(ip))

    # ------------------------------------------------------------- monitoring
    def _request_stats(self):
        now = time.time()
        for ip, exp in list(self.blocked.items()):
            if exp <= now:
                del self.blocked[ip]
                self._log_event('UNBLOCK', ip, 'timeout')
                log.info('[SDN UNBLOCK] %s', ip)
        for con in self.switches.values():
            con.send(of.ofp_stats_request(body=of.ofp_flow_stats_request()))

    def _handle_FlowStatsReceived(self, event):
        if self.mode != 'sdn':
            return
        per_src = {}
        for f in event.stats:
            m = f.match
            if f.priority != PRIO_CHAT or m.nw_src is None or m.tp_dst != CHAT_PORT:
                continue                                      # only client -> server chat flows
            src = str(m.nw_src)
            key = (event.dpid, src, m.tp_src)
            prev = self.prev_bytes.get(key, 0)
            delta = f.byte_count - prev if f.byte_count >= prev else f.byte_count
            self.prev_bytes[key] = f.byte_count
            per_src[src] = per_src.get(src, 0) + delta
        for src, nbytes in per_src.items():
            if nbytes > 100000:
                log.debug('[MONITOR] %s sent %d B in last interval', src, nbytes)
            if nbytes > self.threshold and src not in self.blocked:
                self._block(src, 'flooding: %d B in %ds (threshold %d)' %
                            (nbytes, self.interval, self.threshold), self.block_time)


def launch(mode='sdn', threshold=1000000, interval=2, block_time=30, blocked='', vip='vip'):
    core.registerNew(ChatController, mode, int(threshold), int(interval), int(block_time),
                     set(x for x in str(blocked).split(',') if x),
                     set(x for x in str(vip).split(',') if x))
