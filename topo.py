#!/usr/bin/env python3
"""
Mininet topology: one OVS switch (s1), remote POX controller.

  h1   10.0.0.1    chat server (port s1-eth1 = 10 Mbps bottleneck, 3 OVS queues)
  h2.. 10.0.0.2..  chat clients
  hbg  10.0.0.100  background-traffic host (iperf)
  hf   10.0.0.101  flooding / abusive client

Every switch port gets an OVS linux-htb QoS with three queues:
  q0 background (prio 2, min 10%)   q1 chat (prio 1, min 50%)   q2 VIP room (prio 0, min 30%)
The same queues exist in baseline and sdn mode; only the controller decides
whether packets use them (sdn: enqueue by class, baseline: default queue 0).

Usage:  sudo python3 topology/topo.py --clients 5 [--auto-server] [--web] [--loss 10]
"""
import argparse
import os
import subprocess

from mininet.cli import CLI
from mininet.link import Link
from mininet.log import setLogLevel, info
from mininet.net import Mininet
from mininet.node import Node, OVSKernelSwitch, RemoteController

import loss_control

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SERVER_RATE = 10_000_000       # bottleneck towards the server (bit/s)
CLIENT_RATE = 100_000_000      # all other ports
WEB_GW_IP = '10.0.0.254/24'    # root-namespace address used by the browser gateway
WEB_POOL = (200, 249)          # source IPs 10.0.0.200-249, one per web user


def sh(cmd):
    subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def clear_qos():
    sh('ovs-vsctl --all destroy qos -- --all destroy queue')


def setup_qos(port, rate):
    def q(frac, prio):
        return (f'other-config:min-rate={int(rate * frac)} '
                f'other-config:max-rate={rate} other-config:priority={prio}')
    sh(f'ovs-vsctl set port {port} qos=@qos -- '
       f'--id=@qos create qos type=linux-htb other-config:max-rate={rate} '
       f'queues:0=@q0 queues:1=@q1 queues:2=@q2 -- '
       f'--id=@q0 create queue {q(0.1, 2)} -- '
       f'--id=@q1 create queue {q(0.5, 1)} -- '
       f'--id=@q2 create queue {q(0.3, 0)}')


def apply_loss(net, pct):
    """Drop pct% of the packets *transmitted* by every chat host (and the gateway interface).
    A packet between a client and the server crosses exactly one of these egress
    interfaces per direction, so pct is the loss per direction. Background host hbg is excluded.
    netem is attached on the host side, so it does not interfere with the OVS queues."""
    targets = [(h.pid, h.defaultIntf().name) for h in net.hosts if h.name != 'hbg']
    if getattr(net, 'root_intf', None):
        targets.append((None, net.root_intf))
    for pid, dev in targets:
        cmd = loss_control.netem_cmd(dev, pct)
        if pid:
            cmd = ['mnexec', '-a', str(pid)] + cmd          # run inside the host's namespace
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def build_network(n_clients=5, ctrl_ip='127.0.0.1', ctrl_port=6633, web=False):
    net = Mininet(controller=RemoteController, switch=OVSKernelSwitch,
                  link=Link, autoSetMacs=True)
    net.addController('c0', controller=RemoteController, ip=ctrl_ip, port=ctrl_port)
    s1 = net.addSwitch('s1', protocols='OpenFlow10')

    server = net.addHost('h1', ip='10.0.0.1/24')
    net.addLink(server, s1)                               # first link -> s1-eth1
    clients = [net.addHost(f'h{i + 2}', ip=f'10.0.0.{i + 2}/24') for i in range(n_clients)]
    hbg = net.addHost('hbg', ip='10.0.0.100/24')
    hf = net.addHost('hf', ip='10.0.0.101/24')
    for h in clients + [hbg, hf]:
        net.addLink(h, s1)

    root_intf = None
    if web:   # attach the root namespace to s1 so the browser gateway can reach the network
        root = Node('root', inNamespace=False)
        rlink = net.addLink(root, s1)
        root.setIP(WEB_GW_IP, intf=rlink.intf1)
        root_intf = rlink.intf1.name
    net.root_intf = root_intf

    net.start()
    clear_qos()
    server_port = net.linksBetween(server, s1)[0].intf2.name
    for intf in s1.intfList():
        if intf.name == 'lo':
            continue
        setup_qos(intf.name, SERVER_RATE if intf.name == server_port else CLIENT_RATE)
    if web:
        sh(f'ip link set {root_intf} up')
        for i in range(WEB_POOL[0], WEB_POOL[1] + 1):
            sh(f'ip addr add 10.0.0.{i}/24 dev {root_intf}')
    return net


def start_server(net):
    net.get('h1').cmd(f'python3 {ROOT}/server/chat_server.py > /tmp/chat_server.log 2>&1 &')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--clients', type=int, default=5)
    ap.add_argument('--auto-server', action='store_true', help='start chat server on h1')
    ap.add_argument('--loss', default='0',
                    help='initial packet loss in percent (0-100); change later with set_loss.py or the web UI')
    ap.add_argument('--web', action='store_true',
                    help='connect the root namespace to s1 so web/gateway.py can serve browsers')
    a = ap.parse_args()
    setLogLevel('info')
    net = build_network(a.clients, web=a.web)
    loss = loss_control.LossControl(lambda p: apply_loss(net, p))
    loss.start()
    loss.set(a.loss)
    info('*** packet loss control ready (now %g%%): python3 topology/set_loss.py <percent>\n' % loss.pct)
    if a.auto_server:
        start_server(net)
        info('*** chat server started on h1 (log: /tmp/chat_server.log)\n')
    if a.web:
        info('*** web access ready. In another terminal run:\n'
             '    python3 web/gateway.py --server 10.0.0.1 --pool 10.0.0.200-10.0.0.249\n'
             '    then open http://localhost:8080\n')
    CLI(net)
    net.get('h1').cmd('pkill -f chat_server.py')
    loss.stop()
    net.stop()
    clear_qos()


if __name__ == '__main__':
    main()
