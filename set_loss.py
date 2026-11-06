#!/usr/bin/env python3
"""
Change the packet loss of the running Mininet network (topo.py must be running).

  python3 topology/set_loss.py 0       # no loss
  python3 topology/set_loss.py 12.5    # 12.5 % of packets dropped
  python3 topology/set_loss.py 100     # every packet dropped (total outage)
  python3 topology/set_loss.py         # show the current value
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import loss_control  # noqa: E402

try:
    reply = loss_control.request('GET' if len(sys.argv) < 2 else 'LOSS ' + sys.argv[1])
except OSError:
    sys.exit('loss control not reachable: is topology/topo.py running?')
print(reply)
sys.exit(0 if reply.startswith('OK') else 1)
