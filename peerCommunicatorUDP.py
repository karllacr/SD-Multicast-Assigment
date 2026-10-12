"""Peer MPComm: UDP confiavel por retransmissao e FIFO por emissor.

Execute um peer por maquina/container, usando --id fixo. O servidor de
comparacao prepara todos os peers antes de iniciar a rodada.
"""
import argparse
import json
import random
import socket
import threading
import time
from common import recv_json, send_json
from constMP import (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT, SERVER_ADDR,
                     SERVER_PORT, PEER_TCP_PORT, PEER_UDP_PORT, N,
                     RETRY_INTERVAL, EXPERIMENT_TIMEOUT)
from totalOrder import TotalOrder


class Peer:
    def __init__(self, pid, host, udp_port, tcp_port):
        self.pid, self.host = pid, host
        self.udp_port, self.tcp_port = udp_port, tcp_port
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind(('0.0.0.0', udp_port))
        self.udp.settimeout(0.15)
        self.tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp.bind(('0.0.0.0', tcp_port))
        self.tcp.listen(5)
        self.state_lock = threading.RLock()
        self.round = None
        self.active = False
        self.running = True
        self.finished = False
        self.sent_count = 0
        self.pending = {}  # (sender, wire_seq) -> {packet, confirmed}
        self.buffer = {}   # sender -> {wire_seq: packet}
        self.next_seq = {} # proximo numero FIFO por emissor
        self.next_wire = 0
        self.to = None
        self.peers = {}
        self.nmsgs = 0
        self.total = None
        self.sender_thread = None
        self.udp_thread = threading.Thread(target=self.udp_loop, daemon=True)
        self.udp_thread.start()
        self.retry_thread = threading.Thread(target=self.retry_loop, daemon=True)
        self.retry_thread.start()

    def send_udp(self, packet, dest):
        data = json.dumps(packet, separators=(',', ':')).encode()
        self.udp.sendto(data, (dest['host'], dest['udp_port']))
        self.sent_count += 1

    def multicast(self, kind, fields):
        with self.state_lock:
            ts = self.total.local_timestamp()
            seq = self.next_wire
            self.next_wire += 1
            packet = {'round': self.round, 'kind': kind, 'sender': self.pid,
                      'wire_seq': seq, 'ts': ts, **fields}
            self.pending[(self.pid, seq)] = {'packet': packet, 'confirmed': {self.pid}}
            # Processa localmente sem depender de loopback UDP.
            self.process_ordered(packet)
            for peer_id, dest in self.peers.items():
                if peer_id != self.pid:
                    self.send_udp(packet, dest)
            return packet

    def udp_loop(self):
        while self.running:
            try:
                data, _ = self.udp.recvfrom(65535)
                packet = json.loads(data.decode())
                with self.state_lock:
                    if not self.active or packet.get('round') != self.round:
                        continue
                    sender = packet.get('sender')
                    if sender not in self.peers or sender == self.pid:
                        continue
                    if packet.get('kind') == 'WIRE_ACK':
                        key = (self.pid, packet['ack_seq'])
                        if key in self.pending:
                            self.pending[key]['confirmed'].add(sender)
                            if len(self.pending[key]['confirmed']) == N:
                                del self.pending[key]
                        continue
                    if packet.get('kind') not in ('DATA', 'ORDER_ACK'):
                        continue
                    seq = packet['wire_seq']
                    self.send_udp({'round': self.round, 'kind': 'WIRE_ACK',
                                   'sender': self.pid, 'ack_seq': seq}, self.peers[sender])
                    if seq < self.next_seq[sender]:
                        continue
                    self.buffer[sender].setdefault(seq, packet)
                    while self.next_seq[sender] in self.buffer[sender]:
                        current = self.buffer[sender].pop(self.next_seq[sender])
                        self.next_seq[sender] += 1
                        self.total.observe(current['ts'])
                        self.process_ordered(current)
            except socket.timeout:
                pass
            except (OSError, ValueError, KeyError, TypeError) as exc:
                if self.running:
                    print(f'P{self.pid} UDP: {exc}', flush=True)

    def process_ordered(self, packet):
        if packet['kind'] == 'DATA':
            mid = self.total.add_data(packet)
            # ACK de ordenacao enviado a todos (incluindo emissor).
            self.multicast('ORDER_ACK', {'mid': list(mid)})
        elif packet['kind'] == 'ORDER_ACK':
            self.total.acknowledge(tuple(packet['mid']), packet['sender'])
        for mid, item in self.total.drain():
            print(f'P{self.pid} ENTREGA {item} (ordem {mid})', flush=True)

    def retry_loop(self):
        while self.running:
            time.sleep(RETRY_INTERVAL)
            with self.state_lock:
                if not self.active:
                    continue
                for entry in list(self.pending.values()):
                    for pid, dest in self.peers.items():
                        if pid != self.pid and pid not in entry['confirmed']:
                            self.send_udp(entry['packet'], dest)

    def send_data(self):
        for seq in range(self.nmsgs):
            time.sleep(random.uniform(.01, .099))
            with self.state_lock:
                self.multicast('DATA', {'seq': seq})
        deadline = time.monotonic() + EXPERIMENT_TIMEOUT
        expected = N * self.nmsgs
        while time.monotonic() < deadline:
            with self.state_lock:
                done = (len(self.total.delivered) == expected and not self.pending)
                if done:
                    result = {'op': 'result', 'round': self.round, 'id': self.pid,
                              'delivered': [list(m) for m in self.total.delivered],
                              'udp_sent': self.sent_count}
                    self.finished = True
                    break
            time.sleep(.05)
        else:
            with self.state_lock:
                result = {'op': 'result', 'round': self.round, 'id': self.pid,
                          'delivered': [list(m) for m in self.total.delivered],
                          'udp_sent': self.sent_count, 'timeout': True}
                self.finished = True
        try:
            with socket.create_connection((SERVER_ADDR, SERVER_PORT), timeout=10) as conn:
                send_json(conn, result)
        except OSError as exc:
            print('Falha ao enviar resultado:', exc, flush=True)

    def handle(self, request):
        op = request.get('op')
        with self.state_lock:
            if op == 'prepare':
                if self.sender_thread and self.sender_thread.is_alive():
                    return {'ok': False, 'error': 'rodada anterior ainda ativa'}
                peers = {int(p['id']): p for p in request['peers']}
                if len(peers) != N or self.pid not in peers:
                    return {'ok': False, 'error': 'lista de peers invalida'}
                self.peers = peers
                self.round = request['round']
                self.nmsgs = int(request['messages'])
                self.total = TotalOrder(self.pid, N)
                self.next_wire = 0
                self.next_seq = {pid: 0 for pid in peers}
                self.buffer = {pid: {} for pid in peers}
                self.pending = {}
                self.sent_count = 0
                self.active = True
                self.finished = False
                return {'ok': True}
            if op == 'start' and self.active and request.get('round') == self.round:
                if self.sender_thread and self.sender_thread.is_alive():
                    return {'ok': False, 'error': 'ja iniciado'}
                self.sender_thread = threading.Thread(target=self.send_data, daemon=True)
                self.sender_thread.start()
                return {'ok': True}
        return {'ok': False, 'error': 'comando invalido'}

    def serve(self):
        print(f'Peer P{self.pid} TCP {self.tcp_port} UDP {self.udp_port}', flush=True)
        while True:
            conn, _ = self.tcp.accept()
            with conn:
                try:
                    conn.settimeout(10)
                    request = recv_json(conn)
                    send_json(conn, self.handle(request))
                except Exception as exc:
                    print('Erro TCP:', exc, flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int, required=True)
    parser.add_argument('--host', default=None, help='IP/DNS acessivel pelos outros peers')
    parser.add_argument('--udp-port', type=int, default=PEER_UDP_PORT)
    parser.add_argument('--tcp-port', type=int, default=PEER_TCP_PORT)
    args = parser.parse_args()
    if not 0 <= args.id < N:
        parser.error(f'--id deve estar entre 0 e {N-1}')
    # Quando host nao e informado, GroupMngr registra IP de origem da conexao.
    peer = Peer(args.id, args.host, args.udp_port, args.tcp_port)
    with socket.create_connection((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT), timeout=10) as conn:
        send_json(conn, {'op': 'register', 'id': args.id, 'host': args.host,
                         'udp_port': args.udp_port, 'tcp_port': args.tcp_port})
        print('Registro:', recv_json(conn), flush=True)
    peer.serve()


if __name__ == '__main__':
    main()
