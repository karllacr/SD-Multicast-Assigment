"""Multicast totalmente ordenado com relogios de Lamport e ACKs.

Cada DATA recebe identificador (timestamp, emissor). Todos os peers enviam
ACK multicast apos receber DATA. A entrega da cabeca da fila requer ACKs
DE TODOS os membros. Cada ACK e enviado depois de processar DATA e os
canais por emissor sao FIFO (numero de sequencia + buffering), de modo
que ACK de um peer prova que todas as DATA anteriores daquele peer ja
foram vistas. Retransmissao e deduplicacao asseguram progresso sob perdas
transitorias, mas falhas permanentes bloqueiam a entrega.
"""
import threading


class TotalOrder:
    def __init__(self, peer_id, n_peers):
        self.peer_id = peer_id
        self.n_peers = n_peers
        self.clock = 0
        self.queue = {}  # (lamport, sender) -> (sender, local_seq)
        self.acks = {}   # id -> conjunto de peers
        self.delivered = []
        self.lock = threading.RLock()

    def local_timestamp(self):
        with self.lock:
            self.clock += 1
            return self.clock

    def observe(self, timestamp):
        with self.lock:
            self.clock = max(self.clock, timestamp) + 1
            return self.clock

    def add_data(self, packet):
        mid = (packet['ts'], packet['sender'])
        with self.lock:
            self.queue.setdefault(mid, (packet['sender'], packet['seq']))
            self.acks.setdefault(mid, set())
        return mid

    def acknowledge(self, mid, peer):
        with self.lock:
            self.acks.setdefault(mid, set()).add(peer)

    def drain(self):
        newly = []
        with self.lock:
            while self.queue:
                mid = min(self.queue)
                if len(self.acks.get(mid, set())) < self.n_peers:
                    break
                item = self.queue.pop(mid)
                self.acks.pop(mid, None)
                self.delivered.append(item)
                newly.append((mid, item))
        return newly
