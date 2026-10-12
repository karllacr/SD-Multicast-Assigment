"""Inicia uma rodada e compara logs de entrega de todos os peers."""
import argparse
import socket
import time
from collections import Counter
from common import recv_json, send_json
from constMP import (SERVER_PORT, GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT,
                     PEER_TCP_PORT, N, EXPERIMENT_TIMEOUT)


def rpc(host, port, request, timeout=10):
    with socket.create_connection((host, port), timeout=timeout) as sock:
        send_json(sock, request)
        return recv_json(sock)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--messages', type=int, default=10)
    parser.add_argument('--round', default='run1')
    args = parser.parse_args()
    if args.messages < 1:
        parser.error('--messages deve ser >= 1')
    peers = rpc(GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT, {'op': 'list'})['peers']
    if len(peers) != N:
        raise RuntimeError(f'Esperados {N} peers, encontrados {len(peers)}')
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('0.0.0.0', SERVER_PORT))
        srv.listen(N)
        srv.settimeout(EXPERIMENT_TIMEOUT)
        for peer in peers:
            reply = rpc(peer['host'], peer['tcp_port'],
                        {'op': 'prepare', 'round': args.round, 'messages': args.messages,
                         'peers': peers})
            if not reply.get('ok'):
                raise RuntimeError(reply)
        print('Peers preparados. Iniciando rodada.', flush=True)
        start = time.monotonic()
        for peer in peers:
            reply = rpc(peer['host'], peer['tcp_port'],
                        {'op': 'start', 'round': args.round})
            if not reply.get('ok'):
                raise RuntimeError(reply)
        logs = {}
        while len(logs) < N:
            conn, _ = srv.accept()
            with conn:
                conn.settimeout(10)
                msg = recv_json(conn)
                if msg.get('round') == args.round and msg.get('op') == 'result':
                    logs[msg['id']] = msg
                    print('Resultado recebido de P', msg['id'], flush=True)
        elapsed = time.monotonic() - start
    reference = [tuple(msg) for msg in logs[0]['delivered']]
    expected = N * args.messages
    ok = True
    total_divergences = 0
    print('\n========== RESULTADO DA COMPARACAO ==========')
    print('Rodada:', args.round)
    print('Mensagens esperadas por peer:', expected)
    print('Duracao global (s):', round(elapsed, 4))

    for p in range(N):
        delivered = [tuple(msg) for msg in logs[p]['delivered']]
        # Conta posicoes diferentes em relacao a P0; posicoes ausentes
        # tambem contam como divergencia.
        divergences = sum(
            i >= len(reference) or i >= len(delivered)
            or reference[i] != delivered[i]
            for i in range(max(len(reference), len(delivered)))
        )
        # Mensagens ausentes e duplicadas sao reportadas separadamente.
        missing = sum((Counter(reference) - Counter(delivered)).values())
        extra = sum((Counter(delivered) - Counter(reference)).values())
        total_divergences += divergences
        if len(delivered) != expected or delivered != reference:
            ok = False
        print(f'P{p}: {len(delivered)} entregues, '
              f'{logs[p]["udp_sent"]} datagramas UDP transmitidos, '
              f'{divergences} posicoes divergentes em relacao a P0, '
              f'{missing} ausentes e {extra} extras em relacao a P0')

    print('Total de posicoes divergentes (P1..P5 vs P0):', total_divergences)
    print('Totalmente ordenado:', ok)
    print('=============================================')
    if not ok:
        raise SystemExit('ERRO: divergencia ou mensagens ausentes')



if __name__ == '__main__':
    main()
