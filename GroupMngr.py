"""Registro de peers. Um registro por id de peer."""
import socket
import threading
from common import recv_json, send_json
from constMP import GROUPMNGR_TCP_PORT, N

members = {}
lock = threading.Lock()


def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('0.0.0.0', GROUPMNGR_TCP_PORT))
        srv.listen()
        print('Group manager na porta', GROUPMNGR_TCP_PORT, flush=True)
        while True:
            conn, addr = srv.accept()
            with conn:
                try:
                    req = recv_json(conn)
                    with lock:
                        if req['op'] == 'register':
                            pid = int(req['id'])
                            if not 0 <= pid < N:
                                raise ValueError('ID invalido')
                            members[pid] = {'id': pid, 'host': req.get('host') or addr[0],
                                            'udp_port': int(req['udp_port']),
                                            'tcp_port': int(req['tcp_port'])}
                            response = {'ok': True, 'members': len(members)}
                        elif req['op'] == 'list':
                            response = {'ok': True, 'peers': [members[k] for k in sorted(members)]}
                        else:
                            response = {'ok': False, 'error': 'operacao desconhecida'}
                    send_json(conn, response)
                except Exception as exc:
                    print('Erro:', exc, flush=True)


if __name__ == '__main__':
    main()
