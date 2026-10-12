"""Protocolo simples de enquadramento para conexoes TCP."""
import json
import struct


def recvall(sock, count):
    result = bytearray()
    while len(result) < count:
        chunk = sock.recv(count - len(result))
        if not chunk:
            raise ConnectionError('Conexao TCP encerrada prematuramente')
        result.extend(chunk)
    return bytes(result)


def send_json(sock, obj):
    data = json.dumps(obj, separators=(',', ':')).encode('utf-8')
    sock.sendall(struct.pack('!I', len(data)) + data)


def recv_json(sock):
    length = struct.unpack('!I', recvall(sock, 4))[0]
    if length > 10_000_000:
        raise ValueError('Mensagem TCP excessivamente grande')
    return json.loads(recvall(sock, length).decode('utf-8'))
