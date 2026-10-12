# MPComm — multicast totalmente ordenado (Lamport)

Adaptacao didatica do MPComm original. O transporte entre peers permanece UDP; o sistema usa sequencias por emissor, buffer FIFO, ACKs de transporte e retransmissoes para perdas transitorias. Para ordenacao total, cada mensagem DATA recebe chave `(timestamp_Lamport, id_emissor)`; cada peer difunde `ORDER_ACK` apos receber DATA; uma mensagem so e entregue quando e a menor pendente e recebeu ACK de todos. Os ACKs passam pelo mesmo canal FIFO por emissor, assegurando que mensagens anteriores de cada emissor foram processadas antes de se considerar a entrega segura.

## Arquivos
- `constMP.py`: enderecos, portas, quantidade de peers e timeout.
- `GroupMngr.py`: cadastro de membros.
- `comparisonServer.py`: inicio das rodadas, coleta e comparacao dos logs.
- `peerCommunicatorUDP.py`: peers UDP, FIFO, retransmissoes, Lamport e relatorios.
- `totalOrder.py`: fila de ordenacao total e ACKs logicos.
- `common.py`: transporte de comandos e logs TCP com enquadramento JSON.
- `README-original.md`: texto original do professor.

## Teste local (3 peers)

Em `constMP.py`, use `N=3`, `SERVER_ADDR='127.0.0.1'`.

Em cinco terminais, dentro desta pasta:

```bash
python3 GroupMngr.py
python3 peerCommunicatorUDP.py --id 0 --host 127.0.0.1 --udp-port 6789 --tcp-port 5679
python3 peerCommunicatorUDP.py --id 1 --host 127.0.0.1 --udp-port 6790 --tcp-port 5683
python3 peerCommunicatorUDP.py --id 2 --host 127.0.0.1 --udp-port 6791 --tcp-port 5684
python3 comparisonServer.py --messages 10 --round teste1
```

Aguarde o resultado `Totalmente ordenado: True`. O numero esperado por peer e `N * messages`.

## AWS

Defina `SERVER_ADDR` com IP publico acessivel do servidor e `GROUPMNGR_ADDR` se necessario. Execute GroupMngr e comparisonServer no servidor. Em cada EC2 peer, rode `peerCommunicatorUDP.py --id <id>` (sem `--host` para usar o IP de origem da conexao, ou informe o IP/DNS publico acessivel). Configure os security groups para permitir TCP 5680 ao group manager, TCP 5678 ao servidor de comparacao, TCP 5679 nos peers (ou portas escolhidas), e UDP 6789 nos peers (ou portas escolhidas). Restrinja as origens aos IPs necessarios. Regioes diferentes podem ter regras de firewall/NAT adicionais.

## Premissas e limitacoes

- Grupo fixo durante cada rodada, IDs de 0 a N-1.
- Retransmissoes e deduplicacao mitigam perdas transitorias; falhas permanentes, particoes e peers que parem de responder impedem progresso.
- Nao ha autenticacao ou criptografia no protocolo didatico. Use apenas em ambiente controlado.
- Os logs so comprovam a ordem das entregas nas execucoes testadas, nao uma prova formal.
- `pickle` do original foi substituido por JSON para evitar desserializacao insegura de trafego de rede.
- A medicao global inclui o envio dos comandos de inicio e a coleta dos resultados. Para relatorio rigoroso, repetir experimentos e usar condicoes equivalentes.
