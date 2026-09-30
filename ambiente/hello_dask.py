"""hello_dask.py - HPC Aula 4 - Prof. João Luisi

O "hello" distribuido do checkpoint. Conecta no scheduler, espera os workers,
e prova tres coisas:
  1. quantos workers existem e em que nos eles estao;
  2. que as tarefas realmente rodaram espalhadas pelos nos;
  3. que um resultado calculado nos workers volta certo para o cliente.
"""
import argparse
import collections
import os
import socket
import time

from dask.distributed import Client


def quem_sou(i):
    """Roda DENTRO de um worker. Devolve onde a tarefa i foi executada."""
    time.sleep(0.05)  # so para a tarefa nao ser instantanea
    return socket.gethostname(), os.getpid(), i


def minha_funcao(x):
    """A funcao que o grupo mostra funcionando. Troquem por uma de voces:
    qualquer funcao pura de um numero serve. O esperado e recalculado sozinho."""
    return x * x


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scheduler-file", required=True)
    ap.add_argument("--workers", type=int, required=True,
                    help="quantos workers esperar antes de comecar")
    ap.add_argument("--nos", type=int, default=1,
                    help="em quantos nos distintos as tarefas precisam aparecer")
    ap.add_argument("--tarefas", type=int, default=64)
    args = ap.parse_args()

    client = Client(scheduler_file=args.scheduler_file)
    print(f"cliente rodando em : {socket.gethostname()}")
    print(f"scheduler em       : {client.scheduler.address}")

    client.wait_for_workers(args.workers, timeout=120)
    info = client.scheduler_info()["workers"]
    por_no = collections.Counter(w["host"] for w in info.values())
    print(f"workers conectados : {len(info)}")
    for ip, n in sorted(por_no.items()):
        print(f"  {ip}: {n} workers")

    # 1) Onde as tarefas rodaram
    t0 = time.perf_counter()
    futuros = client.map(quem_sou, range(args.tarefas))
    respostas = client.gather(futuros)
    dt = time.perf_counter() - t0

    por_host = collections.Counter(h for h, _, _ in respostas)
    processos = {(h, p) for h, p, _ in respostas}
    print(f"\n{args.tarefas} tarefas em {dt:.2f} s, executadas por {len(processos)} processos:")
    for host, n in sorted(por_host.items()):
        print(f"  {host}: {n} tarefas")

    # 2) A funcao do grupo aplicada de 0 a 999 nos workers, somada, e conferida
    #    contra a mesma conta feita em fila aqui no cliente.
    total = client.submit(sum, client.map(minha_funcao, range(1000))).result()
    esperado = sum(minha_funcao(x) for x in range(1000))
    print(f"\nsoma de minha_funcao : {total} (esperado {esperado})")

    print(f"nos distintos que executaram tarefas: {len(por_host)} (esperado {args.nos})")
    ok = len(por_host) == args.nos and total == esperado and len(info) >= args.workers
    print("\nCHECKPOINT OK" if ok else "\nCHECKPOINT FALHOU")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
