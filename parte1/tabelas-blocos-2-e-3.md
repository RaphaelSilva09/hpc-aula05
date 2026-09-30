# Parte 1: tabelas dos Blocos 2 e 3 (Aula 3)

Números extraídos das saídas originais do cluster, que estão neste repositório em `parte1/resultados/` (`pingpong-46.out`, `pingpong-47.out`, `soma-48.out` a `soma-53.out` e `soma-62.out`). As respostas escritas às perguntas de cada bloco estão no **relatório do grupo** (`parte1/relatorio-aula03.pdf`, quando adicionado).

## Bloco 2: ping-pong (job 46: mesmo nó c1; job 47: nós c1 e c2)

| Tamanho | Mesmo nó: ida e volta | Nós diferentes: ida e volta | Razão | Vazão entre nós |
|---|---|---|---|---|
| 1 B | 0,24 µs | 235,37 µs | 980,7× | ~0,0 MB/s |
| 1 KiB | 0,36 µs | 221,33 µs | 614,8× | 9,3 MB/s |
| 1 MiB | 125,24 µs | 19.560,92 µs | 156,2× | 107,2 MB/s |

107,2 MB/s são cerca de 86% dos 125 MB/s teóricos de um gigabit.

## Bloco 3: `soma_reduce`, N = 2.000.000.000 (o programa confere contra N(N+1)/2 e imprimiu OK em todas)

| Processos | Job | Faixa do rank 0 | Tempo do rank 0 |
|---|---|---|---|
| 1 | 48 | 1 a 2.000.000.000 | 0,478 s |
| 2 | 49 | 1 a 1.000.000.000 | 0,478 s |
| 4 | 50 | 1 a 500.000.000 | 0,241 s |
| 8 | 51 | 1 a 250.000.000 | 0,129 s |
| 32 | 52 | 1 a 62.500.000 | 0,033 s |

Repetições com 4 processos: job 53 (0,257 s) e job 62 (0,240 s), este último com o `MPI_Allreduce` do mini desafio.

Observação: com 2 processos o rank 0 somou metade do intervalo e levou o mesmo tempo que com 1 (0,478 s). Isso sugere que os dois processos caíram nas duas threads do mesmo core (SMT), e não em dois cores. Vale confirmar o mapeamento no `SLURM_JOB_NODELIST` e nos `--hint`.
