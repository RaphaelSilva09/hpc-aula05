# Parte 1: as cinco respostas do item 4.4 (RASCUNHO para revisão do grupo)

Base: `parte1/resultados/speedup.csv` (série de 30/09, posição controlada por `speedup_2rodadas.sh`, 2 rodadas, menor tempo por ponto, 4 bilhões de pontos).

| procs | nós | t_total (s) | speedup | eficiência | t_serial (s) |
|---|---|---|---|---|---|
| 1 | 1 | 34.866 | 1.00 | 100% | 0.0000 |
| 2 | 1 | 17.477 | 1.99 | 100% | 0.0000 |
| 4 | 1 | 9.448 | 3.69 | 92% | 0.0000 |
| 8 | 2 | 4.758 | 7.33 | 92% | 0.0040 |
| 16 | 4 | 2.360 | 14.78 | 92% | 0.0069 |
| 32 | 4 | 1.616 | 21.57 | 67% | 0.0074 |

Fração serial pelo ajuste de Amdahl: f = 0.013 (pontos de 1 a 16) e f = 0.014 (de 1 a 32); teto 1/f ≈ 79 a 74.

## 1. Onde a curva dobra (eficiência abaixo de 70%)?
A eficiência fica em 92% ou mais até 16 processos e só cai abaixo de 70% em **32 processos (67%)**. Até 16 há um processo por core físico (4 nós × 4 cores); em 32 os cores físicos já estão ocupados e os processos passam a dividir cada core com uma segunda thread.

## 2. De 16 para 32, quanto se ganhou? O que é SMT?
O tempo caiu de 2.360 s para 1.616 s: ganho de **1.46×** (o ideal seria 2×). O `lscpu` de cada nó mostra 4 cores × 2 threads por core = 8 CPUs lógicas. SMT (Hyper-Threading) é executar duas threads no mesmo core físico: elas dividem as mesmas unidades de execução e caches, então o ganho fica entre 10% e 50%, e não 100%.

## 3. Qual é a fração serial? A coluna t_serial cresce com p?
f ≈ 1.3% (1 a 16) e 1.4% (1 a 32). O `t_serial` é 0,000 s nos pontos de 1 nó e sobe para 0.0040 s (8), 0.0069 s (16) e 0.0074 s (32): cresce porque o `MPI_Reduce` passa a atravessar o switch gigabit e a envolver mais participantes. É de milésimos de segundo, então a parte serial do `pi_mpi` é quase só o arranque e o Reduce.

## 4. O Amdahl acertou?
Com f = 0.013 (ajustado em 1 a 16), Amdahl prevê S(32) = 22.95; o medido foi 21.57 (diferença de 6%). Amdahl supõe 32 processadores reais, e aqui os 16 últimos são threads SMT, que rendem menos que um core. Por isso o ajuste que inclui o 32 (0.014) mistura serialidade e SMT: o f deixa de ser só fração serial. Só os pontos de 1 a 16 (cores físicos) dão uma estimativa limpa.

## 5. Se cada rank lesse a sua fatia de um arquivo de 200 MB no NFS antes de calcular?
Todos os ranks leem pelo mesmo cabo gigabit do master (~107 MB/s medidos no ping-pong), então os 200 MB custam cerca de 1.9 s **independentemente do número de processos**: viram tempo serial. Com t(1) = 34.9 s, f sobe de 1.3% para cerca de 6.6%, o teto cai de 79 para 15, e Amdahl passa a prever S(16) ≈ 8.0 e S(32) ≈ 10.5 (contra 14.8 e 21.6 medidos). Com 50 mil contratos em PDF seria pior: além da leitura, há o custo de extrair o texto de cada arquivo, e o disco do master atende todos os leitores. É exatamente o que a Parte 2 mostra com dados reais.
