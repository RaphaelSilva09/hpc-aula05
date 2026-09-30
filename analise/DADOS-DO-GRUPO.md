# Dados do grupo para a análise individual

Este arquivo reúne **os números medidos** (iguais para todos). A **interpretação é individual**: cada integrante escreve o seu `analise/<nome>.md` com as suas palavras, o gráfico, a discussão dos gargalos e a estimativa de f. Nada aqui substitui isso.

## O que o enunciado pede em `analise/<nome>.md`
1. O gráfico com **três curvas**: speedup do corpus, speedup do `pi_mpi` (Aula 3) e linear ideal (`resultados/curvas.png` já tem as três, com eixo x em log₂).
2. A discussão dos gargalos que explicam a distância entre as curvas: **I/O no NFS**, **shuffle e serialização**, **rede gigabit**, **hyperthreading a partir de 16 workers** e **fração serial**. Para cada um: onde aparece na curva e qual número prova.
3. A **estimativa da fração serial do pipeline pela lei de Amdahl** (e o que o ajuste diz sobre o modelo).

## Speedup (mediana de 3 rodadas por ponto, corpus de 300.000 resenhas)
| workers | nós | t_total (s) | speedup | eficiência | t_serial (s) | t_calc (s) | Karp-Flatt |
|---|---|---|---|---|---|---|---|
| 1 | 1 | 27.34 | 1.00 | 100% | 16.97 | 10.21 | - |
| 2 | 1 | 47.16 | 0.58 | 29% | 41.10 | 5.93 | 2.45 |
| 4 | 1 | 37.75 | 0.72 | 18% | 33.72 | 4.04 | 1.51 |
| 8 | 2 | 33.58 | 0.81 | 10% | 30.23 | 3.35 | 1.26 |
| 16 | 4 | 26.98 | 1.01 | 6% | 24.93 | 1.65 | 0.99 |
| 32 | 4 | 22.51 | 1.21 | 4% | 20.49 | 2.01 | 0.82 |

Speedup do `pi_mpi` (menor de 2 rodadas): S(1)=1.00, S(2)=1.99, S(4)=3.69, S(8)=7.33, S(16)=14.78, S(32)=21.57.

## Tempo por etapa (mediana, segundos)
| workers | leitura | tokeniza | stopwords | tf | df (larga) | tfidf | stats | subida do cluster (fora do t_total) |
|---|---|---|---|---|---|---|---|---|
| 1 | 2.25 | 4.72 | 2.31 | 1.72 | 7.31 | 1.48 | 7.35 | 2.93 |
| 2 | 1.72 | 2.52 | 1.19 | 1.28 | 16.33 | 0.94 | 23.01 | 2.47 |
| 4 | 1.67 | 1.52 | 1.01 | 0.74 | 12.57 | 0.78 | 19.42 | 2.89 |
| 8 | 2.71 | 1.24 | 0.62 | 0.67 | 10.90 | 0.72 | 16.18 | 32.90 |
| 16 | 4.56 | 0.37 | 0.22 | 0.32 | 7.73 | 0.72 | 12.22 | 36.94 |
| 32 | 2.13 | 0.30 | 0.20 | 0.21 | 7.11 | 1.31 | 11.18 | 32.30 |

## Fatos que a análise precisa explicar (sem a explicação pronta)
- O speedup do corpus fica **abaixo de 1** de 2 a 8 workers e chega a apenas 1.21 com 32. O do `pi_mpi` chega a 21.6.
- As etapas **estreitas** escalam (`tokeniza` de 4.7 s para 0.3 s; `t_calc` de 10.2 s para 2.0 s).
- As etapas **largas** (`df`, `stats`) **pioram** ao passar de 1 para 2 workers (`df`: 7.3 s → 16.3 s; `stats`: 7.3 s → 23.0 s) e depois melhoram devagar com mais workers.
- Já com 1 worker, 62% do tempo é `t_serial` (leitura, DF, estatísticas).
- A **subida do cluster** (`t_sobe`) é ~3 s em 1 nó e ~33 a 37 s com 2 ou 4 nós (fora do `t_total`).
- **O ajuste de Amdahl degenera** nos dados do corpus: o speedup é menor que 1, então a reta de 1/S contra 1/p dá f = 1 (truncado). O Karp-Flatt e(p) = (1/S − 1/p)/(1 − 1/p) vale 2,45; 1,51; 1,26; 0,99; 0,82 para p = 2, 4, 8, 16, 32: **não é constante** (Amdahl exigiria e constante) e **decresce**, isto é, há um custo fixo de sair do caso de 1 worker que vai sendo diluído.
- Comparação de estatísticas: a curva do `pi_mpi` usa o **menor** tempo (2 rodadas) e a do corpus usa a **mediana** (3 rodadas). Diga isso na análise.

## O que NÃO foi feito e você deve dizer com clareza
- Esta é a **v1** do pipeline. As agregações `df` e `stats` usam `frequencies` e `reduction` com parâmetros padrão do Dask; uma variante (v2) pode reduzir o custo dessas etapas e ainda não foi medida.
- Não foi medido um baseline serial puro (script sem Dask); o "1 worker" é o mesmo código com 1 worker, como sugere o guia.
