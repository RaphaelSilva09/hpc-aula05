# Análise individual: por que o corpus não escala como o pi_mpi

Raphael Silva · Ponderada da Aula 5 (HPC) · cluster de 4 nós com SLURM, MPI e Dask

## 1. Resumo

O mesmo cluster produziu duas curvas de speedup muito diferentes. Com 32 processos, o `pi_mpi` da Aula 3 chega a **21,57×** (eficiência de 67%). O pipeline TF-IDF em Dask, sobre 300.000 resenhas, chega a **1,21×** (eficiência de 4%). Com 2 workers ele fica pior que o caso de 1 worker (0,58×), e só passa de 1× com 16 workers.

A tese deste documento é que a distância entre as curvas não tem uma causa única. O `pi_mpi` é a régua do hardware: cada processo calcula sozinho e há apenas um `MPI_Reduce` de um número no fim. O pipeline usa o mesmo hardware, mas com um problema que move dados, e duas coisas aparecem:

1. Já com 1 worker, **62%** do tempo é de etapas que não são de cálculo local (leitura, contagem de documentos por termo e estatísticas finais). Isso limita o speedup a cerca de 1,6× antes de qualquer overhead.
2. Ao passar de 1 para 2 workers, essas mesmas etapas ficam **mais lentas** (a contagem de documentos por termo vai de 7,3 s para 16,3 s; as estatísticas, de 7,4 s para 23,0 s), no mesmo nó, onde a rede não existe. É o custo de serializar e redistribuir dados entre processos, que o caso de 1 worker não paga.

A estimativa da fração serial depende do que se chama de serial. Pela medida direta no caso de 1 worker, f = 0,62. Pelo ajuste da lei de Amdahl aos pontos com 2 ou mais workers, f = 0,88. No `pi_mpi`, f fica em 0,013 a 0,014. O corpus tem entre 45 e 70 vezes mais fração não paralela que o `pi_mpi`, e o modelo de Amdahl com f fixo descreve mal a curva dele, pelos motivos da seção 8.

O restante do texto segue essa ordem: como foi medido (seções 2 e 3), a figura e a régua (4 e 5), o corpus por dentro (6), cada gargalo com o número que o prova (7), Amdahl (8) e os limites do que foi medido (9 e 10).

## 2. Plataforma e método

### Hardware e software

| Item | Valor |
|---|---|
| Nós de cálculo | 4 (c1 a c4) mais um master |
| CPU por nó | Intel Core i3-13100T, 4 cores × 2 threads = 8 CPUs lógicas |
| Total | 16 cores físicos, 32 CPUs lógicas |
| Memória por nó | 7,6 GB (cerca de 4,1 GB livres) |
| Rede | Gigabit Ethernet, switch isolado |
| Armazenamento | NFS exportado pelo master; `/tmp` dos nós é RAM (nós stateless) |
| Sistema | Rocky Linux 9.8, OpenHPC, Warewulf 4, SLURM 25.11.4 |
| MPI | `gnu15` + `openmpi5` |
| Python | 3.11.16, dask/distributed 2026.8.0, pandas 3.0.5, pyarrow 25.0.0, scikit-learn 1.9.1, numpy 2.4.6 |

A quantidade de núcleos físicos importa para tudo que segue: até 16 processos há um processo por core; com 32 cada core executa dois (hyperthreading, também chamado SMT).

### Como os programas foram medidos

- **pi_mpi** (Monte Carlo, 4 bilhões de pontos, xorshift64 com semente por rank). Mede `t_total` (depois de um `MPI_Barrier` inicial, até os dois `MPI_Reduce`), `t_calc` (só o laço de sorteio, máximo entre ranks) e define `t_serial = t_total − t_calc`. Esse `t_serial` inclui o `Bcast`, os `Reduce` e qualquer desbalanceamento entre ranks, então **não é serial puro**; é uma cota superior do que não é cálculo.
- **Pipeline do corpus** (`pipeline/pipeline.py`). Cada etapa faz `persist()` seguido de `wait()`, para que o tempo de uma etapa não vaze para a seguinte. `t_calc = tokeniza + stopwords + tf + tfidf` (etapas que operam partição a partição) e `t_serial = t_total − t_calc` (leitura, `df` e `stats`). O `t_total` conta só depois de `wait_for_workers`; a subida do cluster (`t_sobe`) fica fora.
- **Estatística.** O `pi_mpi` usa o **menor de 2 rodadas**; o corpus usa a **mediana de 3 rodadas**, com as rodadas intercaladas entre as configurações e encadeadas por `--dependency=afterany`. Os dois critérios diferem, e isso pesa na comparação (seção 9).

### Posicionamento dos processos

| Processos | Nós | Opções |
|---|---|---|
| 1, 2, 4 | 1 | `--hint=nomultithread` |
| 8 | 2 | 4 por nó, `--hint=nomultithread` |
| 16 | 4 | 4 por nó, `--hint=nomultithread` |
| 32 | 4 | 8 por nó, `--ntasks-per-core=2` (SMT) |

No Dask, as configurações (nós × workers por nó) foram 1×1, 1×2, 1×4, 2×4, 4×4 e 4×8, sempre com 128 partições (problema de tamanho fixo, portanto escalabilidade forte) e uma thread por worker. Os workers do Dask foram lançados com `--cpu-bind=none`, então o mapeamento de workers em cores não foi fixado nem verificado.

### Problemas de ambiente que moldaram a medição

- **Memória no SLURM.** Sem `RealMemory` em `slurm.conf`, o SLURM assumia 1 MB por nó, o Dask lia o limite do cgroup e cada worker ficava com 128 KiB, com `StreamBufferFullError` em resultados acima de cerca de 500 KB. Correção: `RealMemory=7000` e `--memory-limit 0` nos workers.
- **Sistema de arquivos.** `/opt` é somente leitura nos nós (o código foi para `/home`), e `/tmp` é RAM, o que torna o diretório de spill do Dask uma forma de consumir memória.
- **Spark.** O `pandas<3` exigido pelo pacote `--spark` conflita com o pandas instalado, então o ambiente não tem Spark.
- **matplotlib.** O ambiente `hpc` não o tem; os gráficos saem de `pip install --target ~/pylib`.
- **Verificação do ambiente.** `confere_nos.sh` confirma versões iguais nos 4 nós e `hello_dask.py` confirma tarefas distribuídas em nós distintos (`CHECKPOINT OK`).

## 3. Microbenchmarks da Parte 1: a calibração do cluster

Antes de medir o corpus, a Parte 1 mediu o que o hardware entrega, e esses números servem de referência para todos os gargalos adiante.

### Rede (ping-pong, jobs 46 e 47)

| Tamanho | Mesmo nó | Nós diferentes | Razão | Vazão entre nós |
|---|---|---|---|---|
| 1 B | 0,24 µs | 235,37 µs | 980,7× | ~0,0 MB/s |
| 1 KiB | 0,36 µs | 221,33 µs | 614,8× | 9,3 MB/s |
| 1 MiB | 125,24 µs | 19.560,92 µs | 156,2× | 107,2 MB/s |

Dois fatos saem daqui. Primeiro, a latência entre nós é de cerca de 220 a 235 µs, três ordens de grandeza acima da intranó, então qualquer padrão de muitas mensagens pequenas sofre. Segundo, a vazão máxima entre nós é de 107,2 MB/s, cerca de 86% dos 125 MB/s teóricos de um gigabit; essa é a velocidade máxima de qualquer dado que atravesse o cabo, inclusive a leitura do NFS.

### Cálculo puro (`soma_reduce`, N = 2·10⁹, tempo do rank 0)

| Processos | Tempo |
|---|---|
| 1 | 0,478 s |
| 2 | 0,478 s |
| 4 | 0,241 s |
| 8 | 0,129 s |
| 32 | 0,033 s |

O speedup de 1 para 32 é 14,5× (eficiência de 45%), e de 1 para 4 é 1,98×. Dois pontos pedem cuidado:

- Com 2 processos o tempo é igual ao de 1. Isso sugere que os dois ranks caíram nas duas threads do mesmo core, o que seria o efeito do SMT; o mapeamento desse job não foi verificado.
- Nas repetições com 4 processos (jobs 53, 62 e 167) o tempo variou de 0,239 a 0,258 s, cerca de 7% de ruído entre execuções do mesmo programa.

Vale registrar que a medição só cobre o laço, sem barreira nem comunicação, e que os jobs 48 a 52 rodaram uma versão anterior do código (com o `if (rank==0)` na impressão), enquanto 53, 62 e 167 rodaram a versão em que todos os ranks imprimem. O resultado numérico foi conferido contra N(N+1)/2 em todos.

### Colaterais

- `hello_mpi` (job 45) confirmou 2 ranks por nó nos 4 nós, e o job 168 confirmou 8 ranks por nó em 32 processos.
- O mini desafio do `MPI_Allreduce` (job 167) fez todos os 4 ranks imprimirem o mesmo total, `2000000001000000000`, o que valida a soma.

## 4. A figura: três curvas

![Speedup: corpus TF-IDF em Dask vs pi_mpi vs linear ideal](../resultados/curvas.png)

| p | Linear ideal | pi_mpi | Eficiência pi_mpi | Corpus | Eficiência corpus | pi_mpi ÷ corpus |
|---|---|---|---|---|---|---|
| 1 | 1 | 1,00 | 100% | 1,00 | 100% | 1,0 |
| 2 | 2 | 1,99 | 100% | 0,58 | 29% | 3,4 |
| 4 | 4 | 3,69 | 92% | 0,72 | 18% | 5,1 |
| 8 | 8 | 7,33 | 92% | 0,81 | 10% | 9,0 |
| 16 | 16 | 14,78 | 92% | 1,01 | 6% | 14,6 |
| 32 | 32 | 21,57 | 67% | 1,21 | 4% | 17,8 |

A distância absoluta entre o `pi_mpi` e o corpus cresce a cada ponto: 1,42; 2,97; 6,51; 13,76 e 20,36. A leitura da figura tem três trechos:

- **De 1 a 16**, o `pi_mpi` acompanha a linear ideal com 92% de eficiência, e o corpus fica colado em 1.
- **Em 2 workers o corpus vai para baixo de 1.** É o único ponto em que a curva do corpus se afasta para menos que o caso de 1 worker, e é ali que está a pista principal (seção 7.1).
- **De 16 para 32**, as duas curvas ganham menos que o dobro: o `pi_mpi` ganha 1,46× e o corpus 1,20×. O dobro de processos coincide com o uso das segundas threads de cada core.

## 5. O `pi_mpi` como régua do hardware

O `pi_mpi` permite separar o que é limite do cluster do que é limite do pipeline, porque praticamente não comunica.

- **Tempo de cálculo.** `t_calc` vai de 34,87 s (1 processo) a 1,61 s (32), e o `t_total` de 34,87 s a 1,62 s.
- **Custo não computacional.** O `t_serial` é 0 nos pontos de 1 nó e sobe para cerca de 0,005 s (8 processos), 0,009 s (16) e 0,008 s (32), que são de 0,1% a 0,5% do tempo total de cada ponto. Ele aparece quando o `Reduce` atravessa o switch gigabit e deixa de ser uma operação em memória compartilhada.
- **Ponto de dobra.** A eficiência é de 92% ou mais até 16 processos (um por core físico) e cai para 67% em 32. As respostas da seção 4.4 atribuem a queda ao SMT: duas threads no mesmo core dividem unidades de execução e caches, e o ganho típico é de 10% a 50%, não 100%.
- **Sensibilidade ao posicionamento.** A série de 09/09 (descartada) mostra que a mesma contagem de processos rende muito diferente conforme o posicionamento. Com 8 processos, o speedup foi de 5,56× em 1 nó (SMT não controlado, 6,26 s), 7,33× em 2 nós (série final, 4,76 s) e 7,48× em 4 nós com 2 por nó (4,66 s). Com 16 processos em 2 nós foi de 10,92× contra 14,78× em 4 nós. A série foi descartada porque os jobs rodaram concorrentes e sem posição controlada (8 processos em um nó só, por exemplo), mas serve como prova: no hardware desse cluster, **espalhar processos por mais nós vale mais que empilhá-los**.

O `pi_mpi` mostra, portanto, o teto do cluster com quase nenhum dado para mover. Qualquer queda adicional do corpus em relação a essa curva não vem do hardware de cálculo.

## 6. O corpus por dentro

### Escolha do corpus

O corpus escolhido foi o **Amazon Polarity** (300.000 resenhas em inglês, 83 MB em 32 arquivos Parquet; o original tem 259,8 MB). O número de documentos foi limitado por memória (cerca de 4,1 GB livres por nó). A escolha veio de uma calibração com 1 worker nos dois candidatos:

| | AG News | Amazon |
|---|---|---|
| Documentos | 120.000 | 300.000 |
| Vocabulário | 61.488 | 196.361 |
| `t_total` | 11,91 s | 27,83 s |
| `t_serial` | 6,28 s (53%) | 17,52 s (63%) |

Com 12 s de execução total, o AG News deixaria o custo de subir o cluster e a variação entre rodadas pesando mais que as etapas medidas. O Amazon dá uma execução mais longa e com mais sinal, e o corpus foi decidido antes da série.

### Anatomia do caso de 1 worker

| Etapa | Tempo (s) | Fração do `t_total` | Natureza |
|---|---|---|---|
| leitura | 2,25 | 8% | I/O |
| tokeniza | 4,72 | 17% | estreita |
| stopwords | 2,31 | 8% | estreita |
| tf | 1,72 | 6% | estreita |
| df | 7,31 | 27% | larga (shuffle) |
| tfidf | 1,48 | 5% | estreita |
| stats | 7,35 | 27% | redução |

Somando `df`, `stats` e `leitura` obtém-se `t_serial` = 16,97 s, que são **62%** do `t_total` de 27,34 s; o restante, `t_calc` = 10,21 s, é o que escala bem. Isso já define o teto: se o `t_serial` fosse uma fração fixa e o `t_calc` escalasse perfeitamente, o speedup máximo seria 1/0,62 = 1,6×. O resultado medido (1,21× com 32 workers) está abaixo desse teto, e o que o faz ficar abaixo é o assunto da seção seguinte.

### Quem escala e quem não escala

Nas etapas estreitas, o speedup de 1 para 16 workers é de 12,8× em `tokeniza`, 10,5× em `stopwords` e 5,4× em `tf`. O `tfidf` é a exceção: cai de 1,48 s para 0,72 s e não baixa mais disso, com 0,72 s em 8 e em 16 workers. O `t_calc` inteiro vai de 10,21 s para 1,65 s em 16 workers, um speedup de 6,2× (39% de eficiência), e depois sobe para 2,01 s em 32.

Em contraste, as três etapas do `t_serial` perdem quase todo o ganho possível. A tabela de todas as etapas, em mediana:

| workers | leitura | tokeniza | stopwords | tf | df | tfidf | stats | `t_sobe` |
|---|---|---|---|---|---|---|---|---|
| 1 | 2,25 | 4,72 | 2,31 | 1,72 | 7,31 | 1,48 | 7,35 | 2,93 |
| 2 | 1,72 | 2,52 | 1,19 | 1,28 | 16,33 | 0,94 | 23,01 | 2,47 |
| 4 | 1,67 | 1,52 | 1,01 | 0,74 | 12,57 | 0,78 | 19,42 | 2,89 |
| 8 | 2,71 | 1,24 | 0,62 | 0,67 | 10,90 | 0,72 | 16,18 | 32,90 |
| 16 | 4,56 | 0,37 | 0,22 | 0,32 | 7,73 | 0,72 | 12,22 | 36,94 |
| 32 | 2,13 | 0,30 | 0,20 | 0,21 | 7,11 | 1,31 | 11,18 | 32,30 |

Em valor absoluto, o `t_serial` é 16,97 s (1 worker), 41,10 s (2), 33,72 s (4), 30,23 s (8), 24,93 s (16) e 20,49 s (32). Ou seja, ele **cresce 2,4×** ao sair do caso de 1 worker e só volta para baixo do valor inicial com mais de 32 workers. É esse crescimento, e não a rede, o responsável pelo 0,58× em 2 workers.

## 7. Os gargalos, um a um

Cada gargalo segue o mesmo roteiro: onde ele aparece na curva, o número que o prova e o peso que tem. Eles estão ordenados por peso.

### 7.1 Shuffle e serialização: o gargalo dominante

**Onde aparece.** Nas etapas `df` (contagem de documentos por termo, `flatten().frequencies()`) e `stats` (`reduction` mais a distribuição de tamanhos). São as duas etapas largas, em que cada partição produz um dicionário parcial de até centenas de milhares de termos que precisa ser combinado.

**Números que provam.**

- De 1 para 2 workers, **no mesmo nó**, `df` vai de 7,31 s para 16,33 s (2,2×) e `stats` de 7,35 s para 23,01 s (3,1×). Como os dois workers estão na mesma máquina, o aumento não pode ser de rede; só sobra o custo de serializar os dicionários, movê-los entre processos e combiná-los.
- `df` e `stats` somam **54%** do `t_total` com 1 worker e **83%, 85%, 81%, 74% e 81%** com 2, 4, 8, 16 e 32 workers.
- Nas etapas que não movem dados, o ganho é real (seção 6). O que não escala é exatamente o que serializa.
- O histórico da etapa `tfidf` reforça o argumento. No desenvolvimento, passar o vocabulário de IDF como argumento das tarefas custava 175 s (como `Future` em `bag.map`) ou 7 s com 111 mil termos e nem terminava em mais de 10 minutos com 300 mil documentos. Enviar uma vez por worker com `client.run` e lê-lo com `get_worker()` levou **0,17 s**. A mesma informação, o mesmo cluster, três ordens de grandeza de diferença: o custo de serializar e transportar dados de Python em Dask não é detalhe.

**Peso na curva.** É a maior parte da diferença. É o que faz o corpus pior que o caso de 1 worker em 2 e 4 workers, e o que mantém `t_serial` acima de 20 s mesmo com 32 workers.

**Ressalva.** O shuffle e a serialização não foram medidos isoladamente: o tempo deles está dentro de `df` e `stats`. O argumento acima é por eliminação (mesmo nó, etapas largas, sem rede) e pelo experimento do IDF. Além disso, este é o pipeline na sua versão 1, com `frequencies` e `reduction` nos parâmetros padrão do Dask, sem `split_every` nem qualquer ajuste. Uma versão 2 poderia reduzir esse custo, mas não foi medida.

### 7.2 Fração serial

O que a curva do corpus chama de parte serial é, na verdade, a soma de três coisas: a leitura, o custo de redistribuir dados e a combinação final. Só a última é serial de verdade (o driver combina dicionários parciais). A estimativa e a discussão do que Amdahl diz e não diz estão na seção 8.

### 7.3 I/O no NFS

**Onde aparece.** Na etapa `leitura`, que lê 83 MB de Parquet do NFS do master.

**Números que provam.**

- A 107,2 MB/s (o que o ping-pong mediu), 83 MB levariam cerca de **0,8 s**. A leitura mediana é de 1,67 a 4,56 s, **2 a 6 vezes** o tempo do cabo, o que indica que o limite não é só a largura de banda (há custo de decodificar Parquet, de serviço do NFS e de concorrência por um único servidor).
- A leitura não cai com mais workers; ela **sobe**: 2,25 s (1 worker), 1,72 s (2), 1,67 s (4), 2,71 s (8), 4,56 s (16) e 2,13 s (32). Com 16 workers ela é **17%** do `t_total`, a maior fatia relativa.
- A variação entre rodadas é grande nesse ponto: com 16 workers a leitura foi de 4,56 s, 2,12 s e 6,44 s. Esse é o comportamento de um recurso compartilhado, em que todos os nós disputam o mesmo servidor.

**Peso na curva.** Moderado, e só nos pontos com 8 ou mais workers. É a razão de o ponto de 16 workers ser o mais instável (seção 9).

**Ligação com a Parte 1.** A resposta 4.4 (item 5) faz exatamente esse raciocínio para o `pi_mpi`: se cada rank lesse 200 MB do NFS pelo mesmo cabo gigabit, isso seriam cerca de 1,9 s **independentes de p**, isto é, tempo serial. Com `t(1)` = 34,87 s, f sobe de 1,3% para cerca de 6,7%, o teto de Amdahl cai de 79 para 15 e as previsões caem para S(16) ≈ 8,0 e S(32) ≈ 10,4, contra 14,8 e 21,6 medidos sem essa leitura. A leitura do corpus é essa mesma situação, observada de fato.

### 7.4 Rede gigabit

**Onde aparece.** Só quando há mais de um nó: a partir de 8 workers (2 nós).

**Números que provam.**

- A rede entre nós tem latência de 221 a 235 µs e vazão de 107,2 MB/s (seção 3).
- O argumento contra a rede como causa principal é a forma da curva. O salto ruim acontece de 1 para 2 workers, no mesmo nó. Quando o pipeline cruza para o segundo nó (4 para 8 workers), `df` e `stats` **continuam caindo** (12,57 para 10,90 s e 19,42 para 16,18 s) em vez de subir. Não há degrau na fronteira de nós.
- A rede aparece, sim, em `t_sobe`: a subida do cluster custa cerca de 2,5 a 3,1 s com 1 nó e de **32 a 37 s** com 2 ou 4 nós. Esse tempo fica fora do `t_total` e não entra no speedup, mas é custo real de quem usa o cluster. A causa não foi verificada (hipóteses: cache frio do NFS nos nós novos, conexão dos workers ao scheduler, `srun` em nós remotos).

**Peso na curva.** Pequeno no `t_total`; a serialização custa mais que o transporte. A rede se torna relevante para o corpus se a serialização for reduzida, já que aí o cabo de 107 MB/s passaria a ser o próximo limite.

### 7.5 Hyperthreading a partir de 16 workers

**Onde aparece.** Na passagem de 16 para 32, em que cada core passa a ter dois workers.

**Números que provam.**

- No `pi_mpi`, dobrar os processos rendeu 1,46× (2,36 s para 1,62 s), com a eficiência caindo de 92% para 67%. Isso já é o efeito do SMT em um código puramente de cálculo.
- No corpus, o `t_total` vai de 26,98 s para 22,51 s (**1,20×**), um ganho de 4,47 s.
- O `t_calc` **sobe** de 1,65 s para 2,01 s, o que parece confirmar um efeito ruim do SMT. Olhando por etapa, porém, a explicação é outra. As três etapas de cálculo puro somam 0,91 s em 16 workers e 0,71 s em 32 (ganho de 1,28× com o dobro de workers, que é o efeito do SMT, parecido com os 1,46× do `pi_mpi`). Quem sobe é o `tfidf`, de 0,72 s para 1,31 s. O aumento líquido do `t_calc` (0,36 s) é, portanto, quase todo o `tfidf` (+0,59 s), compensado em parte pelo ganho das outras três etapas (−0,20 s).
- A hipótese para o `tfidf` é o custo de entregar o IDF a 32 workers em vez de 16 (`client.run` toca todos os workers). Ela não foi testada.

**Peso na curva.** Decide o último ponto: sem as segundas threads, o pi_mpi estaria em 14,78 e o corpus em 1,01. Quem usa SMT aceita um ganho de 20% a 46% pelo dobro de processos.

### 7.6 Síntese: onde cada gargalo aparece

| Trecho da curva | Gargalo dominante | Evidência |
|---|---|---|
| 1 → 2 workers (mesmo nó) | Serialização e shuffle | `df` ×2,2 e `stats` ×3,1 sem rede |
| 2 → 8 workers | Serialização, com leitura começando a pesar | `df+stats` = 81% a 85%; leitura sobe de 1,7 s para 2,7 s |
| 8 → 16 workers (4 nós) | Leitura no NFS e fração serial | leitura = 17% do total; `t_serial` = 92% do total |
| 16 → 32 workers (SMT) | Hyperthreading e entrega do IDF | `pi_mpi` ×1,46; corpus ×1,20; `tfidf` 0,72 para 1,31 s |
| Todos | Fração serial de base (62% em 1 worker) | `t_serial` = 16,97 s de 27,34 s |

## 8. Fração serial e a lei de Amdahl

A lei de Amdahl diz que S(p) = 1 / (f + (1 − f)/p), em que f é a fração do tempo em 1 processo que não se paraleliza. O teto é 1/f. Para estimar f há pelo menos três caminhos, e o resultado de cada um conta algo diferente.

### 8.1 pi_mpi: Amdahl funciona

O ajuste por mínimos quadrados de 1/S contra 1/p dá **f = 0,0127** com os pontos de 1 a 16 e **f = 0,0136** com todos (teto de 79 e 74). Com f = 0,0127, o modelo prevê S(16) = 13,44 e S(32) = 22,96; os valores medidos são 14,78 e 21,57. O erro é de −9% em 16 e +6% em 32, e muda de sinal, o que é uma pista: o desvio em 32 não é uma fração serial, é SMT (o modelo supõe 32 processadores reais). Por isso o f de 1 a 16 é a estimativa limpa, e o de 1 a 32 mistura serialidade e hyperthreading. O `t_serial` medido (no máximo 0,009 s em 34,87 s, isto é, 0,03%) é muito menor que o f ajustado de 1,3%, e a diferença vem de desbalanceamento entre ranks e de efeitos que o `t_serial` não captura.

### 8.2 Corpus: três estimativas

| Método | f | Teto 1/f | Observação |
|---|---|---|---|
| Medida direta no caso de 1 worker (`t_serial / t_total`) | 0,62 | 1,6 | fração de etapas que não são de cálculo local |
| Ajuste MQ, todos os pontos | 1,14, truncado em 1,0 | 1,0 | degenera (speedup menor que 1) |
| Ajuste MQ, p ≥ 2 | 0,88 | 1,13 | exclui o ponto p = 1, que não paga overhead distribuído |
| Karp-Flatt (p = 2, 4, 8, 16, 32) | 2,45; 1,51; 1,26; 0,99; 0,82 | - | não é constante, decresce |

O corpus tem um teto de Amdahl entre 1,0× e 1,6×, conforme o método, e a medida de 1,21× com 32 workers fica dentro desse intervalo. Três leituras ajudam a interpretar:

1. **O ajuste degenera porque Amdahl supõe que a parte serial não cresce com p.** Aqui ela cresce (o `t_serial` vai de 16,97 s para 41,10 s ao passar de 1 para 2 workers). Um modelo em que o tempo serial é constante não consegue produzir speedup menor que 1, e por isso o ajuste completo dá f > 1.
2. **Karp-Flatt mede o overhead que o modelo não explica, e ele decresce.** O valor 2,45 em 2 workers (maior que 1, impossível como fração) cai para 0,82 em 32. Isso é a assinatura de um custo fixo de entrar no regime distribuído, que é diluído à medida que a parte paralela encolhe, e não de uma fração serial constante. Ele diz que a distância entre o modelo e a medida é o imposto de distribuição.
3. **O ponto em que o imposto é pago.** O corpus só empata com o caso de 1 worker em 16 workers (S = 1,01), então o custo de distribuir equivale ao ganho de paralelizar as etapas de cálculo até esse ponto. Até lá, cada worker novo paga o que o anterior economizou.

### 8.3 O que concluir

O valor honesto da fração não paralela do pipeline é um intervalo, **f entre 0,62 e 0,88**: 0,62 é o que já não é cálculo local no caso de 1 worker, e 0,88 é a fração efetiva que o ajuste enxerga depois que o custo de distribuir entra. O teto correspondente de speedup é de 1,1× a 1,6×, e com 32 workers o pipeline está em 1,21×, isto é, cerca de 7% acima do que o ajuste p ≥ 2 prevê (1,13×) e cerca de 75% do que a fração direta permite (1,6×). Comparado ao `pi_mpi` (f ≈ 0,013), o corpus tem uma fração não paralela **cerca de 45 a 70 vezes maior** (0,62 ÷ 0,0136 e 0,88 ÷ 0,0127).

Ainda assim, a parte que é de cálculo escala de forma razoável: 6,2× em 16 workers (39%). O gargalo não é que o pipeline seja inerentemente serial, e sim que o trabalho que escala é pequeno (10,2 s de 27,3 s) perto do que movimenta dados.

## 9. Robustez e inconsistências que encontrei

### Variação entre rodadas

| workers | Rodadas de `t_total` (s) | Dispersão |
|---|---|---|
| 1 | 27,03 / 27,34 / 27,42 | 1,4% |
| 2 | 48,63 / 41,45 / 47,16 | 15,2% |
| 4 | 37,75 / 36,87 / 40,69 | 10,1% |
| 8 | 32,18 / 33,93 / 33,58 | 5,2% |
| 16 | 26,98 / 23,67 / 27,90 | 15,7% |
| 32 | 22,65 / 22,50 / 22,51 | 0,6% |

Os extremos (1 e 32 workers) são estáveis; os pontos intermediários, com 2 e 16 workers, variam muito. A sensibilidade é grande no ponto 16: com a **mediana** usada no gráfico o speedup é 1,01, e com o **menor** tempo seria 1,14, isto é, a afirmação de que o corpus só empata com o caso de 1 worker em 16 workers depende do critério estatístico. Já o resultado de 32 workers (1,21× com a mediana, 1,20× com o mínimo) não muda. O gráfico usa critérios diferentes para as duas curvas (mínimo no `pi_mpi`, mediana no corpus); no `pi_mpi` a diferença entre as duas rodadas é de no máximo 2% em todos os pontos, então essa assimetria não muda a leitura.

### Inconsistências nos dados

- O `t_serial` do `pi_mpi` na resposta 4.4 (0,0040, 0,0069 e 0,0074 s para 8, 16 e 32 processos) vem da linha de menor `t_serial` de cada rodada e difere do de `speedup.csv` (0,0047, 0,0085 e 0,0078 s), que vem da linha de menor `t_total`. A conclusão (cresce com o número de nós) vale nos dois casos, mas o ponto 16 para 32 cai no segundo.
- A resposta 4.4 prevê S(32) = 22,95 com f = 0,013; recalculando, dá 22,96 com f = 0,0127 e 22,51 com f = 0,0136. O `analisa_speedup.py` usa todos os pontos e reporta f = 0,014, teto 73,6 e S(32) = 22,52.
- Os jobs 48 a 52 do `soma_reduce` usaram uma versão anterior do código, diferente da dos jobs 53, 62 e 167. A comparação entre eles vale só para o tempo do laço, que é igual nas duas versões.
- O comentário do `pipeline.py` cita um teste com 111 mil termos que não corresponde a nenhum vocabulário medido (61.488 no AG News, 196.361 no Amazon) e não tem log em `calibracao/`.
- O relatório da Aula 3, citado em `tabelas-blocos-2-e-3.md`, não está no repositório.
- Existem cópias duplicadas de `speedup.csv`, `speedup_bruto.csv` e `speedup_pi_mpi.png` em `resultados/` e em `parte1/resultados/`; os conteúdos que conferi são equivalentes.

### Limites do experimento

- **Não há baseline serial puro** (um programa sem Dask). O "1 worker" inclui o overhead do Dask, e o speedup é relativo a ele. Um baseline mais rápido só deixaria as curvas do corpus mais baixas.
- **Um corpus e um cluster.** O efeito do tamanho do problema (lei de Gustafson, em que mais dados melhorariam a fração paralela) não foi testado.
- **Não foram medidos** memória, tráfego de rede, tamanho dos dicionários serializados nem o tempo de shuffle isolado. Por isso as atribuições da seção 7.1 são por eliminação e as da 7.5 e 7.4 são hipóteses.
- **A série de 09/09** do `pi_mpi` foi descartada por execução concorrente e sem posição controlada; os números dela só aparecem aqui como contraste.
- **Mapeamento de workers em cores** não foi fixado no Dask (`--cpu-bind=none`) e o dos jobs `soma_reduce` com 2 processos não foi verificado.

## 10. Conclusão e próximos passos

O `pi_mpi` e o corpus rodaram no mesmo cluster e diferem em quase tudo o que importa para escalar. O primeiro calcula sozinho em cada processo e comunica um número; chega a 92% de eficiência até 16 processos e cai para 67% ao usar as segundas threads dos cores. O segundo já gasta 62% do tempo fora do cálculo local com 1 worker, e a distribuição piora essa parte antes de melhorá-la: o pipeline só empata com 1 worker em 16 e chega a 1,21× em 32.

Em ordem de peso, o que separa as curvas é a serialização e o shuffle das etapas `df` e `stats`, a fração não paralela de base, a leitura no NFS (que pesa de 8 workers em diante), a rede (que só aparece no custo de subir o cluster) e o hyperthreading (que decide o último ponto). A fração serial do pipeline fica entre **0,62 e 0,88**, contra 0,013 no `pi_mpi`, e o modelo de Amdahl com f fixo descreve mal o corpus porque o custo não paralelo cresce ao entrar no regime distribuído (Karp-Flatt decrescente de 2,45 a 0,82).

O que faria a seguir, pela evidência desta análise:

1. **Reduzir o custo de `df` e `stats`** (versão 2 do pipeline), com `split_every` nas reduções, vocabulário persistido em vez de recomputado e redução em árvore. É o ataque direto ao gargalo de maior peso.
2. **Medir em vez de supor**: dashboard do Dask (porta 8787) para separar shuffle, serialização e transporte; tráfego de rede por etapa; `t_sobe` por nó.
3. **Tirar a leitura do NFS** (cópia local ou cache por nó) e repetir a série, para separar o efeito do I/O.
4. **Testar 32 workers sem SMT** (mais nós) e o mapeamento de cores com `--cpu-bind`.
5. **Aumentar o corpus** para testar a escalabilidade fraca (Gustafson): se o custo de distribuir é fixo, um corpus maior melhora a fração paralela.
6. **Acrescentar um baseline serial puro** para saber quanto do 62% é Dask e quanto é o problema.

## 11. Reprodução

```bash
# Gráfico das três curvas (na raiz do repositório)
python3 pipeline/plota_curvas.py resultados/speedup_corpus.csv resultados/speedup.csv

# Tabelas, Karp-Flatt, medianas por etapa e ajuste de Amdahl do corpus
python3 pipeline/analisa_corpus.py resultados/corpus_bruto.csv resultados/speedup_corpus.csv

# Ajuste de Amdahl do pi_mpi (grava speedup.png no diretório atual)
cd parte1/codigo_mpi && python3 analisa_speedup.py ../resultados/speedup.csv
```

Os dados brutos estão em `resultados/corpus_bruto.csv` (18 execuções, jobs 149 a 166 em `logs_serie/`) e em `resultados/speedup_bruto.csv` (2 rodadas, jobs 134 a 145). Os microbenchmarks estão em `parte1/resultados/` (jobs 45 a 53, 62, 167 e 168), e a calibração dos corpora em `calibracao/`.
