# HPC, Aula 5 (ponderada): TF-IDF distribuído em Dask no cluster do grupo

Módulo HPC, Inteli 2026.2. Prof. João Luisi.

> **v1 concluída.** Único `TODO` restante: licença e citação do dataset (conferir no cartão do Hugging Face). A v2 (agregações `df`/`stats` otimizadas) ainda não foi medida.

## O que este repositório contém

```
README.md          este arquivo (visão geral e como reproduzir do zero)
ambiente/          scripts da Aula 4: instala_ambiente.sh, confere_nos.sh, ambiente.sh, hello_dask.py, job_hello_dask.sbatch
pipeline/          pipeline.py, job_corpus.sbatch, sobe_dask.sh, serie_corpus.sh,
                   prepara_dataset.py, analisa_corpus.py, plota_curvas.py
parte1/            MPI da Aula 3: código, tabelas dos Blocos 2 e 3, speedup do pi_mpi e respostas do item 4.4
resultados/        speedup.csv (pi_mpi), corpus_bruto.csv (18 execuções), speedup_corpus.csv (mediana), gráficos
analise/           DADOS-DO-GRUPO.md e uma análise individual por integrante (<nome>.md)
calibracao/        testes de 1 worker que justificaram trocar o AG News pelo Amazon Polarity
logs_serie/        saída dos 18 jobs da série do corpus
```

Cada pasta com resultados tem o seu README curto (`parte1/`, `resultados/`, `analise/`).

## Ambiente

| Item | Valor |
|---|---|
| Nós | 1 master + 4 nós (c1 a c4), stateless, rodam da RAM |
| CPU de cada nó | Intel Core i3-13100T: 4 cores físicos, 2 threads por core (8 CPUs lógicas) |
| Total | 16 cores físicos, 32 CPUs lógicas |
| Memória por nó | 7.627 MB (a imagem do sistema também mora na RAM: ~4,1 GB livres) |
| Rede | Gigabit Ethernet, switch isolado 10.10.10.0/24 (~110 MB/s medidos) |
| SO | Rocky Linux 9.8 (master e nós) |
| Gerência | OpenHPC, Warewulf 4, SLURM 25.11.4 (configless), munge, chrony, NFS |
| Python | 3.11.16 (ambiente `hpc` em `/opt/ohpc/pub/apps/miniforge3`) |
| Bibliotecas | dask e distributed 2026.8.0, pandas 3.0.5, pyarrow 25.0.0, scikit-learn 1.9.1, numpy 2.4.6 |
| MPI (Parte 1) | gnu15, openmpi5 (`module load gnu15 openmpi5`) |

## Dataset

**Corpus usado nas medições:** subconjunto de 300.000 resenhas do Amazon Polarity.

| Campo | Valor |
|---|---|
| Nome | Amazon Polarity (resenhas de produtos da Amazon), primeiras 300.000 do arquivo de treino `train-00000-of-00004` |
| Fonte | Hugging Face, `fancyzhx/amazon_polarity`: https://huggingface.co/datasets/fancyzhx/amazon_polarity |
| Licença e citação | **TODO: conferir no cartão do dataset e registrar aqui** |
| Documentos | 300.000 (o arquivo inteiro tem 900.000 e o dataset completo, 3,6 milhões) |
| Tamanho em disco | 83 MB em 32 arquivos Parquet (arquivo original: 259.761.770 bytes) |
| Formato | Parquet, uma coluna `texto` (título + conteúdo da resenha) |
| Idioma | inglês |
| Tamanho médio | ~79 palavras por documento |
| SHA-256 do original | `57c367f8c74210dde3742b17d103af33820df3af39d029f2a5051a6f87810661` |
| Local no cluster | `/opt/ohpc/pub/grupo/dados/amazon` (visível nos 4 nós) |
| Vocabulário (depois das stopwords) | 196.361 termos |

O subconjunto é determinístico (as primeiras 300.000 linhas), e a conversão é feita uma vez, fora da medição, por `pipeline/prepara_dataset.py --max-docs 300000`.

### Por que não o AG News

O primeiro corpus escolhido foi o **AG News** (Hugging Face, `fancyzhx/ag_news`, partição de treino: 120.000 notícias, 18 MB, inglês, SHA-256 do original `fc508d6d9868594e3da960a8cfeb63ab5a4746598b93428c224397080c1f52ee`). Ele atende o mínimo do enunciado (50.000 documentos), mas foi **descartado antes de qualquer medição da série**, por um teste de calibração com 1 worker e 128 partições (`calibracao/calibracao_agnews_1worker.out`):

| Corpus | Documentos | t_total com 1 worker | Parte que já não escala (leitura, DF, estatísticas) |
|---|---|---|---|
| AG News | 120.000 | **11,9 s** | 6,3 s (53%) |
| Amazon Polarity (subconjunto) | 300.000 | **27,8 s** | 17,5 s (63%) |

Com 11,9 s, os custos fixos dominariam a curva e o speedup ficaria limitado a cerca de 2×, sem informar sobre os gargalos. O guia da atividade recomenda um caso de 1 worker de pelo menos algumas dezenas de segundos (o `pi_mpi` levava ~35 s). O Amazon deu 27,8 s, na faixa esperada.

Regras seguidas: o corpus foi decidido **antes** da série e **não mudou** entre as configurações (problema de tamanho fixo, sem Gustafson). O tamanho do subconjunto (300.000, e não os 900.000 do arquivo) vem da memória: cada nó tem ~4,1 GB livres, e um worker precisa segurar as etapas intermediárias.

## Pipeline

Quatro etapas, como pede o enunciado, cada uma cronometrada com `persist()` e `wait()`:

| Etapa | Tipo | O que faz |
|---|---|---|
| leitura | I/O no NFS | os workers leem as partições direto do NFS |
| tokenização | estreita | minúsculas e regex `[^\W\d_]{2,}` |
| stopwords | estreita | lista de inglês do scikit-learn |
| TF | estreita | frequência relativa por documento |
| DF e IDF | **larga** (shuffle) | em quantos documentos cada termo aparece, com `frequencies` (combina antes de mover) |
| TF-IDF | estreita | o IDF vai uma vez para cada worker (`client.run`) |
| estatísticas | redução | tamanho do vocabulário, distribuição do tamanho dos documentos e termos de maior TF-IDF |

Decisões de projeto:

- **Dask, e não Spark.** As etapas são funções Python por documento, e o ambiente validado em aula é o Dask.
- **4 workers de 1 thread por nó** (por causa do GIL). Com 32 workers são 8 por nó (SMT).
- **Partições:** 128, o mesmo valor nas 6 configurações (problema de tamanho fixo).
- **"Maior TF-IDF"** = soma do TF-IDF do termo sobre todos os documentos.
- **Só números pequenos voltam ao cliente.** A matriz TF-IDF nunca sai dos workers.
- **Scheduler dentro do job**, no primeiro nó da alocação. O painel é acessado só por túnel SSH.

Definições usadas nos CSVs (mesmo formato da Aula 3, `nprocs,nnodes,t_total,t_serial,t_calc`):

- `t_calc` = tokenização + stopwords + TF + TF-IDF (etapas estreitas);
- `t_serial` = `t_total` menos `t_calc` (leitura, shuffle do DF, redução final e overhead do scheduler);
- `t_total` conta a partir de depois do `wait_for_workers`. O tempo de subida do cluster é registrado à parte (`t_sobe`).

## Reproduzir do zero

0. **Configuração do SLURM (obrigatória neste cluster).** O `NodeName=c[1-4]` do `/etc/slurm/slurm.conf` precisa de `RealMemory=7000`; sem isso o SLURM assume 1 MB por nó e o Dask não funciona (ver "Problemas encontrados", item 3). Depois: `scontrol reconfigure`.
1. **Cluster de pé:** `sinfo` com c[1-4] `idle`, e o NFS montado nos nós (`srun -N 4 df -h /opt/ohpc/pub`).
2. **Código:** clonar este repositório em uma pasta **gravável e visível nos 4 nós**. Neste cluster, `/opt` é montado somente leitura nos nós (perfil do Warewulf). Use `/home/<usuario>/...`.
3. **Ambiente Python no NFS:** `cd ambiente && sudo ./instala_ambiente.sh` (leia o script antes de rodar com sudo) e `./confere_nos.sh`. Devem sair 4 linhas iguais. Opcional: `sbatch job_hello_dask.sbatch` deve terminar com `CHECKPOINT OK` em 4 nós.
4. **Dataset:**
   ```bash
   D=/opt/ohpc/pub/grupo/dados; mkdir -p $D/raw
   curl -L -o $D/raw/amazon_polarity_train-00000-of-00004.parquet \
     https://huggingface.co/datasets/fancyzhx/amazon_polarity/resolve/main/amazon_polarity/train-00000-of-00004.parquet
   sha256sum $D/raw/amazon_polarity_train-00000-of-00004.parquet   # confere com o valor acima
   source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc
   python pipeline/prepara_dataset.py --parquet $D/raw/amazon_polarity_train-00000-of-00004.parquet \
       --colunas title content --max-docs 300000 --saida $D/amazon --arquivos 32
   chmod -R a+rX $D; srun -N 4 ls $D/amazon | wc -l    # 32 em cada nó
   ```
5. **Série do corpus** (18 jobs encadeados, 6 configurações × 3 rodadas, rodadas intercaladas):
   ```bash
   cd pipeline
   DADOS=/opt/ohpc/pub/grupo/dados/amazon PARTICOES=128 TEMPO=00:10:00 ./serie_corpus.sh
   squeue                                         # acompanhe
   python3 analisa_corpus.py                      # mediana, Amdahl, Karp-Flatt
   python3 plota_curvas.py resultados/speedup_corpus.csv resultados/speedup.csv
   ```
6. **Série do `pi_mpi` (Parte 1):** `cd parte1/codigo_mpi && module load gnu15 openmpi5 && make && ./speedup_2rodadas.sh && python3 analisa_speedup.py` (detalhes em `parte1/README.md`).

Escada de configurações (posição controlada; sem isso a curva mente):

| Workers | `-N` | por nó |
|---|---|---|
| 1 | 1 | 1 |
| 2 | 1 | 2 |
| 4 | 1 | 4 |
| 8 | 2 | 4 |
| 16 | 4 | 4 |
| 32 | 4 | 8 (SMT) |

## Problemas encontrados e como foram resolvidos

1. **Nós marcados `DOWN` depois de um reboot.** Era só estado do SLURM. `scontrol update nodename=c[1-4] state=resume`.
2. **`/opt` montado `ro` nos nós.** O job morre em 1 s, sem saída, porque o SLURM não consegue criar o arquivo de saída. Solução: rodar em `/home/<usuario>` (rw).
3. **`slurm.conf` sem `RealMemory`.** O SLURM assumia 1 MB por nó e aplicava `RLIMIT_RSS` de 1024 KB aos jobs. O Dask lê esse limite e, com ele, os workers ficavam com 128 KiB e qualquer resultado maior que ~500 KB voltando ao cliente falhava (`StreamBufferFullError`). **Correção:** `RealMemory=7000` em `NodeName=c[1-4]` e `scontrol reconfigure` (backup em `/etc/slurm/slurm.conf.bak-20260930`). Testes que **não** resolveram: `--memory-limit 0`, `--mem=0`, `--propagate=NONE` e `ulimit`.
4. **Como enviar o IDF aos workers.** O IDF é um dicionário grande (196 mil termos). Medido em 111 mil termos, 4 workers: `Future` em `bag.map` levou 175 s (o Dask resolve o Future a cada documento); dicionário ou `delayed` como argumento de `map_partitions` levou 7 s, e com 300.000 documentos o job **travou por mais de 10 minutos**; `client.run` guardando o IDF no objeto `worker` (uma cópia por worker) levou **0,17 s**, com resultado idêntico ao método simples (conferido em 120.000 documentos). É o método usado. O custo de mover esse dicionário cresce com o número de workers e entra na análise de gargalos (serialização).

## Resultados (v1)

Corpus de 300.000 resenhas, 128 partições, mediana de 3 rodadas por configuração (`resultados/corpus_bruto.csv` tem as 18 execuções; `resultados/speedup_corpus.csv`, as 6 medianas; logs em `logs_serie/`).

| workers | nós | t_total (s) | speedup | eficiência | t_serial (s) | t_calc (s) |
|---|---|---|---|---|---|---|
| 1 | 1 | 27,34 | 1,00 | 100% | 16,97 | 10,21 |
| 2 | 1 | 47,16 | 0,58 | 29% | 41,10 | 5,93 |
| 4 | 1 | 37,75 | 0,72 | 18% | 33,72 | 4,04 |
| 8 | 2 | 33,58 | 0,81 | 10% | 30,23 | 3,35 |
| 16 | 4 | 26,98 | 1,01 | 6% | 24,93 | 1,65 |
| 32 | 4 | 22,51 | 1,21 | 4% | 20,49 | 2,01 |

![curvas](resultados/curvas.png)

Leitura resumida: as etapas **estreitas** escalam (`t_calc` cai de 10,2 s para ~2 s), mas as etapas **largas** (`df`, `stats`), que movem dicionários de ~196 mil termos entre processos, **pioram** ao sair de 1 worker (onde tudo fica no mesmo processo) e só melhoram devagar com mais workers. O speedup fica abaixo de 1 até 8 workers. O ajuste de Amdahl degenera (f = 1, truncado) porque o speedup é menor que 1; o Karp-Flatt e(p) = 2,45; 1,51; 1,26; 0,99; 0,82 (p = 2 a 32) não é constante, como Amdahl exigiria: **decresce**, o que aponta para um custo fixo de sair do caso de 1 worker (serialização entre processos), diluído conforme entram mais workers. A subida do cluster (fora do `t_total`) leva ~3 s em 1 nó e ~33 a 37 s com 2 ou 4 nós. Números por etapa e demais fatos: `analise/DADOS-DO-GRUPO.md`. As análises individuais ficam em `analise/<nome>.md`.

**Limites da v1:** as agregações `df` e `stats` usam `frequencies` e `reduction` com os parâmetros padrão do Dask. Uma variante que reduza o custo dessas etapas (v2) ainda não foi medida. Não há baseline serial puro (o "1 worker" é o mesmo código do Dask).

### Parte 1 (`pi_mpi`, Aula 3)

Série refeita em 30/09 com `speedup_2rodadas.sh` (posição controlada, 2 rodadas, menor tempo por ponto): S(1..32) = 1,00; 1,99; 3,69; 7,33; 14,78; 21,57. Eficiência de 92% até 16 e 67% em 32 (ganho de 1,46× de 16 para 32: SMT). Amdahl: f ≈ 1,3% (1 a 16). Detalhes e respostas do item 4.4: `parte1/respostas-4.4-RASCUNHO.md`. A série de 09/09 (jobs 54 a 59) foi rodada **sem controle de posição e com os jobs concorrentes**, e por isso não foi usada (mantida em `parte1/resultados/` como rascunho). As tabelas dos Blocos 2 e 3 estão no relatório da Aula 3 do grupo.

## Segurança

- nunca comitar a `munge.key`;
- não abrir as portas 8786 e 8787 no Wi-Fi do master (usar túnel SSH: `ssh -N -L 8787:c1:8787 <usuario>@<ip-do-master>`);
- o scheduler roda dentro do job e morre com ele (`-t` é o seguro);
- ler os scripts antes de rodar com `sudo`;
- por padrão, Dask e Spark confiam em quem tem rota (`client.run` executa código nos workers): a defesa é a rede isolada e o túnel SSH.
