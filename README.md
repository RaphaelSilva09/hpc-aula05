# HPC, Aula 5 (ponderada): TF-IDF distribuído em Dask no cluster do grupo

Módulo HPC, Inteli 2026.2. Prof. João Luisi.

## O que foi feito

- **Parte 1 (MPI, Aula 3):** série do `pi_mpi` com 1, 2, 4, 8, 16 e 32 processos, com a posição dos processos controlada, além das tabelas dos Blocos 2 e 3 e das respostas do item 4.4.
- **Parte 2 (Aula 5):** pipeline de PLN em Dask sobre **300.000 resenhas do Amazon Polarity**, com 4 etapas (tokenização, stopwords, TF-IDF e estatísticas do corpus), submetido via `sbatch` em 6 configurações (1, 2, 4, 8, 16 e 32 workers), **3 rodadas cada, com a mediana** registrada.
- **Análise:** gráfico de 3 curvas (corpus, `pi_mpi` e linear ideal), gargalos e fração serial pela lei de Amdahl, em `analise/`.

## Resultado

| workers | nós | t_total (s) | speedup | eficiência |
|---|---|---|---|---|
| 1 | 1 | 27,34 | 1,00 | 100% |
| 2 | 1 | 47,16 | 0,58 | 29% |
| 4 | 1 | 37,75 | 0,72 | 18% |
| 8 | 2 | 33,58 | 0,81 | 10% |
| 16 | 4 | 26,98 | 1,01 | 6% |
| 32 | 4 | 22,51 | 1,21 | 4% |

![curvas](resultados/curvas.png)

Para comparação, o `pi_mpi` (Parte 1) chega a 21,57× com 32 processos. O gráfico acima mostra as três curvas pedidas (corpus, `pi_mpi` e linear ideal). A discussão dos gargalos e a estimativa da fração serial pela lei de Amdahl estão nas análises individuais, em `analise/`.

## Estrutura

```
ambiente/    instalação e verificação do ambiente Python no NFS (instala_ambiente.sh, confere_nos.sh, hello_dask.py, ...)
pipeline/    pipeline.py, job_corpus.sbatch, sobe_dask.sh, serie_corpus.sh,
             prepara_dataset.py, analisa_corpus.py, plota_curvas.py
parte1/      código MPI, tabelas dos Blocos 2 e 3, resultados do pi_mpi e respostas do 4.4
resultados/  CSVs (speedup.csv do pi_mpi, corpus_bruto.csv, speedup_corpus.csv) e gráficos
analise/     análises individuais (<nome>.md) e DADOS-DO-GRUPO.md (números do grupo)
calibracao/  testes de 1 worker que justificaram a escolha do corpus
logs_serie/  saída dos 18 jobs da série
```

## Dataset

| Campo | Valor |
|---|---|
| Nome e fonte | Amazon Polarity, Hugging Face `fancyzhx/amazon_polarity` (primeiras 300.000 resenhas do arquivo `train-00000-of-00004`) |
| Documentos | 300.000 |
| Tamanho em disco | 83 MB (32 arquivos Parquet); original de 259,8 MB |
| Formato e idioma | Parquet com uma coluna `texto` (título + conteúdo); inglês |
| Local no cluster | `/opt/ohpc/pub/grupo/dados/amazon` (visível nos 4 nós) |

Licença: Apache License 2.0 (cartão do dataset no Hugging Face). Citação: McAuley, J.; Leskovec, J. *Hidden factors and hidden topics: understanding rating dimensions with review text.* Proceedings of the 7th ACM Conference on Recommender Systems, pp. 165-172, 2013. SHA-256 do arquivo original: `57c367f8c74210dde3742b17d103af33820df3af39d029f2a5051a6f87810661`.

**Por que não o AG News:** foi o primeiro corpus escolhido (`fancyzhx/ag_news`, 120.000 notícias, 18 MB, inglês), mas um teste de calibração com 1 worker e 128 partições (`calibracao/`) deu **11,9 s**, dos quais 53% já eram etapas que não escalam. Com esse tamanho os custos fixos dominariam a curva. O Amazon Polarity (300.000 resenhas) deu **27,8 s**. O corpus foi decidido **antes** da série e **não mudou** entre as configurações. O subconjunto é de 300.000 documentos (e não os 900.000 do arquivo) por causa da memória: cada nó tem ~4,1 GB livres.

## Ambiente

Master + 4 nós (c1 a c4, Intel Core i3-13100T: 4 cores × 2 threads, 7,6 GB de RAM cada; total de 16 cores físicos e 32 CPUs lógicas), Gigabit Ethernet, Rocky Linux 9.8, OpenHPC, Warewulf 4, SLURM 25.11.4, NFS. Python 3.11.16, dask/distributed 2026.8.0, pandas 3.0.5, pyarrow 25.0.0, scikit-learn 1.9.1, numpy 2.4.6. MPI: `module load gnu15 openmpi5`.

## Como reproduzir do zero

0. **SLURM:** em `/etc/slurm/slurm.conf`, o `NodeName=c[1-4]` precisa de `RealMemory=7000`; depois `scontrol reconfigure`. Sem isso o SLURM assume 1 MB por nó, aplica um limite de memória de 1 MB aos jobs, e o Dask falha ao devolver resultados maiores que ~500 KB ao cliente (`StreamBufferFullError`).
1. **Cluster de pé:** `sinfo` com c[1-4] `idle`; NFS montado (`srun -N 4 df -h /opt/ohpc/pub`).
2. **Código:** clone este repositório em uma pasta **gravável e visível nos 4 nós**. Aqui, `/opt` é somente leitura nos nós; use `/home/<usuario>/`.
3. **Ambiente Python no NFS:** `cd ambiente && sudo ./instala_ambiente.sh` (leia o script antes) e `./confere_nos.sh` (4 linhas iguais).
4. **Dataset:**
   ```bash
   D=/opt/ohpc/pub/grupo/dados; mkdir -p $D/raw
   curl -L -o $D/raw/amazon_polarity_train-00000-of-00004.parquet \
     https://huggingface.co/datasets/fancyzhx/amazon_polarity/resolve/main/amazon_polarity/train-00000-of-00004.parquet
   sha256sum $D/raw/amazon_polarity_train-00000-of-00004.parquet   # 57c367f8...810661
   source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc
   python pipeline/prepara_dataset.py --parquet $D/raw/amazon_polarity_train-00000-of-00004.parquet \
       --colunas title content --max-docs 300000 --saida $D/amazon --arquivos 32
   chmod -R a+rX $D
   ```
5. **Série do `pi_mpi` (Parte 1):**
   ```bash
   cd parte1/codigo_mpi && module load gnu15 openmpi5 && make
   ./speedup_2rodadas.sh && python3 analisa_speedup.py
   ```
6. **Série do corpus (Parte 2):** 18 jobs encadeados (6 configurações × 3 rodadas).
   ```bash
   cd pipeline
   DADOS=/opt/ohpc/pub/grupo/dados/amazon PARTICOES=128 TEMPO=00:10:00 ./serie_corpus.sh
   squeue                                     # acompanhe até esvaziar
   python3 analisa_corpus.py                  # gera resultados/speedup_corpus.csv (mediana)
   cp ../parte1/resultados/speedup.csv resultados/speedup.csv
   python3 plota_curvas.py resultados/speedup_corpus.csv resultados/speedup.csv   # exige matplotlib
   ```
   O ambiente `hpc` não tem matplotlib. Instale fora do ambiente compartilhado, por exemplo `pip install --target ~/pylib matplotlib` e `PYTHONPATH=~/pylib`.

Escada de configurações: 1, 2 e 4 workers em 1 nó; 8 em 2 nós (4 por nó); 16 em 4 nós (4 por nó); 32 em 4 nós (8 por nó, SMT).

Definições dos CSVs (formato da Aula 3: `nprocs,nnodes,t_total,t_serial,t_calc`): `t_calc` = etapas estreitas (tokenização, stopwords, TF, TF-IDF); `t_serial` = `t_total − t_calc`; o tempo conta depois do `wait_for_workers`. `resultados/speedup.csv` (`pi_mpi`) usa o **menor** tempo de 2 rodadas, e `speedup_corpus.csv` usa a **mediana** de 3. Os arquivos `parte1/resultados/*0909*` vêm de uma série anterior, sem posição controlada e com jobs concorrentes, e não foram usados.

## Como cada integrante adiciona a sua análise

A análise individual fica em `analise/<nome-sobrenome>.md` (uma por integrante, escrita por cada um; os números do grupo estão em `analise/DADOS-DO-GRUPO.md`).

1. Faça um **fork** deste repositório (botão *Fork* no GitHub) e clone o seu fork:
   ```bash
   git clone https://github.com/<seu-usuario>/hpc-aula05.git && cd hpc-aula05
   ```
2. Crie o seu arquivo `analise/<nome-sobrenome>.md` e faça commit e push no seu fork:
   ```bash
   git add analise/<nome-sobrenome>.md
   git commit -m "Analise individual: <nome>"
   git push origin main
   ```
3. Abra um **Pull Request** do seu fork para `pedroauler12/hpc-aula05` (branch `main`). O dono do repositório faz o merge, e a análise passa a fazer parte do repositório entregue.

Alternativa sem fork: peça ao dono do repositório para adicionar você em *Settings → Collaborators* e faça o push direto.

## Segurança

Nunca comite a `munge.key`; não abra as portas 8786 e 8787 no Wi-Fi (use túnel SSH); o scheduler roda dentro do job e morre com ele; leia os scripts antes de rodar com `sudo`.
