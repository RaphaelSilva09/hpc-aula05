#!/bin/bash
# job_corpus.sbatch - HPC Aula 5 (ponderada). Um cluster Dask DENTRO do job, roda o pipeline, desmonta.
#
# Uso (o -N e o -t vao na linha de comando; quem chama e o serie_corpus.sh):
#   WORKERS_POR_NO=4 DADOS=/opt/ohpc/pub/grupo/dados/corpus sbatch -N 4 job_corpus.sbatch
#
#SBATCH -J corpus
#SBATCH --ntasks-per-node=1       # um lancador de workers por no
#SBATCH --cpus-per-task=8         # o no inteiro (8 CPUs logicas)
#SBATCH --exclusive               # ninguem mais usa os nos: medicao comparavel
#SBATCH -t 00:30:00               # seguro. Dimensione pelo caso de 1 worker, que e o mais lento.
#SBATCH -o logs/corpus-%j.out

INICIO_JOB=$(date +%s.%N); export INICIO_JOB
WORKERS_POR_NO=${WORKERS_POR_NO:-4}
DADOS=${DADOS:?defina DADOS=/caminho/da/pasta/parquet (visivel nos 4 nos)}
PARTICOES=${PARTICOES:-128}
COLUNA=${COLUNA:-texto}
RODADA=${RODADA:-1}
STOPWORDS_ARQ=${STOPWORDS_ARQ:-}

source sobe_dask.sh

python pipeline.py --scheduler-file "$SCHED" --workers "$TOTAL" --nos "$SLURM_NNODES" \
    --dados "$DADOS" --coluna "$COLUNA" --particoes "$PARTICOES" --rodada "$RODADA" \
    ${STOPWORDS_ARQ:+--stopwords "$STOPWORDS_ARQ"} \
    --csv resultados/corpus_bruto.csv
STATUS=$?
rm -f "$SCHED"
exit $STATUS
