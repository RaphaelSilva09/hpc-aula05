#!/bin/bash
# serie_corpus.sh - 6 configuracoes x 3 rodadas = 18 jobs encadeados (--dependency=afterany),
# com as rodadas INTERCALADAS: uma interferencia passageira nao contamina uma configuracao inteira.
#
# Uso:  DADOS=/opt/ohpc/pub/grupo/dados/corpus ./serie_corpus.sh
#       TEMPO=00:45:00 PARTICOES=128 DADOS=... ./serie_corpus.sh
#
# Cada configuracao: "<nos> <workers_por_no>". Ate 16 workers, um por core fisico;
# em 32, dois por core (SMT). Sem fixar -N e workers por no a curva mente.
set -e
: "${DADOS:?defina DADOS (pasta parquet visivel nos 4 nos)}"
export DADOS PARTICOES="${PARTICOES:-128}" COLUNA="${COLUNA:-texto}" STOPWORDS_ARQ="${STOPWORDS_ARQ:-}"
TEMPO="${TEMPO:-00:30:00}"
mkdir -p resultados logs

dep=""
for rodada in 1 2 3; do
  for cfg in "1 1" "1 2" "1 4" "2 4" "4 4" "4 8"; do
    set -- $cfg
    id=$(RODADA=$rodada WORKERS_POR_NO=$2 sbatch --parsable $dep -N "$1" -t "$TEMPO" job_corpus.sbatch)
    echo "rodada $rodada: $(( $1 * $2 )) workers ($1 nos x $2) -> job $id"
    dep="--dependency=afterany:$id"
  done
done
echo
echo "18 jobs na fila. Acompanhe com: squeue   |   depois: python3 analisa_corpus.py"
