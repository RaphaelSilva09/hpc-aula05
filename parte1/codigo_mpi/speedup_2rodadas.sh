#!/bin/bash
# speedup_2rodadas.sh - o mesmo experimento do speedup.sh (posicao controlada), mas as DUAS rodadas
# que o roteiro pede ficam encadeadas (--dependency=afterany) na mesma cadeia: nunca dois jobs juntos.
# Nao altera o speedup.sh original.
set -e
mkdir -p resultados
export PONTOS
[ -f resultados/speedup.csv ] || echo "nprocs,nnodes,t_total,t_serial,t_calc" > resultados/speedup.csv
dep=""
submete() { local n=$1; shift; local id
    id=$(sbatch --parsable $dep -n "$n" "$@" job_pi.sbatch)
    echo "n=$n  job $id  ($*)"; dep="--dependency=afterany:$id"; }
for rodada in 1 2; do
  echo "--- rodada $rodada"
  submete  1 -N 1 --hint=nomultithread
  submete  2 -N 1 --hint=nomultithread
  submete  4 -N 1 --hint=nomultithread
  submete  8 -N 2 --ntasks-per-node=4 --hint=nomultithread
  submete 16 -N 4 --ntasks-per-node=4 --hint=nomultithread
  submete 32 -N 4 --ntasks-per-node=8 --ntasks-per-core=2
done
