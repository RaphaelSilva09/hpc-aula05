#!/bin/bash
# job_hello_dask.sbatch - HPC Aula 4 - Prof. João Luisi
# Sobe um cluster Dask DENTRO de um job do SLURM, roda o hello e desmonta tudo.
#
#SBATCH -J hello-dask
#SBATCH -N 4                      # quatro nos: c1 a c4
#SBATCH --ntasks-per-node=1       # um "lancador" de workers por no
#SBATCH --cpus-per-task=8         # o no inteiro (8 CPUs logicas) para o lancador
#SBATCH --exclusive               # ninguem mais usa os nos enquanto isso roda
#SBATCH -t 00:10:00               # seguro contra o proprio erro: mata em 10 min
#SBATCH -o hello-dask-%j.out

WORKERS_POR_NO=4                  # um por core fisico (Aula 3: o teto real e 16)
TOTAL=$(( SLURM_NNODES * WORKERS_POR_NO ))

# 1) Ambiente Python do NFS (o mesmo caminho existe em todos os nos)
source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc

# 2) O arquivo onde o scheduler escreve o proprio endereco.
#    Fica na pasta de submissao, que precisa ser compartilhada (NFS).
cd "$SLURM_SUBMIT_DIR"
SCHED="$SLURM_SUBMIT_DIR/scheduler-$SLURM_JOB_ID.json"
rm -f "$SCHED"

# 3) Scheduler: um processo so, no primeiro no da alocacao (aqui mesmo).
dask scheduler --scheduler-file "$SCHED" --port 8786 --dashboard-address :8787 \
    > "scheduler-$SLURM_JOB_ID.log" 2>&1 &

# 4) Workers: o srun dispara um lancador em cada no; cada lancador cria
#    WORKERS_POR_NO processos de 1 thread. /tmp dos nos e RAM (nos stateless).
srun --ntasks="$SLURM_NNODES" --ntasks-per-node=1 --cpu-bind=none \
    dask worker --scheduler-file "$SCHED" \
        --nworkers "$WORKERS_POR_NO" --nthreads 1 \
        --local-directory "/tmp/dask-$SLURM_JOB_ID" \
    > "workers-$SLURM_JOB_ID.log" 2>&1 &

# 5) Cliente: o programa de verdade. Ele espera os workers aparecerem.
python hello_dask.py --scheduler-file "$SCHED" --workers "$TOTAL" --nos "$SLURM_NNODES"
STATUS=$?

# 6) Fim: ao sair do script o SLURM derruba scheduler e workers.
rm -f "$SCHED"
exit $STATUS
