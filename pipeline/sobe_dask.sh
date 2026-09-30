# sobe_dask.sh - trecho comum dos jobs Dask. Use com "source sobe_dask.sh"
# DENTRO de um script sbatch, depois de definir WORKERS_POR_NO.
# Entrega duas variaveis: SCHED (arquivo do scheduler) e TOTAL (workers).
#
# Diferenca em relacao ao da Aula 4: --memory-limit 0. O slurm.conf deste
# cluster nao declara RealMemory (o SLURM acha que o no tem 1 MB), o Dask le
# esse limite do cgroup e deixa cada worker com 128 KiB. Ver o README.
: "${WORKERS_POR_NO:=4}"
TOTAL=$(( SLURM_NNODES * WORKERS_POR_NO ))

source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc
cd "$SLURM_SUBMIT_DIR"
mkdir -p resultados logs
SCHED="$SLURM_SUBMIT_DIR/scheduler-$SLURM_JOB_ID.json"
rm -f "$SCHED"

dask scheduler --scheduler-file "$SCHED" --port 8786 --dashboard-address :8787 \
    > "logs/scheduler-$SLURM_JOB_ID.log" 2>&1 &

srun --ntasks="$SLURM_NNODES" --ntasks-per-node=1 --cpu-bind=none \
    dask worker --memory-limit 0 --scheduler-file "$SCHED" \
        --nworkers "$WORKERS_POR_NO" --nthreads 1 \
        --local-directory "/tmp/dask-$SLURM_JOB_ID" \
    > "logs/workers-$SLURM_JOB_ID.log" 2>&1 &

echo "cluster Dask: $SLURM_NNODES nos x $WORKERS_POR_NO workers = $TOTAL | scheduler em $(hostname):8786 | painel em $(hostname):8787"
