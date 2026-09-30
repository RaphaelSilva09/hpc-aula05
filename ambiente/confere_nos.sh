#!/bin/bash
# confere_nos.sh - pergunta a cada no se ele enxerga o ambiente do NFS.
# Roda no master, como usuario comum (nao root), de dentro da pasta do grupo.
# Saida esperada: quatro linhas, uma por no, todas com a mesma versao.
srun -N 4 --ntasks-per-node=1 -t 00:02:00 bash -c '
    source /opt/ohpc/pub/apps/miniforge3/bin/activate hpc
    python -c "import socket, sys, dask, distributed; print(socket.gethostname(), \"| python\", sys.version.split()[0], \"| dask\", dask.__version__, \"|\", sys.executable)"
' | sort
