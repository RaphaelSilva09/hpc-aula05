#!/bin/bash
# instala_ambiente.sh - HPC Aula 4 - Prof. João Luisi
#
# Instala o ambiente Python do cluster DENTRO do NFS (/opt/ohpc/pub).
# Roda no MASTER, como root (sudo), porque so o master tem internet.
# Os nos c1..c4 nao instalam nada: eles enxergam a mesma pasta pelo NFS.
#
# Uso:
#   sudo ./instala_ambiente.sh            # Dask (caminho da aula)
#   sudo ./instala_ambiente.sh --spark    # Dask + Spark (PySpark + OpenJDK 17)
set -euo pipefail

PREFIXO=/opt/ohpc/pub/apps/miniforge3
AMBIENTE=hpc
INSTALADOR=/tmp/Miniforge3-Linux-x86_64.sh
URL=https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh

PACOTES="python=3.11 dask distributed bokeh numpy pandas pyarrow scikit-learn"
if [[ "${1:-}" == "--spark" ]]; then
    PACOTES="$PACOTES pyspark openjdk=17 pandas<3"
fi

if [[ $EUID -ne 0 ]]; then
    echo "Rode com sudo: /opt/ohpc/pub pertence ao root." >&2
    exit 1
fi

if ! mountpoint -q /opt/ohpc/pub && [[ ! -d /opt/ohpc/pub ]]; then
    echo "/opt/ohpc/pub nao existe neste host. Este script roda no master." >&2
    exit 1
fi

if [[ ! -x $PREFIXO/bin/conda ]]; then
    echo ">> Baixando o Miniforge (uns 90 MB pelo Wi-Fi do master)"
    curl -L --fail -o "$INSTALADOR" "$URL"
    echo ">> Conferindo que o download e um script e nao uma pagina de erro"
    head -c 200 "$INSTALADOR" | grep -q "^#!/bin/sh" || { echo "Download invalido." >&2; exit 1; }
    echo ">> Instalando em $PREFIXO"
    mkdir -p /opt/ohpc/pub/apps
    bash "$INSTALADOR" -b -p "$PREFIXO"
else
    echo ">> Miniforge ja esta em $PREFIXO, pulando o download"
fi

if [[ ! -d $PREFIXO/envs/$AMBIENTE ]]; then
    echo ">> Criando o ambiente '$AMBIENTE' com: $PACOTES"
    "$PREFIXO/bin/mamba" create -y -n "$AMBIENTE" -c conda-forge $PACOTES
else
    echo ">> Ambiente '$AMBIENTE' ja existe, instalando o que faltar"
    "$PREFIXO/bin/mamba" install -y -n "$AMBIENTE" -c conda-forge $PACOTES
fi

# Todo mundo le e executa, so o root escreve.
chmod -R a+rX "$PREFIXO"

echo
echo ">> Pronto. Para usar em qualquer shell ou job:"
echo "   source $PREFIXO/bin/activate $AMBIENTE"
"$PREFIXO/envs/$AMBIENTE/bin/python" -c "import dask, distributed; print('   dask', dask.__version__, '| distributed', distributed.__version__)"
