# Problemas encontrados e como foram resolvidos

1. **Nós marcados `DOWN` depois de um reboot.** Era só estado do SLURM. `scontrol update nodename=c[1-4] state=resume`.
2. **`/opt` montado `ro` nos nós.** O job morre em 1 s, sem saída, porque o SLURM não consegue criar o arquivo de saída. Solução: rodar em `/home/<usuario>` (rw).
3. **`slurm.conf` sem `RealMemory`.** O SLURM assumia 1 MB por nó e aplicava `RLIMIT_RSS` de 1024 KB aos jobs. O Dask lê esse limite e, com ele, os workers ficavam com 128 KiB e qualquer resultado maior que ~500 KB voltando ao cliente falhava (`StreamBufferFullError`). **Correção:** `RealMemory=7000` em `NodeName=c[1-4]` e `scontrol reconfigure` (backup em `/etc/slurm/slurm.conf.bak-20260930`). Testes que **não** resolveram: `--memory-limit 0`, `--mem=0`, `--propagate=NONE` e `ulimit`.
4. **Como enviar o IDF aos workers.** O IDF é um dicionário grande (196 mil termos). Medido em 111 mil termos, 4 workers: `Future` em `bag.map` levou 175 s (o Dask resolve o Future a cada documento); dicionário ou `delayed` como argumento de `map_partitions` levou 7 s, e com 300.000 documentos o job **travou por mais de 10 minutos**; `client.run` guardando o IDF no objeto `worker` (uma cópia por worker) levou **0,17 s**, com resultado idêntico ao método simples (conferido em 120.000 documentos). É o método usado. O custo de mover esse dicionário cresce com o número de workers e entra na análise de gargalos (serialização).

Ver também `../analise/pedro-auler.md`, seção 6.
