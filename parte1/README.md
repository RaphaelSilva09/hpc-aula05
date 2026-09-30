# parte1/: MPI e SLURM (Aula 3)

| Item | Onde |
|---|---|
| Código (hello, ping-pong, soma_reduce, pi_mpi, Makefile, job scripts, `speedup.sh`, `analisa_speedup.py`) | `codigo_mpi/` |
| Tabelas dos Blocos 2 e 3 | `tabelas-blocos-2-e-3.md` |
| `speedup.csv` (6 pontos do `pi_mpi`) e gráfico | `resultados/speedup.csv`, `resultados/speedup_pi_mpi.png` |
| As cinco respostas do item 4.4 | `respostas-4.4-RASCUNHO.md` (**rascunho, para revisão do grupo**) |
| Saídas originais dos jobs de 09/09 | `resultados/*.out` |

## Como reproduzir a série do `pi_mpi`
```bash
cd parte1/codigo_mpi
module load gnu15 openmpi5 && make
./speedup_2rodadas.sh          # 12 jobs encadeados (2 rodadas), posição dos processos controlada
python3 analisa_speedup.py     # tabela, f de Amdahl e speedup.png
```
`speedup_2rodadas.sh` mantém o mesmo mapeamento do `speedup.sh` original (1, 2 e 4 processos em 1 nó; 8 em 2 nós; 16 em 4 nós; 32 em 4 nós com SMT), mas encadeia as duas rodadas na mesma cadeia de dependências, para que nunca haja dois jobs ao mesmo tempo.

## Sobre os arquivos de 09/09
`resultados/speedup_0909_rascunho_concorrente.csv` (sem cabeçalho, fora de ordem) e `speedup_reconstruido_0909.csv` vêm de uma primeira série em que os processos **não tiveram a posição controlada** e os jobs **rodaram ao mesmo tempo**. Não foram usados nas análises. Ficam como registro.
