# analise/: análises individuais

Cada integrante entrega **o seu** arquivo `analise/<nome>.md`. Os números do grupo estão em `DADOS-DO-GRUPO.md`; a interpretação é individual.

Cada análise deve conter (enunciado):
1. o gráfico com **três curvas**: speedup do corpus, speedup do `pi_mpi` e linear ideal (`../resultados/curvas.png`);
2. a discussão dos **gargalos** que explicam a distância entre as curvas: I/O no NFS, shuffle e serialização, rede gigabit, hyperthreading a partir de 16 workers e fração serial. Para cada um: onde aparece na curva e qual número prova;
3. a **estimativa da fração serial** do pipeline pela lei de Amdahl.

Arquivos: `nome-sobrenome.md` (um por integrante).
