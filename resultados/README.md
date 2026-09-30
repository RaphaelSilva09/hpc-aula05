# resultados/

| Arquivo | O que é |
|---|---|
| `speedup.csv` | **Parte 1**: `pi_mpi`, 6 pontos (1, 2, 4, 8, 16, 32 processos), menor tempo de 2 rodadas. Colunas `nprocs,nnodes,t_total,t_serial,t_calc`. |
| `speedup_bruto.csv` | as 12 execuções do `pi_mpi` que originaram o `speedup.csv` (2 rodadas) |
| `corpus_bruto.csv` | **Parte 2**: as 18 execuções do corpus (6 configurações × 3 rodadas), com o tempo de cada etapa (`t_leitura`, `t_tokeniza`, `t_stopwords`, `t_tf`, `t_df`, `t_tfidf`, `t_stats`) e a subida do cluster (`t_sobe`) |
| `speedup_corpus.csv` | **Parte 2**: a **mediana** por configuração (6 linhas), no mesmo formato do CSV da Aula 3 |
| `curvas.png` | gráfico de 3 curvas: speedup do corpus, speedup do `pi_mpi` e linear ideal (eixo x em log₂) |
| `speedup_pi_mpi.png` | gráfico de speedup da Parte 1 (saída do `analisa_speedup.py`) |

Definições de `t_total`, `t_serial` e `t_calc` do corpus: ver o README da raiz (seção Pipeline).
A estatística usada difere entre as partes: o `pi_mpi` usa o **menor** tempo (roteiro da Aula 3) e o corpus usa a **mediana** (enunciado da Parte 2).
As estatísticas do corpus (vocabulário, distribuição de tamanho, termos de maior TF-IDF) são impressas nos logs de cada execução, em `logs_serie/`.
