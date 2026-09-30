# Análise individual: Diogo Burgierman

HPC, Inteli 2026.2 (Prof. João Luisi). Atividade ponderada da Aula 5, item individual.

Os números são os que o grupo mediu (`../resultados/` e `DADOS-DO-GRUPO.md`). O foco e a interpretação abaixo são meus.

## 1. A pergunta

No mesmo cluster, o `pi_mpi` chega a 21,6× com 32 processos, e o pipeline de TF-IDF chega a 1,2× com 32 workers. Minha hipótese é que a diferença vem das etapas que agregam o vocabulário. O custo delas depende do número de termos distintos do corpus (196.361), e esse número não diminui quando o trabalho é dividido entre mais workers.

## 2. As três curvas

![Speedup: corpus TF-IDF, pi_mpi e linear ideal](../resultados/curvas.png)

| workers | corpus t_total (s) | corpus speedup | corpus eficiência | `pi_mpi` speedup | `pi_mpi` eficiência |
|---|---|---|---|---|---|
| 1 | 27,34 | 1,00 | 100% | 1,00 | 100% |
| 2 | 47,16 | 0,58 | 29% | 1,99 | 100% |
| 4 | 37,75 | 0,72 | 18% | 3,69 | 92% |
| 8 | 33,58 | 0,81 | 10% | 7,33 | 92% |
| 16 | 26,98 | 1,01 | 6% | 14,78 | 92% |
| 32 | 22,51 | 1,21 | 4% | 21,57 | 67% |

O corpus usa a mediana de 3 rodadas, e o `pi_mpi`, o menor tempo de 2 rodadas. Recalculando o corpus com o menor tempo de cada configuração, como no `pi_mpi`, fica S(16) = 1,14 e S(32) = 1,20. A conclusão não muda.

## 3. O que escala e o que não escala

Separando o tempo por etapa (mediana), o pipeline se divide em duas partes que se comportam de forma oposta:

| etapa | tipo | S(2) | S(4) | S(8) | S(16) | S(32) |
|---|---|---|---|---|---|---|
| tokeniza | estreita | 1,87 | 3,11 | 3,81 | 12,60 | 15,81 |
| stopwords | estreita | 1,94 | 2,30 | 3,71 | 10,29 | 11,51 |
| tf | estreita | 1,35 | 2,34 | 2,59 | 5,47 | 8,26 |
| df | larga | 0,45 | 0,58 | 0,67 | 0,95 | 1,03 |
| stats | larga | 0,32 | 0,38 | 0,45 | 0,60 | 0,66 |

- **Etapas estreitas:** trabalham documento por documento e escalam de forma parecida com o `pi_mpi`. A tokenização chega a 15,8× com 32 workers.
- **Etapas largas:** o `df` conta em quantos documentos cada termo aparece, e o `stats` soma o TF-IDF de cada termo. Elas ficam **abaixo de 1** em quase toda a curva.
- **Peso no tempo total:** com 1 worker, `df` + `stats` já são 54% do `t_total`. Com 2 ou mais workers, ficam entre 74% e 85%. A curva do corpus é, basicamente, a curva dessas duas etapas.

## 4. O vocabulário como variável escondida

Nas duas etapas largas, cada partição gera um dicionário termo → valor, e depois os 128 dicionários são juntados num só, com todos os termos do corpus. O tamanho do que é juntado depende do **vocabulário**, e não do número de documentos:

- Com mais workers, cada worker recebe menos documentos, mas o dicionário final continua com 196.361 termos.
- Pela lei de Zipf, poucas palavras aparecem muito e a maioria aparece pouco. Por isso, cada partição já traz uma parte grande dos termos. Isso eu não medi: é o que eu esperaria dessa distribuição.

Para testar essa ideia, usei os dois testes de calibração com 1 worker (`../calibracao/`), feitos antes da série, com o AG News e com o Amazon:

| | AG News | Amazon | razão |
|---|---|---|---|
| documentos | 120.000 | 300.000 | 2,5× |
| vocabulário (depois das stopwords) | 61.488 | 196.361 | 3,2× |
| tokeniza | 1,08 s | 4,74 s | 4,4× |
| df | 2,06 s | 7,27 s | 3,5× |
| stats | 2,31 s | 7,71 s | 3,3× |
| parte que não escala (`t_serial`), sem contar o `tfidf` | 74% | 66% | |

O `tfidf` ficou fora da tabela e da última linha por causa da ordem dos jobs registrada na cronologia do Pedro (seção 6.1 da análise dele):
- a calibração do AG News (job 146) rodou antes da troca do envio do IDF para `client.run`;
- essa troca só foi feita depois do job 147;
- por isso o `tfidf` do AG News (3,46 s, mais que os 1,48 s do Amazon) foi medido com o método antigo e não é comparável.

- **Etapas largas:** `df` e `stats` cresceram de 3,3× a 3,5×. É mais que o crescimento do número de documentos (2,5×) e perto do crescimento do vocabulário (3,2×).
- **Tokenização:** depende só do texto de cada documento e cresceu 4,4×, porque as resenhas são mais longas que as notícias. Pelo histograma dos logs, a maioria das notícias tem de 20 a 29 tokens úteis, e as resenhas se espalham de 10 até mais de 70.
- **Os dois efeitos se misturam:** o `df` também percorre os termos de cada documento, então parte do custo dele acompanha o texto.
- **Parte que não escala:** foi **menor** no Amazon (66% contra 74%), mesmo com mais vocabulário.
  - As etapas estreitas, que dependem do texto, cresceram mais (4,4×) que as largas (3,3× a 3,5×).
  - O peso da parte que não escala depende do equilíbrio entre texto e vocabulário, e não só do vocabulário.

A qualidade da tokenização também pesa aqui. Entre os termos de maior TF-IDF do Amazon aparece "don", que vem de *don't* cortado pela regex. No AG News aparecem "quot", "gt" e "lt", restos de entidades HTML (`&quot;`, `&gt;`, `&lt;`). Cada pedaço desses vira um termo a mais no vocabulário e, portanto, mais custo nas etapas largas.

## 5. Os gargalos, um por um

### I/O no NFS
- **Onde aparece:** a leitura não diminui com mais workers: 2,25 s (1), 1,72 s (2), 1,67 s (4), 2,71 s (8), 4,56 s (16) e 2,13 s (32). Todos leem do master pelo mesmo cabo.
- **Número que prova:** os 83 MB do corpus levariam ~0,8 s a 107 MB/s, e a leitura mede de 2 a 6 vezes isso.
  - O valor de 16 workers é instável: as três rodadas deram 2,12 / 4,56 / 6,44 s.
  - Leio isso como disputa pelo NFS, que varia de uma rodada para outra, e não como tendência.
- **Peso:** de 4% a 17% do `t_total`. Pesa, mas não é o principal.
- **Fora do `t_total`:** a subida do cluster leva ~3 s em 1 nó e de 32 a 37 s com 2 ou 4 nós. Não investiguei a causa.

### Shuffle e serialização (o principal)
- **Onde aparece:** no salto de 1 para 2 workers, `df` vai de 7,31 s para 16,33 s, e `stats`, de 7,35 s para 23,01 s. Com 2 workers, o `t_total` (47,16 s) fica maior que o de 1 worker.
- **Número que prova:** esses 2 workers estão **no mesmo nó**, então a piora não é rede.
  - Com 1 worker, os dicionários parciais ficam no mesmo processo, e nada é serializado.
  - Com 2, eles precisam ser convertidos em bytes (pickle) e copiados entre processos.
- **Ligação com o vocabulário:** o pipeline já usa `frequencies`, que combina dentro da partição antes de mover, como na Aula 4. Mesmo assim, o que se move são dicionários com uma parte grande dos 196 mil termos.
- **Por que melhora pouco depois:** a contagem dentro das partições se divide entre os workers, mas a junção final num único dicionário e o envio dele ao cliente continuam sendo feitos por uma tarefa só.

### Rede gigabit
- **Medido na Parte 1 (ping-pong):** 235 µs de ida e volta para 1 B entre nós e 107,2 MB/s com 1 MiB.
- **Onde aparece:** ao passar de 1 para 2 nós (4 → 8 workers), o tempo total **melhora** (37,75 → 33,58 s), e `df` e `stats` também melhoram. Nesse volume, a rede não é o limite das etapas largas.
- **Onde a rede aparece:** no `tfidf`, que sobe de 0,72 s (16 workers) para 1,31 s (32). Nessa etapa, o dicionário de IDF, com os 196 mil termos, é enviado a cada worker.
  - Uma conta grosseira: 196.361 termos × ~20 bytes por termo ≈ 4 MB por cópia.
  - De 16 para 32 workers, são 12 cópias a mais saindo de c1 para os outros nós: ~48 MB ÷ 107 MB/s ≈ 0,45 s.
  - O medido foi +0,59 s. A ordem de grandeza bate. Não medi o tamanho real do dicionário.
  - É, de novo, o vocabulário, agora passando pela rede.

### Hyperthreading a partir de 16 workers
- **Por que 16 é o limite:** cada nó tem 4 cores físicos com 2 threads. Com 16 workers, os 16 cores estão ocupados, e o scheduler e o cliente, que rodam em c1, já disputam core com os workers. Com 32, são 2 workers por core.
- **`pi_mpi`:** a eficiência cai de 92% (16) para 67% (32). O ganho de 16 para 32 é de 1,46×, e o ideal seria 2×.
- **Corpus:** a tokenização ganha só 1,25× de 16 para 32 (0,37 → 0,30 s), e o `t_calc` até sobe (1,65 → 2,01 s, por causa do `tfidf`).
  - O ganho total de 16 para 32 (26,98 → 22,51 s) veio da leitura, que estava alta com 16, e de uma melhora pequena em `df` e `stats`.
  - Não veio do SMT.

### Fração serial
É o assunto da próxima seção. Resumindo: no `pi_mpi` ela é de ~1,3%, e no pipeline a parte que não escala já passa de metade do tempo com 1 worker.

## 6. Fração serial pela lei de Amdahl

A lei é S(p) = 1 / (f + (1 − f)/p). Escrita como 1/S = f + (1 − f)·(1/p), vira uma reta em 1/p, e f é o intercepto.

- **`pi_mpi` (Parte 1):** f ≈ 1,3% (ajuste com 1 a 16 processos), com teto de ~79×.
- **Corpus, ajuste da reta com os 6 pontos:** não funciona. Como o speedup é menor que 1, 1/S fica acima de 1, e o intercepto da reta dá 1,14. Como f não pode passar de 1, o script trunca em f = 1, o que não é uma estimativa útil.
- **Corpus, pela divisão do tempo com 1 worker:** f = `t_serial` / `t_total` = 16,97 / 27,34 ≈ **0,62**, contando leitura, df, stats e o overhead do scheduler.
  - Com esse f, Amdahl preveria S(32) = 1 / (0,62 + 0,38/32) ≈ 1,58 e um teto de 1/0,62 ≈ 1,6×.
  - O medido foi 1,21, abaixo da previsão. A parte "serial" não é fixa: ela aumenta ao sair de 1 worker (`df` + `stats`: 14,7 s com 1 worker, 39,3 s com 2).
- **Karp-Flatt:** e(p) = (1/S − 1/p) / (1 − 1/p) dá 2,45 (2), 1,51 (4), 1,26 (8), 0,99 (16) e 0,82 (32).
  - Se Amdahl valesse, e(p) seria constante.
  - Valores acima de 1 significam mais lento que 1 worker.
  - O valor cai conforme entram mais workers. Ou seja, o custo de juntar os dicionários é pago de uma vez ao sair de 1 processo e depois se dilui.
- **O f depende do corpus:** fazendo a mesma divisão na calibração, sem o `tfidf` (seção 4), dá 0,74 no AG News e 0,66 no Amazon. Isso combina com o que foi dito na Aula 3: f não é uma propriedade só do programa, muda com o dado. Aqui, ele depende do equilíbrio entre o texto de cada documento (etapas estreitas) e o vocabulário (etapas largas).

Minha estimativa é **f ≈ 0,6** para este corpus, quase 50 vezes a do `pi_mpi`. Mesmo assim, Amdahl descreve mal a curva, porque a parte que não escala não é constante.

## 7. Limites desta análise

- **A comparação de vocabulário:** tem só dois corpora, com textos de tipos diferentes, medidos com 1 worker. É um indício, não uma prova.
  - Não medi o tamanho dos dicionários parciais nem os bytes movidos.
  - O `tfidf` ficou fora da comparação porque, na calibração do AG News, ele foi medido com o método antigo de envio do IDF (seção 4).
  - Uma forma simples de testar a hipótese seria descartar os termos raros (por exemplo, os que aparecem em menos de 5 documentos) e ver se `df` e `stats` ficam mais rápidos.
- **Agregações:** usam `frequencies` e `reduction` com os parâmetros padrão do Dask. Uma versão otimizada dessas etapas não foi medida.
- **Baseline:** o caso de 1 worker é o mesmo código em Dask, e não um script serial puro.
- **Ruído:** os pontos de 2 e 16 workers são os mais variáveis entre as rodadas (15% e 16%).

## 8. Conclusão

As etapas que tratam um documento por vez escalam quase como o `pi_mpi`. As que juntam o vocabulário do corpus inteiro ficam mais lentas quando o trabalho sai de um processo e quase não melhoram depois. O custo delas acompanha o tamanho do vocabulário, que não se divide entre os workers. Por isso, a fração que não escala fica em torno de 0,6, e a curva do corpus fica perto de 1, enquanto a do `pi_mpi` chega a 21,6×.
