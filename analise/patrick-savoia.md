# Análise individual: Patrick Savoia

HPC, Aula 5 (ponderada), item individual da Parte 2. Os números são os medidos pelo grupo (`resultados/speedup_corpus.csv`, `resultados/corpus_bruto.csv` e `resultados/speedup.csv`); a interpretação e as estimativas são minhas.

## Resumo

- O pipeline TF-IDF em Dask **não escala** neste cluster: fica mais lento que 1 worker entre 2 e 8 workers (S = 0,58 a 0,81) e chega a só **S(32) = 1,21**. O `pi_mpi` chega a **21,6** com os mesmos 32 processos.
- O problema não está no processamento de texto em si: as etapas estreitas (tokenização, stopwords e TF) escalam de 8× a 16×. O tempo está nas duas **agregações** (`df` e `stats`), que ocupam de 74% a 85% do tempo em toda configuração com mais de 1 worker.
- O primeiro custo que aparece é a **serialização** entre processos, e não a rede: `df` e `stats` já pioram de 1 para 2 workers **no mesmo nó**.
- Pela lei de Amdahl, estimo a fração serial do pipeline em **f ≈ 0,62**, o que dá um teto de 1/f ≈ **1,6×** mesmo com infinitos workers. O `pi_mpi` tem f ≈ 0,013.

## 1. Gráfico: speedup do corpus, do `pi_mpi` e linear ideal

![speedup: corpus, pi_mpi e linear ideal](../resultados/curvas.png)

| p | nós | T corpus (s) | S corpus | efic. | T pi_mpi (s) | S pi_mpi | efic. |
|---|---|---|---|---|---|---|---|
| 1 | 1 | 27,34 | 1,00 | 100% | 34,87 | 1,00 | 100% |
| 2 | 1 | 47,16 | 0,58 | 29% | 17,48 | 1,99 | 100% |
| 4 | 1 | 37,75 | 0,72 | 18% | 9,45 | 3,69 | 92% |
| 8 | 2 | 33,58 | 0,81 | 10% | 4,76 | 7,33 | 92% |
| 16 | 4 | 26,98 | 1,01 | 6% | 2,36 | 14,78 | 92% |
| 32 | 4 | 22,51 | 1,21 | 4% | 1,62 | 21,57 | 67% |

**As duas curvas não usam a mesma estatística.** O `pi_mpi` usa o **menor** tempo de 2 rodadas (roteiro da Aula 3), e o corpus usa a **mediana** de 3 rodadas (Parte 2). Para ver se isso muda a conclusão, recalculei o corpus com o menor tempo de cada ponto (as 18 rodadas estão em `corpus_bruto.csv`): S = 1; 0,65; 0,73; 0,84; 1,14; 1,20. As diferenças aparecem nos pontos que mais variam entre rodadas (com 2 workers, de 41,5 s a 48,6 s), mas o formato da curva é o mesmo.

### Onde o tempo vai

Speedup **de cada etapa** (T₁ da etapa / Tₚ da etapa, medianas):

| etapa | tipo | p=2 | p=4 | p=8 | p=16 | p=32 |
|---|---|---|---|---|---|---|
| tokeniza | estreita | 1,87 | 3,11 | 3,81 | 12,8 | 15,7 |
| stopwords | estreita | 1,94 | 2,29 | 3,73 | 10,5 | 11,6 |
| tf | estreita | 1,34 | 2,32 | 2,57 | 5,38 | 8,19 |
| tfidf | estreita + envio do IDF | 1,57 | 1,90 | 2,06 | 2,06 | **1,13** |
| leitura | I/O no NFS | 1,31 | 1,35 | **0,83** | **0,49** | 1,06 |
| df | larga (`frequencies`) | **0,45** | 0,58 | 0,67 | 0,95 | 1,03 |
| stats | redução | **0,32** | 0,38 | 0,45 | 0,60 | 0,66 |

Com 1 worker, `df` e `stats` somam 54% do tempo; com 2 workers ou mais, passam a somar de 74% a 85%. As etapas estreitas ficam até 15× mais rápidas, mas eram só 37% do tempo com 1 worker (`t_calc` = 10,2 s de 27,3 s) e com 32 workers somam 2 s de 22,5 s.

## 2. Gargalos que explicam a distância entre as curvas

Em ordem de peso, do maior para o menor.

### 2.1 Shuffle e serialização (o maior, e o primeiro a aparecer)

**Onde na curva:** na queda de S = 1,00 para S = 0,58 ao passar de 1 para 2 workers.

**Número:** `df` passa de 7,3 s para 16,3 s e `stats` de 7,3 s para 23,0 s, com as duas configurações em **1 nó só**. Ou seja, a rede física não participa.

**Por quê:** com 1 worker, os resultados parciais das 128 partições ficam na memória do mesmo processo, e a agregação só lê dicionários que já estão ali. Com 2 workers, `frequencies` e `reduction` montam uma árvore de combinação, e parte dos dicionários parciais precisa passar de um processo para outro. Para isso, cada `dict` Python de dezenas de milhares de termos é serializado com pickle, enviado por TCP (loopback, neste caso) e desserializado do outro lado. Esse trabalho gasta CPU em Python, e cada worker tem 1 thread (`--nthreads 1`). No fim do `df`, o vocabulário de **196.361 termos** ainda volta inteiro ao cliente.

Depois do salto, `df` e `stats` **melhoram devagar** (`stats`: 23,0 → 19,4 → 16,2 → 12,2 → 11,2 s). Minha interpretação é que o custo de serializar também se divide entre os workers, porque cada um processa pedaços menores ao mesmo tempo. Por isso a curva volta a subir depois de p = 2, só que a partir de um patamar pior que o de 1 worker. O Karp-Flatt decrescente mostra exatamente isso (seção 3.3).

### 2.2 Fração serial

**Onde na curva:** no teto. Mesmo que a comunicação não custasse nada, o corpus não passaria muito de 1,5×.

**Número:** com 1 worker, leitura + `df` + `stats` = 2,25 + 7,31 + 7,35 = **16,9 s de 27,3 s (62%)**. Com 32 workers, essas três etapas têm speedup entre 0,66 e 1,06: na prática, não escalam.

O `pi_mpi` tem outra estrutura. Cada processo calcula a sua fatia de forma independente e, no fim, troca **um `double`** (`MPI_Reduce`); o `t_serial` dele é de milésimos de segundo (0,007 s com 32 processos). No TF-IDF, calcular o DF e somar o TF-IDF por termo exige juntar todos os documentos no mesmo dicionário global. A fração serial vem do próprio algoritmo, e não só da implementação.

### 2.3 I/O no NFS

**Onde na curva:** a leitura não melhora com mais workers e fica instável quando o job usa vários nós (8 e 16 workers).

**Números:**
- A leitura levou de 1,7 a 2,3 s em 1 nó, 2,7 s em 2 nós e, em 4 nós com 16 workers, teve mediana de **4,6 s**, com rodadas de **2,1 s, 4,6 s e 6,4 s**. Foi a etapa que mais variou em toda a série.
- O limite físico é o cabo do master, que serve o NFS aos 4 nós. O ping-pong da Aula 3 mediu **107 MB/s** entre nós, então os 83 MB do corpus levam no mínimo 83 / 107 ≈ **0,8 s**, com qualquer número de workers. Esse piso não diminui com p, então conta como tempo serial.
- A etapa não é só transferência de bytes: inclui decodificar o Parquet, reparticionar de 32 arquivos para 128 partições e criar 300 mil strings Python no `to_bag`. Por isso nunca fica abaixo de ~1,7 s.

Essa variação distorce o último ponto do gráfico. De 16 para 32 workers o `t_total` caiu 4,5 s, e **2,4 s dessa queda vieram só da leitura** (4,56 → 2,13 s). Tirando a leitura, o ganho de 16 para 32 é de ~1,10× (22,4 → 20,4 s).

Há um custo parecido **fora do `t_total`**: a subida do cluster Dask (`t_sobe`) leva ~3 s em 1 nó e **33 a 37 s** a partir de 2 nós. Não isolei a causa; as hipóteses são o `srun` iniciando os workers remotos, a importação do ambiente Python (que está no NFS) nos outros nós e o registro dos workers no scheduler. Se esse tempo entrasse na conta, S(32) = (27,34 + 2,93) / (22,51 + 32,30) = **0,55**.

### 2.4 Rede gigabit

**Onde na curva:** de 4 para 8 workers, quando o job passa de 1 para 2 nós.

**Número:** entre nós, a latência é ~1000× maior que dentro do mesmo nó (235 µs contra 0,24 µs para 1 byte, no ping-pong), e a banda fica em ~107 MB/s. Mesmo assim, ao passar para 2 nós, `df` **melhorou** (12,6 → 10,9 s) e `stats` também (19,4 → 16,2 s).

**Conclusão:** a rede tem custo, mas **não é o gargalo dominante**. Se fosse, as agregações piorariam ao cruzar para 2 nós, e elas melhoraram. A piora grande veio antes, ainda dentro de um nó (seção 2.1). A rede pesa mais onde todo o tráfego passa pelo **mesmo cabo**: o NFS do master (seção 2.3) e o retorno do resultado ao cliente.

### 2.5 Hyperthreading a partir de 16 workers

**Onde na curva:** entre 16 e 32 workers. Com 16, cada worker tem um core físico (4 nós × 4 cores); com 32, os workers extras dividem cada core com a segunda thread (SMT).

**Números:**
- `pi_mpi`: de 16 para 32 processos, o ganho foi de **1,46×**, e não de 2×; a eficiência caiu de 92% para 67%. Como o `pi_mpi` quase não se comunica, esse é o efeito do SMT isolado.
- Corpus: as etapas estreitas ganharam na mesma ordem (`tokeniza` 1,23×, `stopwords` 1,10×, `tf` 1,52×), mas o `t_calc` **piorou** (1,65 → 2,01 s) por causa do `tfidf`, que subiu de 0,72 s para **1,31 s**. No início do `tfidf`, o `client.run(guarda_idf, idf)` envia uma cópia do dicionário IDF (196 mil termos) para **cada** worker. Com 32 workers são 32 cópias serializadas, o dobro de 16: um custo que **cresce com p**.
- Memória: com 8 workers por nó, são 8 cópias do IDF, além das partições, nos ~4,1 GB livres de cada nó.

No corpus, o efeito do SMT fica escondido atrás do envio do IDF e da variação da leitura.

## 3. Estimativa da fração serial pela lei de Amdahl

Pela lei de Amdahl, se uma fração f do trabalho é serial:

S(p) = 1 / (f + (1 − f)/p), ou seja, 1/S = f + (1 − f)·(1/p)

Então 1/S é uma reta em 1/p com intercepto f, e o teto do speedup é 1/f.

### 3.1 O ajuste direto não funciona, e o motivo é informativo

O ajuste por mínimos quadrados em todos os pontos (`analisa_corpus.py`) dá **f = 1** (truncado): o valor ajustado passaria de 1. Isso acontece porque S(2) = 0,58 < 1, e Amdahl nunca prevê S < 1, já que no modelo acrescentar processadores não deixa o programa mais lento. O modelo supõe que o trabalho total é o mesmo com qualquer p e só é dividido. Aqui o trabalho **aumenta** ao sair de 1 worker, porque surge a serialização da seção 2.1, que não existe com p = 1.

Para comparar: no `pi_mpi`, o mesmo ajuste dá f = 0,013 (pontos de 1 a 16, teto de ~79×) e prevê S(32) = 22,95, contra 21,57 medido. Lá Amdahl funciona, e o erro de 6% vem do SMT.

### 3.2 Minha estimativa: f pela decomposição em etapas

Como o ajuste global falha, estimei f diretamente: com 1 worker, qual fração do tempo está em etapas que, na prática, não escalam (speedup de ~1 ou menos com 32 workers, pela tabela da seção 1):

f ≈ (t_leitura + t_df + t_stats) / t_total = (2,25 + 7,31 + 7,35) / 27,34 = **0,62**

O resultado coincide com `t_serial / t_total` do CSV: 16,97 / 27,34 = 0,62.

| p | Amdahl (f = 0,62) | medido | diferença |
|---|---|---|---|
| 2 | 1,23 | 0,58 | −53% |
| 4 | 1,40 | 0,72 | −49% |
| 8 | 1,50 | 0,81 | −46% |
| 16 | 1,55 | 1,01 | −35% |
| 32 | 1,58 | 1,21 | −23% |

**Teto: 1/0,62 ≈ 1,6×.** Mesmo com infinitos workers e comunicação sem custo, este pipeline, do jeito que está escrito, não passaria de ~1,6×. A curva medida fica **abaixo** da previsão em todos os pontos. A diferença é o overhead de distribuição (serialização, envio do IDF, NFS), que Amdahl não modela, e ela **diminui** com p (de −53% para −23%), como faz um custo fixo dividido entre mais workers.

Essa estimativa vale para a implementação medida. `df` e `stats` têm uma parte que poderia ser paralela (a contagem dentro de cada partição), então o algoritmo, em si, teria um f menor. Mas, na v1, essas etapas não escalam, e para prever o comportamento neste cluster o valor a usar é 0,62.

### 3.3 Verificação por Karp-Flatt

A fração serial experimental de Karp-Flatt, e(p) = (1/S − 1/p) / (1 − 1/p):

| p | 2 | 4 | 8 | 16 | 32 |
|---|---|---|---|---|---|
| e(p) | 2,45 | 1,51 | 1,26 | 0,99 | 0,82 |

Se Amdahl valesse, e(p) seria constante e igual a f. Aqui:
1. e(p) **passa de 1** até p = 8, o que não faz sentido para uma fração e só acontece porque S < 1;
2. e(p) **diminui** com p, o que indica um custo fixo pago ao sair de 1 worker e depois dividido. Um overhead que crescesse com p (rede, por exemplo) faria e(p) aumentar.

Com p = 32, e = 0,82. Fica acima do 0,62 da seção 3.2 porque inclui também o overhead de distribuição. As duas estimativas concordam na ordem de grandeza: **entre 60% e 80% do tempo deste pipeline se comporta como serial.**

### 3.4 Ajuste só no regime distribuído (p ≥ 2)

Também ajustei T(p) = T_s + T_p/p deixando de fora o ponto de 1 worker. O f muda conforme os pontos incluídos: 0,33 (p de 2 a 32), 0,25 (4 a 32) e 0,14 (8 a 32). Se o f depende do intervalo escolhido, ele não representa uma fração serial, e sim um modelo que não se ajusta aos dados: o overhead, que também se divide entre os workers, entra no ajuste como se fosse trabalho paralelo. Por isso fico com a estimativa da seção 3.2.

## 4. Conclusão

Os gargalos têm uma ordem de importância. O maior é a fração serial alta (f ≈ 0,62, teto de ~1,6×): DF e estatísticas precisam juntar o corpus inteiro em um dicionário global. Sobre ela, a implementação paga um custo de serialização que só aparece com mais de 1 worker e mantém a curva abaixo de 1 até 8 workers. NFS, rede gigabit e SMT têm efeito mensurável (piso de ~0,8 s na leitura, variação de 2 a 6 s em 4 nós, 1,46× de ganho com SMT no `pi_mpi`), mas pesam menos que os dois primeiros. O `pi_mpi` chega a 21,6× porque tem o perfil oposto: cálculo pesado e independente em cada processo, com um único número trocado no fim.

**Limites:** esta é a v1 do pipeline (`frequencies` e `reduction` com os parâmetros padrão do Dask; nenhuma variante foi medida). Não houve baseline serial sem Dask: o "1 worker" é o mesmo código com 1 worker. A causa dos ~33 s de `t_sobe` com vários nós não foi isolada.
