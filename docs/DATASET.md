# Dataset

**Corpus usado nas medições:** subconjunto de 300.000 resenhas do Amazon Polarity.

| Campo | Valor |
|---|---|
| Nome | Amazon Polarity (resenhas de produtos da Amazon), primeiras 300.000 do arquivo de treino `train-00000-of-00004` |
| Fonte | Hugging Face, `fancyzhx/amazon_polarity`: https://huggingface.co/datasets/fancyzhx/amazon_polarity |
| Licença e citação | a confirmar no cartão do dataset (Hugging Face) |
| Documentos | 300.000 (o arquivo inteiro tem 900.000 e o dataset completo, 3,6 milhões) |
| Tamanho em disco | 83 MB em 32 arquivos Parquet (arquivo original: 259.761.770 bytes) |
| Formato | Parquet, uma coluna `texto` (título + conteúdo da resenha) |
| Idioma | inglês |
| Tamanho médio | ~79 palavras por documento |
| SHA-256 do original | `57c367f8c74210dde3742b17d103af33820df3af39d029f2a5051a6f87810661` |
| Local no cluster | `/opt/ohpc/pub/grupo/dados/amazon` (visível nos 4 nós) |
| Vocabulário (depois das stopwords) | 196.361 termos |

O subconjunto é determinístico (as primeiras 300.000 linhas), e a conversão é feita uma vez, fora da medição, por `pipeline/prepara_dataset.py --max-docs 300000`.

## Por que não o AG News

O primeiro corpus escolhido foi o **AG News** (Hugging Face, `fancyzhx/ag_news`, partição de treino: 120.000 notícias, 18 MB, inglês, SHA-256 do original `fc508d6d9868594e3da960a8cfeb63ab5a4746598b93428c224397080c1f52ee`). Ele atende o mínimo do enunciado (50.000 documentos), mas foi **descartado antes de qualquer medição da série**, por um teste de calibração com 1 worker e 128 partições (`calibracao/calibracao_agnews_1worker.out`):

| Corpus | Documentos | t_total com 1 worker | Parte que já não escala (leitura, DF, estatísticas) |
|---|---|---|---|
| AG News | 120.000 | **11,9 s** | 6,3 s (53%) |
| Amazon Polarity (subconjunto) | 300.000 | **27,8 s** | 17,5 s (63% na calibração; 62% na mediana da série) |

Com 11,9 s, os custos fixos dominariam a curva e o speedup ficaria limitado a cerca de 2×, sem informar sobre os gargalos. O guia da atividade recomenda um caso de 1 worker de pelo menos algumas dezenas de segundos (o `pi_mpi` levava ~35 s). O Amazon deu 27,8 s, na faixa esperada.

Regras seguidas: o corpus foi decidido **antes** da série e **não mudou** entre as configurações (problema de tamanho fixo, sem Gustafson). O tamanho do subconjunto (300.000, e não os 900.000 do arquivo) vem da memória: cada nó tem ~4,1 GB livres, e um worker precisa segurar as etapas intermediárias.
