"""pipeline.py - HPC Aula 5 (ponderada) - TF-IDF de um corpus em Dask.

Cada etapa e cronometrada separadamente (persist + wait), para que a analise
consiga dizer ONDE esta o gargalo, e nao so QUE ele existe:

  leitura   I/O no NFS. Os workers leem as particoes direto (nada passa pelo cliente).
  tokeniza  ESTREITA. minusculas e regex. Python puro: escala como o pi.
  stopwords ESTREITA. remocao das stopwords (etapa separada, como pede o enunciado).
  tf        ESTREITA. Counter por documento.
  df        LARGA. Em quantos documentos cada termo aparece. Usa frequencies
            (combina dentro da particao e so move os contadores), NUNCA groupby.
  tfidf     ESTREITA. O IDF (dicionario pequeno) e enviado a todos os workers.
  stats     REDUCAO. So numeros pequenos voltam ao cliente.

Definicao usada no CSV (a mesma coluna da Aula 3, para o analisa reaproveitar):
  t_calc   = tokeniza + stopwords + tf + tfidf   (etapas estreitas)
  t_serial = t_total - t_calc                (leitura, shuffle do DF, reducao, scheduler)
  t_total  = de depois do wait_for_workers ate o fim da etapa stats.
  t_sobe   = do inicio do job ate os workers chegarem (fora do t_total).

"Termo de maior TF-IDF" = SOMA do TF-IDF do termo sobre todos os documentos.
"""
import argparse
import collections
import math
import os
import re
import statistics
import time

import dask
import dask.bag as db
import dask.dataframe as dd
from dask.distributed import Client, wait

TOKEN = re.compile(r"[^\W\d_]{2,}")     # palavras de 2+ letras (acentos incluidos)


def carrega_stopwords(arquivo):
    if arquivo:
        with open(arquivo, encoding="utf-8") as f:
            return frozenset(l.strip().lower() for l in f if l.strip())
    from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
    return frozenset(ENGLISH_STOP_WORDS)


def tokeniza(texto):
    """Etapa 1: minusculas e regex. Ainda com stopwords."""
    return TOKEN.findall(str(texto).lower())


def sem_stopwords(tokens, stop):
    """Etapa 2: remove as stopwords."""
    return [t for t in tokens if t not in stop]


def tf_de(tokens):
    """TF normalizado (frequencia relativa) de um documento."""
    n = len(tokens)
    if n == 0:
        return {}
    return {t: c / n for t, c in collections.Counter(tokens).items()}


def guarda_idf(idf, dask_worker):
    """Roda UMA vez em cada worker (client.run): o IDF fica no objeto worker, nao viaja com as tarefas."""
    dask_worker.idf = idf


def tfidf_particao(docs):
    from distributed import get_worker
    idf = get_worker().idf
    return [{t: v * idf[t] for t, v in d.items() if t in idf} for d in docs]


def soma_particao(docs):
    """perpartition da reducao: soma o TF-IDF de cada termo dentro da particao."""
    acc = collections.defaultdict(float)
    for d in docs:
        for t, v in d.items():
            acc[t] += v
    return dict(acc)


def junta_somas(dicts):
    acc = collections.defaultdict(float)
    for d in dicts:
        for t, v in d.items():
            acc[t] += v
    return dict(acc)


class Cronometro:
    def __init__(self):
        self.t = collections.OrderedDict()

    def etapa(self, nome, fn):
        t0 = time.perf_counter()
        r = fn()
        self.t[nome] = time.perf_counter() - t0
        print(f"  {nome:<9s} {self.t[nome]:8.3f} s", flush=True)
        return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scheduler-file", required=True)
    ap.add_argument("--workers", type=int, required=True)
    ap.add_argument("--nos", type=int, required=True)
    ap.add_argument("--dados", required=True, help="pasta com parquet (visivel nos 4 nos)")
    ap.add_argument("--coluna", default="texto")
    ap.add_argument("--particoes", type=int, default=128,
                    help="MESMO valor nas 6 configuracoes (problema de tamanho fixo)")
    ap.add_argument("--stopwords", default=None, help="arquivo, um termo por linha (padrao: ingles do sklearn)")
    ap.add_argument("--top", type=int, default=15)
    ap.add_argument("--csv", default=None, help="acrescenta uma linha a este CSV bruto")
    ap.add_argument("--rodada", type=int, default=1)
    args = ap.parse_args()

    client = Client(scheduler_file=args.scheduler_file)
    client.wait_for_workers(args.workers, timeout=180)
    t_sobe = None
    if os.environ.get("INICIO_JOB"):
        t_sobe = time.time() - float(os.environ["INICIO_JOB"])
    print(f"workers: {args.workers} | nos: {args.nos} | particoes: {args.particoes} | dados: {args.dados}")
    if t_sobe is not None:
        print(f"  subida do cluster (fora do t_total): {t_sobe:.1f} s")

    stop = carrega_stopwords(args.stopwords)
    cron = Cronometro()
    t_ini = time.perf_counter()

    # 1) leitura: os workers leem do NFS. persist + wait para isolar o custo.
    def leitura():
        ddf = dd.read_parquet(args.dados, columns=[args.coluna]).repartition(npartitions=args.particoes)
        bag = ddf[args.coluna].to_bag().persist()
        wait(bag)
        return bag
    textos = cron.etapa("leitura", leitura)

    # 2) tokenizacao (estreita)
    def tokenizacao():
        b = textos.map(tokeniza).persist()
        wait(b)
        return b
    tokens = cron.etapa("tokeniza", tokenizacao)
    del textos          # libera memoria: as etapas seguintes nao precisam mais do texto bruto

    # 3) remocao de stopwords (estreita)
    def stopwords():
        b = tokens.map(sem_stopwords, stop).persist()
        wait(b)
        return b
    limpos = cron.etapa("stopwords", stopwords)
    del tokens          # idem: so `limpos` (usado nas estatisticas) e mantido

    # 4) TF por documento (estreita)
    def tf():
        b = limpos.map(tf_de).persist()
        wait(b)
        return b
    tfs = cron.etapa("tf", tf)

    # 5) DF do corpus e IDF (larga). frequencies combina antes de mover.
    def df():
        ndocs = tfs.count().compute()
        dfreq = dict(tfs.map(lambda d: list(d)).flatten().frequencies().compute())
        return ndocs, dfreq
    ndocs, dfreq = cron.etapa("df", df)
    idf = {t: math.log(ndocs / (1 + c)) + 1.0 for t, c in dfreq.items()}

    # 6) TF-IDF (estreita; o IDF vai uma vez para cada worker)
    def tfidf():
        # O IDF (dicionario grande) vai UMA vez para cada worker via client.run. Alternativas medidas:
        #   Future em bag.map ............ 175 s (o Dask resolve o Future a cada documento)
        #   dict/delayed como argumento .. 7 s (111 mil termos) e travou (>10 min) com 300 mil documentos
        #   client.run + get_worker() .... 0,17 s. O custo real de mover o dicionario aparece aqui.
        client.run(guarda_idf, idf)
        b = tfs.map_partitions(tfidf_particao).persist()
        wait(b)
        return b
    tfidfs = cron.etapa("tfidf", tfidf)

    # 7) estatisticas: so numeros pequenos voltam ao cliente
    def stats():
        soma = tfidfs.reduction(soma_particao, junta_somas).compute()
        tam = dict(limpos.map(len).map(lambda n: min(n // 10, 30)).frequencies().compute())
        return soma, tam
    soma, hist = cron.etapa("stats", stats)

    t_total = time.perf_counter() - t_ini
    t_calc = cron.t["tokeniza"] + cron.t["stopwords"] + cron.t["tf"] + cron.t["tfidf"]
    t_serial = t_total - t_calc

    top = sorted(soma.items(), key=lambda kv: -kv[1])[:args.top]
    print(f"\ndocumentos: {ndocs:,} | vocabulario: {len(dfreq):,} termos")
    print("maiores TF-IDF (soma): " + ", ".join(f"{t}={v:.1f}" for t, v in top))
    print("distribuicao de tamanho (tokens uteis/10 -> docs): "
          + ", ".join(f"{k*10}:{n}" for k, n in sorted(hist.items())[:8]) + " ...")
    print(f"\nt_total {t_total:.3f} s | t_calc (estreitas) {t_calc:.3f} s | t_serial {t_serial:.3f} s")

    linha = [args.workers, args.nos, f"{t_total:.4f}", f"{t_serial:.4f}", f"{t_calc:.4f}", args.rodada,
             f"{cron.t['leitura']:.4f}", f"{cron.t['tokeniza']:.4f}", f"{cron.t['stopwords']:.4f}", f"{cron.t['tf']:.4f}",
             f"{cron.t['df']:.4f}", f"{cron.t['tfidf']:.4f}", f"{cron.t['stats']:.4f}",
             f"{t_sobe:.2f}" if t_sobe is not None else ""]
    print("CSV," + ",".join(map(str, linha)))
    if args.csv:
        novo = not os.path.exists(args.csv)
        with open(args.csv, "a") as f:
            if novo:
                f.write("nprocs,nnodes,t_total,t_serial,t_calc,rodada,t_leitura,t_tokeniza,t_stopwords,t_tf,t_df,t_tfidf,t_stats,t_sobe\n")
            f.write(",".join(map(str, linha)) + "\n")


if __name__ == "__main__":
    main()
