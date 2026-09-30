#!/usr/bin/env python3
"""prepara_dataset.py - converte o corpus escolhido para Parquet (uma vez, FORA da medicao).

Roda no MASTER (que tem internet e o pandas/pyarrow do ambiente hpc). Os nos so leem o resultado.
Um CSV com quebra de linha dentro de um campo quebraria a leitura por blocos; o Parquet nao tem esse problema.

Exemplos:
  # CSV real (ex.: AG News: colunas classe,titulo,descricao). Junta as colunas de texto:
  python3 prepara_dataset.py --csv train.csv --sem-cabecalho --colunas 1 2 \
      --saida /opt/ohpc/pub/grupo/dados/corpus --arquivos 32

  # SO PARA TESTAR O PIPELINE (nao serve para a ponderada): corpus sintetico
  python3 prepara_dataset.py --sintetico 20000 --saida /home/test/aula05/dados_teste

Depois: chmod -R a+rX <saida>; confira nos 4 nos: srun -N 4 ls -la <saida>
Registre no README: fonte (URL e licenca), sha256 do arquivo original, numero de documentos, tamanho.
"""
import argparse
import hashlib
import os

import numpy as np
import pandas as pd


def sha256(caminho):
    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        for bloco in iter(lambda: f.read(1 << 20), b""):
            h.update(bloco)
    return h.hexdigest()


def sintetico(n, semente=42, palavras=90):
    """Texto com distribuicao de Zipf. So para testar o codigo."""
    rng = np.random.default_rng(semente)
    def palavra(i):   # so letras: o tokenizador descarta digitos
        s = ""
        for _ in range(5):
            s, i = s + chr(97 + i % 26), i // 26
        return "pal" + s
    vocab = np.array([palavra(i) for i in range(20000)])
    idx = (rng.zipf(1.25, size=(n, palavras)) - 1) % len(vocab)
    return pd.DataFrame({"texto": [" ".join(vocab[l]) for l in idx]})


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--csv")
    g.add_argument("--jsonl")
    g.add_argument("--parquet")
    g.add_argument("--sintetico", type=int, metavar="N")
    ap.add_argument("--sem-cabecalho", action="store_true")
    ap.add_argument("--max-docs", type=int, default=None,
                    help="usa so os N primeiros documentos (subconjunto deterministico)")
    ap.add_argument("--colunas", nargs="+", default=None,
                    help="colunas de texto (nome ou indice), unidas por espaco")
    ap.add_argument("--saida", required=True)
    ap.add_argument("--arquivos", type=int, default=32,
                    help="quantos arquivos parquet (varios permitem leitura paralela)")
    args = ap.parse_args()

    if args.sintetico:
        df = sintetico(args.sintetico)
    else:
        origem = args.csv or args.jsonl or args.parquet
        print(f"sha256 de {origem}: {sha256(origem)}")
        if args.csv:
            df = pd.read_csv(args.csv, header=None if args.sem_cabecalho else "infer")
        elif args.parquet:
            df = pd.read_parquet(args.parquet)
        else:
            df = pd.read_json(args.jsonl, lines=True)
        cols = args.colunas or ["text" if "text" in df.columns else df.columns[-1]]
        cols = [int(c) if args.sem_cabecalho or str(c).isdigit() and c not in df.columns else c for c in cols]
        df = pd.DataFrame({"texto": df[cols].fillna("").astype(str).agg(" ".join, axis=1)})

    df = df[df["texto"].str.strip() != ""].reset_index(drop=True)
    if args.max_docs:
        df = df.iloc[:args.max_docs].reset_index(drop=True)
    os.makedirs(args.saida, exist_ok=True)
    limites = np.linspace(0, len(df), args.arquivos + 1, dtype=int)
    for i in range(args.arquivos):
        df.iloc[limites[i]:limites[i + 1]].to_parquet(
            os.path.join(args.saida, f"parte-{i:03d}.parquet"), index=False)

    tam = sum(os.path.getsize(os.path.join(args.saida, f)) for f in os.listdir(args.saida))
    print(f"{len(df):,} documentos em {args.arquivos} arquivos | {tam/1e6:.1f} MB em disco | {args.saida}")


if __name__ == "__main__":
    main()
