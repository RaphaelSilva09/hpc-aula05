#!/usr/bin/env python3
"""analisa_corpus.py - le resultados/corpus_bruto.csv (18 linhas) e gera:

  resultados/speedup_corpus.csv   6 linhas, MEDIANA por numero de workers (mesmo formato da Aula 3)
  tabela: speedup, eficiencia, Amdahl (f com 1-16 e com 1-32), Karp-Flatt e a mediana de cada etapa

Diferenca deliberada em relacao ao analisa_speedup.py da Aula 3: la se usou o MENOR tempo,
aqui a MEDIANA (a Parte 2 pede isso; a primeira leitura vem do disco, as seguintes do cache).
Diga isso na analise: as duas curvas usam estatisticas diferentes.

Uso: python3 analisa_corpus.py [corpus_bruto.csv] [speedup_corpus.csv]
"""
import csv
import statistics
import sys

BRUTO = sys.argv[1] if len(sys.argv) > 1 else "resultados/corpus_bruto.csv"
SAIDA = sys.argv[2] if len(sys.argv) > 2 else "resultados/speedup_corpus.csv"
ETAPAS = ["t_leitura", "t_tokeniza", "t_stopwords", "t_tf", "t_df", "t_tfidf", "t_stats", "t_sobe"]
CAMPOS = ["t_total", "t_serial", "t_calc"] + ETAPAS


def ajuste_amdahl(ps, t):
    """1/S = f + (1-f)/p e uma reta em x = 1/p (minimos quadrados)."""
    xs = [1.0 / p for p in ps]
    ys = [t[p] / t[1] for p in ps]
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / max(sum((x - mx) ** 2 for x in xs), 1e-12)
    return min(max(my - b * mx, 0.0), 1.0)


def main():
    dados = {}
    with open(BRUTO) as f:
        for lin in csv.DictReader(f):
            try:
                p = int(lin["nprocs"])
            except (KeyError, ValueError):
                continue
            d = dados.setdefault(p, {"nnodes": lin["nnodes"], "n": 0, **{c: [] for c in CAMPOS}})
            d["n"] += 1
            for c in CAMPOS:
                try:
                    d[c].append(float(lin[c]))
                except (KeyError, ValueError):
                    pass
    if 1 not in dados:
        sys.exit("preciso das medicoes com 1 worker para calcular o speedup")

    med = {p: {c: statistics.median(d[c]) for c in CAMPOS if d[c]} | {"nnodes": d["nnodes"], "n": d["n"]}
           for p, d in dados.items()}
    ps = sorted(med)
    t1 = med[1]["t_total"]

    with open(SAIDA, "w") as f:
        f.write("nprocs,nnodes,t_total,t_serial,t_calc\n")
        for p in ps:
            m = med[p]
            f.write(f"{p},{m['nnodes']},{m['t_total']:.4f},{m['t_serial']:.4f},{m['t_calc']:.4f}\n")
    print(f"salvo: {SAIDA} ({len(ps)} linhas, mediana de {min(m['n'] for m in med.values())}"
          f" a {max(m['n'] for m in med.values())} rodadas por ponto)\n")

    print("%6s %5s %9s %9s %9s %8s %7s %8s" % ("workers", "nos", "t_total", "t_serial", "t_calc", "speedup", "efic.", "KarpFl"))
    for p in ps:
        m = med[p]
        S = t1 / m["t_total"]
        kf = ((1 / S - 1 / p) / (1 - 1 / p)) if p > 1 else float("nan")
        print("%6d %7s %9.2f %9.2f %9.2f %8.2f %6.0f%% %8.3f" % (p, m["nnodes"], m["t_total"], m["t_serial"], m["t_calc"], S, 100 * S / p, kf))

    print("\nmediana por etapa (s):")
    print("%6s " % "workers" + " ".join("%10s" % e[2:] for e in ETAPAS))
    for p in ps:
        print("%6d " % p + " ".join("%10.2f" % med[p].get(e, float("nan")) for e in ETAPAS))

    t = {p: med[p]["t_total"] for p in ps}
    for nome, teto in (("1 a 16", 16), ("1 a 32", 32)):
        sub = [p for p in ps if p <= teto]
        if len(sub) >= 3:
            f = ajuste_amdahl(sub, t)
            print(f"\nAmdahl, pontos {nome}: f = {f:.3f} -> teto 1/f = {1/f:.1f}" if f > 0 else f"\nAmdahl, pontos {nome}: f ~ 0")
            print("  previsto: " + "  ".join(f"S({p})={1/(f+(1-f)/p):.2f}" for p in ps))
    print("\nKarp-Flatt e(p): constante => fracao serial fixa (Amdahl); crescente => overhead que cresce com p; "
          "decrescente => custo fixo de sair do caso de 1 processo, diluido conforme p cresce; e > 1 => mais lento que 1 worker.")


if __name__ == "__main__":
    main()
