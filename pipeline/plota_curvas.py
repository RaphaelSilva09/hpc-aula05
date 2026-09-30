#!/usr/bin/env python3
"""plota_curvas.py - grafico de 3 curvas, eixo x em log2 de 1 a 32:
  speedup do corpus (mediana) | speedup do pi_mpi (Parte 1) | linear ideal S = p.
Tambem imprime a tabela de speedup e eficiencia das duas curvas.

Uso: python3 plota_curvas.py resultados/speedup_corpus.csv resultados/speedup.csv
     (o segundo e o speedup.csv do pi_mpi da Aula 3; se houver varias linhas por p, usa o menor)
Sem matplotlib (o ambiente hpc nao tem), imprime so a tabela: plote no Colab ou instale no master.
"""
import csv
import sys


def le(arq, agrega):
    por_p = {}
    with open(arq) as f:
        for lin in csv.DictReader(f):
            por_p.setdefault(int(lin["nprocs"]), []).append(float(lin["t_total"]))
    t = {p: agrega(v) for p, v in por_p.items()}
    return {p: t[1] / t[p] for p in sorted(t)}


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    corpus = le(sys.argv[1], lambda v: sorted(v)[len(v) // 2])   # ja e a mediana; 1 linha por p
    pi = le(sys.argv[2], min)                                    # a Aula 3 usou o menor tempo

    ps = sorted(set(corpus) | set(pi))
    print("%7s %14s %9s %14s %9s" % ("p", "S corpus", "efic.", "S pi_mpi", "efic."))
    for p in ps:
        sc, sp = corpus.get(p), pi.get(p)
        print("%7d %14s %9s %14s %9s" % (p,
              "%.2f" % sc if sc else "-", "%.0f%%" % (100 * sc / p) if sc else "-",
              "%.2f" % sp if sp else "-", "%.0f%%" % (100 * sp / p) if sp else "-"))
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("\n(matplotlib ausente: cole a tabela no Colab para plotar)")
        return
    plt.figure(figsize=(7, 4.5))
    plt.plot(ps, ps, "--", color="#999999", label="linear ideal (S = p)")
    plt.plot(list(pi), list(pi.values()), "s-", color="#2E2540", label="pi_mpi (Aula 3, menor tempo)")
    plt.plot(list(corpus), list(corpus.values()), "o-", color="#FF4546", label="corpus TF-IDF (mediana)")
    plt.xscale("log", base=2); plt.xticks(ps, [str(p) for p in ps])
    plt.xlabel("workers / processos"); plt.ylabel("speedup"); plt.grid(alpha=0.3); plt.legend()
    plt.title("Speedup: corpus TF-IDF em Dask vs pi_mpi")
    plt.tight_layout(); plt.savefig("resultados/curvas.png", dpi=150)
    print("\ngrafico salvo em resultados/curvas.png")


if __name__ == "__main__":
    main()
