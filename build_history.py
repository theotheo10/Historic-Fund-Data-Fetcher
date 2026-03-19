#!/usr/bin/env python3
"""
build_history.py — Backfill completo do histórico de cotas CVM.

Uso:
    python build_history.py

Output:
    history_full.json  — histórico completo desde 2019-01-02 até hoje

Envie o arquivo gerado ao Claude para integrar no history.json do site.

Fonte: https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/
  - Pré-2021: arquivos anuais  /HIST/inf_diario_fi_YYYY.zip
  - 2021+:    arquivos mensais /inf_diario_fi_YYYYMM.zip
"""

import json, zipfile, io, math, datetime, urllib.request, sys

FUNDS = [
    {"name": "Tarpon GT FIF Cotas FIA",                                            "cnpj": "22232927000190", "cnpjFmt": "22.232.927/0001-90"},
    {"name": "Organon FIF Cotas FIA",                                              "cnpj": "17400251000166", "cnpjFmt": "17.400.251/0001-66"},
    {"name": "Artica Long Term FIA",                                               "cnpj": "18302338000163", "cnpjFmt": "18.302.338/0001-63"},
    {"name": "Genoa Capital Arpa CIC Classe FIM RL",                               "cnpj": "37495383000126", "cnpjFmt": "37.495.383/0001-26"},
    {"name": "Itaú Artax Ultra Multimercado FIF DA CIC RL",                        "cnpj": "42698666000105", "cnpjFmt": "42.698.666/0001-05"},
    {"name": "Guepardo Long Bias RV FIM",                                          "cnpj": "24623392000103", "cnpjFmt": "24.623.392/0001-03"},
    {"name": "Kapitalo Tarkus FIF Cotas FIA",                                      "cnpj": "28747685000153", "cnpjFmt": "28.747.685/0001-53"},
    {"name": "Real Investor FIC FIF Ações RL",                                     "cnpj": "10500884000105", "cnpjFmt": "10.500.884/0001-05"},
    {"name": "Gama Schroder Gaia Contour Tech Equity L&S BRL FIF CIC Mult IE RL", "cnpj": "35744790000102", "cnpjFmt": "35.744.790/0001-02"},
    {"name": "Patria Long Biased FIF Cotas FIM",                                   "cnpj": "38954217000103", "cnpjFmt": "38.954.217/0001-03"},
    {"name": "Absolute Pace Long Biased FIC FIF Ações RL",                         "cnpj": "32073525000143", "cnpjFmt": "32.073.525/0001-43"},
    {"name": "Arbor FIC FIA",                                                      "cnpj": "21689246000192", "cnpjFmt": "21.689.246/0001-92"},
    {"name": "Charles River FIF Ações",                                            "cnpj": "14438229000117", "cnpjFmt": "14.438.229/0001-17"},
    {"name": "SPX Falcon FIF CIC Ações RL",                                        "cnpj": "17397315000117", "cnpjFmt": "17.397.315/0001-17"},
]

START_DATE      = datetime.date(2019, 1, 2)
FIRST_MONTHLY   = 2021   # CVM: mensais a partir daqui
OUTPUT_FILE     = "history_full.json"


# ── Fetch ──────────────────────────────────────────────────────────────────────

def fetch_zip(url: str, timeout: int = 120) -> str | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            return zf.read(zf.namelist()[0]).decode("windows-1252", errors="replace")
    except Exception as e:
        print(f"    ERRO: {e}", file=sys.stderr)
        return None


def parse_csv(content: str) -> dict:
    lines  = content.split("\n")
    header = [h.strip().lstrip("\ufeff") for h in lines[0].split(";")]
    return {
        "lines":     lines,
        "col_cnpj":  next((i for i, h in enumerate(header) if h.startswith("CNPJ")), -1),
        "col_date":  header.index("DT_COMPTC") if "DT_COMPTC"  in header else -1,
        "col_quota": header.index("VL_QUOTA")  if "VL_QUOTA"   in header else -1,
    }


def extract_fund(data: dict, fund: dict) -> dict[str, float]:
    """Retorna {date_str: quota} para o fundo no bloco CSV."""
    if not data or data["col_date"] < 0 or data["col_quota"] < 0:
        return {}
    cnpj, fmt = fund["cnpj"], fund["cnpjFmt"]
    result: dict[str, float] = {}
    for line in data["lines"][1:]:
        if cnpj not in line and fmt not in line:
            continue
        cols = line.split(";")
        try:
            if data["col_cnpj"] >= 0:
                raw = cols[data["col_cnpj"]].strip().replace(".", "").replace("/", "").replace("-", "")
                if raw != cnpj:
                    continue
            d = cols[data["col_date"]].strip()
            q = float(cols[data["col_quota"]].replace(",", "."))
            if d >= START_DATE.isoformat() and q > 0:
                result[d] = q
        except (ValueError, IndexError):
            continue
    return result


# ── Coleta por período ─────────────────────────────────────────────────────────

def collect_all() -> dict[str, dict[str, float]]:
    """
    Retorna {cnpjFmt: {date: quota}} para todos os fundos, desde START_DATE até hoje.
    Cada fundo começa na sua própria data de início — sem interseção forçada.
    """
    today = datetime.date.today()
    quotas: dict[str, dict[str, float]] = {f["cnpjFmt"]: {} for f in FUNDS}

    # ── Anos pré-2021: um ZIP por ano ──────────────────────────────────────
    annual_start = START_DATE.year
    annual_end   = min(FIRST_MONTHLY - 1, today.year)

    for year in range(annual_start, annual_end + 1):
        url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_{year}.zip"
        print(f"  [{year}] {url}")
        content = fetch_zip(url)
        if not content:
            print(f"    → não disponível, pulando")
            continue
        data = parse_csv(content)
        for fund in FUNDS:
            rows = extract_fund(data, fund)
            quotas[fund["cnpjFmt"]].update(rows)
            if rows:
                print(f"    {fund['cnpjFmt'][:14]}… +{len(rows)} cotas")

    # ── 2021+: um ZIP por mês ──────────────────────────────────────────────
    y, m = max(FIRST_MONTHLY, START_DATE.year), 1
    if START_DATE.year >= FIRST_MONTHLY:
        m = START_DATE.month

    while (y, m) <= (today.year, today.month):
        url = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{y}{m:02d}.zip"
        print(f"  [{y}-{m:02d}] buscando…")
        content = fetch_zip(url)
        if content:
            data = parse_csv(content)
            for fund in FUNDS:
                rows = extract_fund(data, fund)
                quotas[fund["cnpjFmt"]].update(rows)
            total = sum(len(extract_fund(data, f)) for f in FUNDS)
            print(f"    → {total} cotas nos {len(FUNDS)} fundos")
        else:
            print(f"    → não disponível")
        m += 1
        if m > 12:
            m = 1; y += 1

    return quotas


# ── Construção do JSON ─────────────────────────────────────────────────────────

def build_json(quotas: dict[str, dict[str, float]]) -> dict:
    """
    Constrói o history_full.json.

    Cada fundo tem sua própria série de datas — começa quando começa,
    sem interseção global forçada. O index.html já lida com fundos de
    comprimentos diferentes (usa os dados disponíveis de cada um).

    Para métricas que exigem série conjunta (correlação, beta, otimizador):
    o index.html usa a interseção das datas disponíveis no momento do cálculo,
    que naturalmente cresce conforme o histórico comum aumenta.
    """
    today = datetime.date.today()

    # Todas as datas presentes em pelo menos 1 fundo (union)
    all_dates = sorted({d for qs in quotas.values() for d in qs})

    print(f"\n  Total de datas únicas: {len(all_dates)} "
          f"({all_dates[0] if all_dates else '?'} → {all_dates[-1] if all_dates else '?'})")

    # Interpolação geométrica para datas em que o fundo existia mas não tem cota
    # (fins de semana, feriados, gaps de divulgação)
    # Só interpola ENTRE datas conhecidas — nunca extrapola antes do início do fundo.
    interp_total = 0
    for fund in FUNDS:
        cnpj = fund["cnpjFmt"]
        qs   = quotas[cnpj]
        if not qs:
            continue
        sorted_known = sorted(qs.keys())
        fund_start   = sorted_known[0]
        fund_end     = sorted_known[-1]

        for d in all_dates:
            if d < fund_start or d > fund_end:
                continue   # fora do período de vida do fundo — não interpola
            if d in qs:
                continue   # já tem dado real

            prev_d = next((x for x in reversed(sorted_known) if x < d), None)
            next_d = next((x for x in sorted_known           if x > d), None)

            if prev_d and next_d:
                t0    = datetime.date.fromisoformat(prev_d)
                t1    = datetime.date.fromisoformat(next_d)
                td    = datetime.date.fromisoformat(d)
                alpha = (td - t0).days / max((t1 - t0).days, 1)
                qs[d] = round(qs[prev_d] * ((qs[next_d] / qs[prev_d]) ** alpha), 8)
                interp_total += 1
            elif prev_d:
                qs[d] = qs[prev_d]
                interp_total += 1

        quotas[cnpj] = qs
        sorted_known = sorted(qs.keys())  # reordena depois de interpolado

    print(f"  Cotas interpoladas: {interp_total}")

    # Datas comuns a todos os fundos que existiam naquela data
    # Para o index.html: usa commonDates (interseção) para métricas conjuntas
    # Cada fundo também carrega sua série individual completa
    date_fund_count = {d: sum(1 for f in FUNDS if d in quotas[f["cnpjFmt"]]) for d in all_dates}

    # commonDates = datas onde pelo menos o fundo mais antigo com dados existe
    # Sem limiar artificial — se a data existe para algum fundo, ela existe.
    # O index.html usa interseção dinâmica por par de fundos onde necessário.
    # Para rolling alpha, correlação etc.: usa a interseção dos dois fundos comparados.
    # Para a tabela principal: cada fundo usa sua própria série.
    common_dates = sorted(d for d, cnt in date_fund_count.items() if cnt >= 1)

    # Serializar fundos
    funds_out = {}
    for fund in FUNDS:
        cnpj = fund["cnpjFmt"]
        qs   = quotas[cnpj]
        if not qs:
            print(f"  AVISO: {fund['name']} sem dados — omitido")
            continue

        # Datas deste fundo especificamente
        fund_dates  = sorted(qs.keys())
        fund_quotas = [qs[d] for d in fund_dates]

        # Retornos diários
        returns = []
        for i in range(1, len(fund_dates)):
            q0, q1 = qs.get(fund_dates[i-1]), qs.get(fund_dates[i])
            returns.append((q1 / q0) - 1 if q0 and q1 else 0.0)

        # Max drawdown
        cum = pk = 1.0; mdd = 0.0
        for r in returns:
            cum *= (1 + r)
            if cum > pk: pk = cum
            dd = (cum - pk) / pk
            if dd < mdd: mdd = dd

        funds_out[cnpj] = {
            "nome":        fund["name"],
            "dates":       fund_dates,
            "quotas":      fund_quotas,
            "returns":     returns,
            "maxDrawdown": round(mdd * 100, 2),
            "nDays":       len(fund_dates),
            "start":       fund_dates[0],
            "end":         fund_dates[-1],
        }
        print(f"  {cnpj}: {len(fund_dates)} pregões ({fund_dates[0]} → {fund_dates[-1]})")

    # Correlação de Pearson (sobre datas comuns a cada par)
    def pearson_pair(ca: str, cb: str) -> float:
        dates_a = set(funds_out[ca]["dates"]) if ca in funds_out else set()
        dates_b = set(funds_out[cb]["dates"]) if cb in funds_out else set()
        common  = sorted(dates_a & dates_b)
        if len(common) < 30:
            return 0.0
        qs_a = quotas[ca]; qs_b = quotas[cb]
        rets_a = [(qs_a[common[i]] / qs_a[common[i-1]]) - 1 for i in range(1, len(common))]
        rets_b = [(qs_b[common[i]] / qs_b[common[i-1]]) - 1 for i in range(1, len(common))]
        n  = len(rets_a)
        ma = sum(rets_a) / n; mb = sum(rets_b) / n
        num = sum((rets_a[i] - ma) * (rets_b[i] - mb) for i in range(n))
        sa  = math.sqrt(sum((x - ma) ** 2 for x in rets_a))
        sb  = math.sqrt(sum((x - mb) ** 2 for x in rets_b))
        return round(num / (sa * sb), 4) if sa * sb > 0 else 0.0

    cnpjs = list(funds_out.keys())
    corr  = {ca: {cb: pearson_pair(ca, cb) for cb in cnpjs} for ca in cnpjs}

    n_years = (datetime.date.fromisoformat(all_dates[-1]) -
               datetime.date.fromisoformat(all_dates[0])).days / 365.25 if all_dates else 0

    return {
        "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "from":        all_dates[0]  if all_dates else "",
        "to":          all_dates[-1] if all_dates else "",
        "nDays":       len(common_dates),
        "nYears":      round(n_years, 2),
        "commonDates": common_dates,   # union de todas as datas (cada fundo tem a sua)
        "correlation": corr,
        "funds":       funds_out,
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print(f"=== Build histórico completo desde {START_DATE} ===")
    print(f"Fundos: {len(FUNDS)}")
    print(f"Output: {OUTPUT_FILE}\n")

    print("1. Coletando cotas da CVM…")
    quotas = collect_all()

    fund_counts = {f["cnpjFmt"]: len(quotas[f["cnpjFmt"]]) for f in FUNDS}
    print("\nCotas coletadas por fundo:")
    for cnpj, n in fund_counts.items():
        print(f"  {cnpj}: {n}")

    print("\n2. Construindo JSON…")
    output = build_json(quotas)

    print(f"\n3. Salvando {OUTPUT_FILE}…")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

    size_mb = __import__("os").path.getsize(OUTPUT_FILE) / 1024 / 1024
    print(f"\n✓ Concluído!")
    print(f"  Arquivo: {OUTPUT_FILE} ({size_mb:.1f} MB)")
    print(f"  Período: {output['from']} → {output['to']}")
    print(f"  Pregões (union): {output['nDays']}")
    print(f"  Anos: {output['nYears']:.1f}")
    print(f"\nEnvie {OUTPUT_FILE} ao Claude para integrar no history.json do site.")


if __name__ == "__main__":
    main()
