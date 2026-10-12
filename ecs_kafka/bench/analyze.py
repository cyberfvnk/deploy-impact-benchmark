"""
Análise de uma ou mais rodadas (pastas em runs/).

Fontes:
  sent.csv            -> o que entrou (id, createdAtMs)
  final.csv           -> o que saiu na fila final (pode ter duplicadas)
  orchestrator.jsonl  -> horários do deploy (SIGTERM, parada, exit code)
  logs/*.log          -> eventos EVT das tasks (rebalance, abortadas...)

Uso:  python analyze.py runs/S0-20261011-120000 [runs/S1-... ...]
      python analyze.py runs/S0-*            (várias rodadas: imprime uma linha por rodada)
"""

import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path

SLA_MS = 26_000
EVT_RE = re.compile(r"EVT - (.*)$")


def percentile(values: list[int], p: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round(p / 100 * len(ordered)) - 1))
    return ordered[idx]


def read_csv(path: Path) -> list[dict]:
    with path.open() as f:
        return list(csv.DictReader(f))


def read_events(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def read_evt(logs_dir: Path) -> list[dict]:
    """Converte as linhas 'EVT - type=X task=Y ...' em dicionários."""
    out = []
    for log in sorted(logs_dir.glob("*.log")):
        for line in log.read_text().splitlines():
            m = EVT_RE.search(line)
            if not m:
                continue
            fields = {}
            for token in m.group(1).split(" "):
                if "=" in token:
                    k, v = token.split("=", 1)
                    fields[k] = v
            if "ts" in fields:
                fields["ts"] = int(fields["ts"])
                out.append(fields)
    return out


def analyze(run_dir: Path) -> dict:
    sent = read_csv(run_dir / "sent.csv")
    final = read_csv(run_dir / "final.csv")
    events = read_events(run_dir / "orchestrator.jsonl")
    evt = read_evt(run_dir / "logs")

    first = {e["type"]: e for e in reversed(events)}  # primeira ocorrência de cada tipo
    deploy_start = first["DEPLOY_START"]["ts"]
    deploy_done = first["DEPLOY_DONE"]["ts"]
    deploy_stops = [e for e in events if e["type"] == "TASK_STOPPED" and e.get("phase") == "deploy"]

    # --- Entregas: primeira conclusão de cada id vale; o resto é duplicada ---
    completions: dict[str, dict] = {}
    for row in final:
        if row["id"] not in completions:
            completions[row["id"]] = row
    created = {r["id"]: int(r["createdAtMs"]) for r in sent}
    e2e = [int(r["completedAtMs"]) - int(r["createdAtMs"]) for r in completions.values()]
    on_time = {i for i, r in completions.items() if int(r["completedAtMs"]) - int(r["createdAtMs"]) <= SLA_MS}
    late = {i for i in completions if i not in on_time}
    expired = {e["id"] for e in evt if e["type"] == "EXPIRED"} - on_time
    # Falha de SLA = tudo que não concluiu em 26s (atrasada, expirada -> desfazimento, ou perdida).
    sla_fail = [i for i in created if i not in on_time]
    lost = [i for i in created if i not in completions and i not in expired]
    # Falhas de msgs que entraram bem antes do deploy = falta de capacidade, não efeito do deploy.
    pre_deploy_fail = sum(1 for i in sla_fail if created[i] < deploy_start - SLA_MS)

    # Saturação: latência subindo durante o aquecimento (fila crescendo) invalida a rodada.
    load_start = first["LOAD_START"]["ts"]

    def p50_entre(a: int, b: int) -> int:
        return percentile([int(r["completedAtMs"]) - int(r["createdAtMs"]) for r in completions.values()
                           if a <= int(r["createdAtMs"]) < b], 50)

    warm_p50_inicio = p50_entre(load_start + 5_000, load_start + 20_000)
    warm_p50_fim = p50_entre(deploy_start - 15_000, deploy_start)
    saturated = warm_p50_fim > 3_000 and warm_p50_fim > 1.5 * warm_p50_inicio

    # Duração do shutdown de cada antiga (SIGTERM -> parada). Perto de 30s = estourou o stopTimeout.
    sigterm_at = {e["task"]: e["ts"] for e in events if e["type"] == "TASK_SIGTERM" and e.get("phase") == "deploy"}
    shutdown_s = [round((e["ts"] - sigterm_at[e["task"]]) / 1000, 1) for e in deploy_stops if e["task"] in sigterm_at]

    # --- Kafka: do primeiro SIGKILL/saída de uma antiga até uma nova receber partição ---
    first_old_exit = min((e["ts"] for e in deploy_stops), default=None)
    orphaned = orphaned_partitions(evt, first_old_exit) if first_old_exit else set()
    new_assign = [e for e in evt if e["type"] == "REBALANCE_ASSIGNED" and e["task"].startswith("v2")
                  and e.get("count", "0") != "0" and "bench-reply" in e.get("group", "")]
    first_new_assign = min((e["ts"] for e in new_assign if first_old_exit and e["ts"] >= first_old_exit),
                           default=None)

    evt_counts = Counter(e["type"] for e in evt)
    exit_codes = Counter(e["exitCode"] for e in deploy_stops)

    return {
        "run": run_dir.name,
        "sent": len(sent),
        "completed": len(completions),
        "on_time": len(on_time),
        "sla_fail": len(sla_fail),
        "pre_deploy_fail": pre_deploy_fail,
        "warm_p50": (warm_p50_inicio, warm_p50_fim),
        "saturated": saturated,
        "shutdown_max_s": max(shutdown_s, default=0),
        "late": len(late),
        "expired": len(expired),
        "lost": len(lost),
        "duplicates": len(final) - len(completions),
        "redelivered": sum(1 for r in completions.values() if int(r["receiveCount"] or 1) > 1),
        "p50": percentile(e2e, 50),
        "p99": percentile(e2e, 99),
        "max": max(e2e, default=0),
        "orphaned": len(orphaned),
        "partitions": len({p for e in evt if e["type"] == "REBALANCE_ASSIGNED" and "bench-reply" in e.get("group", "")
                           for p in parse_partitions(e)}),
        "old_exit_to_new_assign_s": round((first_new_assign - first_old_exit) / 1000, 1)
        if first_new_assign and first_old_exit else None,
        "deploy_s": round((deploy_done - deploy_start) / 1000, 1),
        "exit_codes": dict(exit_codes),
        "aborted": evt_counts.get("ABORTED", 0),
        "rebalance_assigned": evt_counts.get("REBALANCE_ASSIGNED", 0),
        "rebalance_revoked": evt_counts.get("REBALANCE_REVOKED", 0),
        "drain_timed_out": first["COLLECT_DONE"].get("drainTimedOut", False),
        "timeline": fail_timeline([created[i] for i in sla_fail], deploy_start),
    }


def parse_partitions(e: dict) -> list[str]:
    raw = e.get("partitions", "[]").strip("[]")
    return [p for p in raw.split(",") if p]


def orphaned_partitions(evt: list[dict], kill_ts: int) -> set[str]:
    """
    Partições de resposta que eram de tasks antigas (v1) no instante em que elas saíram.
    Reconstrói a posse por task aplicando assigned/revoked/lost em ordem (vale para eager e cooperative).
    Essas partições ficam sem consumo até o grupo reagir (session timeout, se não houve LeaveGroup).
    """
    owned: dict[str, set[str]] = {}
    for e in sorted(evt, key=lambda x: x["ts"]):
        if e["ts"] > kill_ts or "bench-reply" not in e.get("group", ""):
            continue
        parts = set(parse_partitions(e))
        if e["type"] == "REBALANCE_ASSIGNED":
            owned.setdefault(e["task"], set()).update(parts)
        elif e["type"] in ("REBALANCE_REVOKED", "REBALANCE_LOST"):
            owned.setdefault(e["task"], set()).difference_update(parts)
    return set().union(*(p for t, p in owned.items() if t.startswith("v1")), set())


def fail_timeline(created_ms: list[int], deploy_start: int, bucket_s: int = 10) -> dict:
    """Falhas de SLA por janela de 10s, pela hora em que a msg ENTROU (relativo ao início do deploy)."""
    buckets = Counter()
    for ts in created_ms:
        offset = (ts - deploy_start) // 1000
        buckets[int(offset // bucket_s * bucket_s)] += 1
    return dict(sorted(buckets.items()))


def print_report(r: dict):
    print(f"\n### {r['run']}")
    print(f"enviadas={r['sent']} no prazo={r['on_time']} | FALHAS DE SLA={r['sla_fail']} "
          f"(atrasadas={r['late']} expiradas/desfazimento={r['expired']} perdidas={r['lost']})")
    if r["saturated"]:
        print(f"ATENÇÃO: SATURADO ANTES DO DEPLOY (p50 no aquecimento {r['warm_p50'][0]}ms -> "
              f"{r['warm_p50'][1]}ms). Rodada inválida para medir o deploy.")
    print(f"falhas antes do deploy (capacidade)={r['pre_deploy_fail']} | "
          f"shutdown mais longo de uma antiga={r['shutdown_max_s']}s (limite 30s)")
    print(f"duplicadas={r['duplicates']} reentregues pelo SQS={r['redelivered']} abortadas={r['aborted']}")
    print(f"latência das concluídas: p50={r['p50']}ms p99={r['p99']}ms máx={r['max']}ms")
    if r["drain_timed_out"]:
        print("ATENÇÃO: a fila final não acalmou em 5 min (possível ciclo de reprocessamento)")
    print(f"partições de resposta órfãs na saída das antigas: {r['orphaned']} de {r['partitions']}")
    print(f"saída da 1ª antiga -> 1ª partição numa nova: {r['old_exit_to_new_assign_s']}s")
    print(f"deploy: {r['deploy_s']}s | exit codes das antigas: {r['exit_codes']} "
          f"(137 = SIGKILL, 143 = tratou SIGTERM)")
    print(f"rebalances: assigned={r['rebalance_assigned']} revoked={r['rebalance_revoked']}")
    if r["timeline"]:
        print("falhas de SLA por janela de entrada (s após início do deploy -> qtd):")
        peak = max(r["timeline"].values())
        for offset, count in r["timeline"].items():
            print(f"  {offset:>5}s {'#' * max(1, round(count / peak * 40))} {count}")


def print_table(results: list[dict]):
    """Linhas no formato da tabela da seção 7 do README."""
    print("\n| Rodada | enviadas | **falhas SLA** | antes do deploy | atrasadas | expiradas | perdidas "
          "| duplicadas | p50 (ms) | p99 (ms) | saída→partição (s) | shutdown máx (s) | exit codes | deploy (s) |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in results:
        flag = " ⚠️ saturado" if r["saturated"] else ""
        print(f"| {r['run']}{flag} | {r['sent']} | **{r['sla_fail']}** | {r['pre_deploy_fail']} | {r['late']} | "
              f"{r['expired']} | {r['lost']} | {r['duplicates']} | {r['p50']} | {r['p99']} | "
              f"{r['old_exit_to_new_assign_s']} | {r['shutdown_max_s']} | {r['exit_codes']} | {r['deploy_s']} |")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    results = [analyze(Path(p)) for p in sys.argv[1:]]
    for r in results:
        print_report(r)
    print_table(results)
