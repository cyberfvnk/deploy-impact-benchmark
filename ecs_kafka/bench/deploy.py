"""
Orquestrador: executa UM cenário imitando um rolling deploy do ECS.

Fluxo de uma rodada:
  1. sobe a infra fixa (kafka, elasticmq, responder) e limpa as filas
  2. sobe N tasks "v1" e espera ficarem healthy
  3. liga carga + coletor e aquece (warmup)
  4. DEPLOY: em ondas, sobe tasks "v2", espera healthy e para as "v1"
     com `docker stop -t <stopTimeout>` (SIGTERM -> espera -> SIGKILL, igual ao ECS)
  5. observa, desliga a carga e espera a fila final secar
  6. salva logs/exit codes de todas as tasks e remove os containers

Saída em runs/<cenário>-<timestamp>/  ->  analisar com: python analyze.py runs/<pasta>

Uso:  python deploy.py S0            (cenários na tabela SCENARIOS abaixo)
      python deploy.py S1 --runs 3
"""

import argparse
import math
import threading
import time
from datetime import datetime

from common import NETWORK, ROOT, RUNS_DIR, EventLog, docker, now_ms
from loadgen import FinalCollector, LoadGenerator, purge_queues

# -----------------------------------------------------------------------------
# Cenários (README, seção 5.4).
#   image     -> bench-sim:shell = entrypoint sem exec (produção hoje)
#                bench-sim:exec  = entrypoint com exec (Java recebe SIGTERM)
#   shutdown  -> SIM_SHUTDOWN_MODE (só tem efeito com a imagem exec)
#   max_pct   -> maximumPercent do ECS
#   v2        -> (transição) config das tasks novas, quando difere das antigas
#   assignors -> COOP_RANGE = produção (assignor cooperative, mas protocolo EAGER por causa do Range)
#                COOP       = só cooperative (protocolo COOPERATIVE de verdade)
# -----------------------------------------------------------------------------
COOP_RANGE = ("org.apache.kafka.clients.consumer.CooperativeStickyAssignor,"
              "org.apache.kafka.clients.consumer.RangeAssignor")
COOP = "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"

SCENARIOS = {
    "S0": dict(image="bench-sim:shell", shutdown="DEFAULT", assignors=COOP_RANGE, max_pct=200,
               desc="Produção hoje (entrypoint shell)"),
    "S1": dict(image="bench-sim:exec", shutdown="GRACEFUL", assignors=COOP_RANGE, max_pct=200,
               desc="Graceful (entrypoint exec + shutdown ordenado)"),
    "S2": dict(image="bench-sim:exec", shutdown="GRACEFUL", assignors=COOP_RANGE, max_pct=125,
               desc="Graceful + maxPercent 125"),
    "S3": dict(image="bench-sim:exec", shutdown="GRACEFUL", assignors=COOP, max_pct=200,
               desc="Graceful + só cooperative"),
    # Diagnóstico: isola o efeito de cada mudança sozinha.
    "D1": dict(image="bench-sim:exec", shutdown="DEFAULT", assignors=COOP_RANGE, max_pct=200,
               desc="Só entrypoint exec, shutdown padrão do Spring"),
    "D2": dict(image="bench-sim:shell", shutdown="DEFAULT", assignors=COOP_RANGE, max_pct=125,
               desc="Só maxPercent 125 (entrypoint shell, sem graceful)"),
    "D3": dict(image="bench-sim:shell", shutdown="DEFAULT", assignors=COOP, max_pct=200,
               desc="Só remover o Range (entrypoint shell, sem graceful)"),
    # Transição = o 1º deploy da correção: antigas (v1) ainda como hoje, novas (v2) já corrigidas.
    "T1": dict(image="bench-sim:shell", shutdown="DEFAULT", assignors=COOP_RANGE, max_pct=200,
               v2=dict(image="bench-sim:exec", shutdown="GRACEFUL", assignors=COOP_RANGE),
               desc="1º deploy: hoje -> graceful"),
    "T2": dict(image="bench-sim:shell", shutdown="DEFAULT", assignors=COOP_RANGE, max_pct=200,
               v2=dict(image="bench-sim:exec", shutdown="GRACEFUL", assignors=COOP),
               desc="1º deploy: hoje -> graceful + remover Range junto"),
}

# stopTimeout de produção (minimumHealthy 100 está embutido na lógica do rolling_deploy).
STOP_TIMEOUT_S = 30

# Health check igual ao da task definition de produção.
HEALTH_ARGS = [
    "--health-cmd", "curl -fs http://localhost:8080/actuator/health || exit 1",
    "--health-interval", "10s",
    "--health-timeout", "6s",
    "--health-start-period", "60s",
    "--health-retries", "2",
]


class Run:
    def __init__(self, scenario_id: str, args):
        self.sc = SCENARIOS[scenario_id]
        self.args = args
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        tag = f"-{args.tag}" if args.tag else ""
        self.run_id = f"{scenario_id}{tag}-{stamp}"
        self.dir = RUNS_DIR / self.run_id
        (self.dir / "logs").mkdir(parents=True)
        self.events = EventLog(self.dir / "orchestrator.jsonl")
        self.containers: list[str] = []
        self.stoppers: list[threading.Thread] = []

    # ---------------------------------------------------------------- tasks --
    def task_config(self, task_id: str) -> dict:
        """Config da task: a do cenário, ou a de `v2` para as tasks novas num cenário de transição."""
        if task_id.startswith("v2") and "v2" in self.sc:
            return {**self.sc, **self.sc["v2"]}
        return self.sc

    def task_env(self, task_id: str) -> dict:
        """Variáveis de cada task. Grupo Kafka único por rodada: sem estado herdado da anterior."""
        cfg = self.task_config(task_id)
        return {
            "TASK_ID": task_id,
            "KAFKA_BOOTSTRAP": "kafka:9092",
            "SQS_ENDPOINT": "http://elasticmq:9324",
            "SIM_SHUTDOWN_MODE": cfg["shutdown"],
            "KAFKA_ASSIGNORS": cfg["assignors"],
            "KAFKA_REPLY_GROUP_ID": f"bench-reply-{self.run_id}",
            "LAT_API_BASE_MS": str(self.args.api_ms),
            "LAT_DYNAMO_WRITE_BASE_MS": str(self.args.dynamo_ms),
            "LAT_DYNAMO_WRITE_JITTER_MS": str(max(1, self.args.dynamo_ms // 2)),
            "LAT_DYNAMO_READ_BASE_MS": str(self.args.dynamo_ms),
            "LAT_DYNAMO_READ_JITTER_MS": str(max(1, self.args.dynamo_ms // 2)),
        }

    def start_task(self, name: str):
        cmd = ["run", "-d", "--name", name, "--label", "bench=1", "--network", NETWORK,
               "--cpus", str(self.args.task_cpus), "--memory", self.args.task_memory, *HEALTH_ARGS]
        for key, value in self.task_env(name).items():
            cmd += ["-e", f"{key}={value}"]
        cmd.append(self.task_config(name)["image"])
        docker(*cmd)
        self.containers.append(name)
        self.events.emit("TASK_STARTED", task=name)

    def wait_healthy(self, names: list[str], timeout_s: int = 240):
        deadline = time.time() + timeout_s
        pending = set(names)
        while pending:
            if time.time() > deadline:
                raise TimeoutError(f"tasks não ficaram healthy: {sorted(pending)}")
            for name in list(pending):
                if docker("inspect", "-f", "{{.State.Health.Status}}", name, check=False) == "healthy":
                    pending.discard(name)
                    self.events.emit("TASK_HEALTHY", task=name)
            time.sleep(1)

    def stop_task_async(self, name: str, phase: str):
        """docker stop = SIGTERM, espera stopTimeout, SIGKILL. Exit 137 = morreu no SIGKILL."""
        def _stop():
            self.events.emit("TASK_SIGTERM", task=name, phase=phase)
            docker("stop", "-t", str(STOP_TIMEOUT_S), name, check=False)
            exit_code = docker("inspect", "-f", "{{.State.ExitCode}}", name, check=False)
            self.events.emit("TASK_STOPPED", task=name, phase=phase, exitCode=int(exit_code or -1))

        t = threading.Thread(target=_stop, daemon=True)
        t.start()
        self.stoppers.append(t)

    # --------------------------------------------------------------- deploy --
    def wave_size(self, n: int) -> int:
        """Quantas tasks novas cabem acima das atuais (o ECS arredonda para baixo)."""
        if self.args.wave_size:
            return self.args.wave_size
        return max(1, math.floor(n * self.sc["max_pct"] / 100) - n)

    def rolling_deploy(self, old: list[str]):
        """
        Simplificação do ECS com minimumHealthy=100: sobe uma onda de novas, espera healthy,
        manda parar a mesma quantidade de antigas e segue para a próxima onda sem esperar as
        antigas morrerem (task em stopping não conta para o maximumPercent).
        """
        n = len(old)
        size = self.wave_size(n)
        self.events.emit("DEPLOY_START", tasks=n, waveSize=size, maxPercent=self.sc["max_pct"])
        remaining = list(old)
        wave = 0
        while remaining:
            wave += 1
            batch = remaining[:size]
            remaining = remaining[size:]
            new = [f"v2-{wave}-{i}" for i in range(len(batch))]
            for name in new:
                self.start_task(name)
            self.wait_healthy(new)
            for name in batch:
                self.stop_task_async(name, phase="deploy")
        for t in self.stoppers:
            t.join()
        self.events.emit("DEPLOY_DONE")

    # ------------------------------------------------------------------ run --
    def execute(self):
        a = self.args
        self.events.emit("RUN_START", scenario=self.run_id, **self.sc, tasks=a.tasks, rate=a.rate,
                         warmup=a.warmup, observe=a.observe, apiMs=a.api_ms, integrationMs=a.integration_ms)

        # A latência da integração mora no responder: o compose recria o container se o valor mudar.
        docker("compose", "-f", str(ROOT / "docker-compose.yml"), "up", "-d", "--wait",
               "kafka", "kafka-init", "elasticmq", "responder",
               env={"LAT_INTEGRATION_BASE_MS": str(a.integration_ms)})
        purge_queues()

        v1 = [f"v1-{i}" for i in range(a.tasks)]
        for name in v1:
            self.start_task(name)
        self.wait_healthy(v1)
        time.sleep(15)  # deixa o grupo Kafka estabilizar antes da carga

        collector = FinalCollector(self.dir / "final.csv")
        loadgen = LoadGenerator(a.rate, self.dir / "sent.csv")
        collector.start()
        loadgen.start()
        self.events.emit("LOAD_START")

        time.sleep(a.warmup)
        self.rolling_deploy(v1)
        time.sleep(a.observe)

        loadgen.stop()
        self.events.emit("LOAD_STOP", sent=loadgen.sent, errors=loadgen.errors)

        # Espera a fila final ficar quieta (msgs reprocessadas voltam após o visibility timeout).
        # Teto de 5 min: se nunca acalmar, há um ciclo de reprocessamento (registrado no evento).
        drain_deadline = now_ms() + 300_000
        while now_ms() - collector.last_receive_ms < a.drain * 1000 and now_ms() < drain_deadline:
            time.sleep(1)
        collector.stop()
        self.events.emit("COLLECT_DONE", received=collector.received,
                         drainTimedOut=now_ms() >= drain_deadline)

        self.teardown()

    def teardown(self):
        """Salva logs e exit codes ANTES de remover os containers."""
        self.stoppers.clear()
        for name in self.containers:
            if docker("inspect", "-f", "{{.State.Running}}", name, check=False) == "true":
                self.stop_task_async(name, phase="teardown")
        for t in self.stoppers:
            t.join()
        for name in self.containers:
            logs = docker("logs", name, check=False)
            (self.dir / "logs" / f"{name}.log").write_text(logs)
            docker("rm", "-f", name, check=False)
        self.events.emit("RUN_DONE", dir=str(self.dir))
        self.events.close()


def cleanup_leftovers():
    """Remove containers de rodadas anteriores interrompidas."""
    ids = docker("ps", "-aq", "--filter", "label=bench=1", check=False).split()
    if ids:
        docker("rm", "-f", *ids, check=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executa um cenário de deploy.")
    parser.add_argument("scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--runs", type=int, default=1, help="repetições do cenário")
    parser.add_argument("--tasks", type=int, default=6, help="tasks (prod 60; local 6)")
    parser.add_argument("--rate", type=float, default=7.0, help="msg/s total (prod 70 em 60 tasks)")
    # Janelas mínimas que ainda cobrem todo o impacto: congelamento (45s) + SLA (26s) cabem em 90s;
    # visibility (30s) cabe no drain de 35s. O sistema estabiliza em segundos com essa carga.
    parser.add_argument("--warmup", type=int, default=60, help="segundos de carga antes do deploy")
    parser.add_argument("--observe", type=int, default=90, help="segundos de carga após o deploy")
    parser.add_argument("--drain", type=int, default=35, help="segundos sem msg na fila final para encerrar")
    parser.add_argument("--wave-size", type=int, default=0, help="força o tamanho da onda (0 = regra do ECS)")
    parser.add_argument("--api-ms", type=int, default=250, help="latência base da API síncrona (lentidão: 500)")
    parser.add_argument("--integration-ms", type=int, default=500,
                        help="latência base da integração via Kafka (lentidão: 2000)")
    # O listener de resposta processa 1 msg por vez por partição: 2 acessos ao Dynamo por msg
    # limitam a vazão por partição (40ms -> ~8 msg/s; 10ms -> ~25 msg/s). Pico de prod = 10 msg/s/partição.
    parser.add_argument("--dynamo-ms", type=int, default=40, help="latência base do Dynamo (pico: 10)")
    parser.add_argument("--tag", default="", help="rótulo da bateria no nome da pasta (ex.: pico, lento)")
    parser.add_argument("--task-cpus", type=float, default=0.5)
    parser.add_argument("--task-memory", default="512m")
    args = parser.parse_args()

    for i in range(args.runs):
        cleanup_leftovers()
        print(f"\n===== {args.scenario} rodada {i + 1}/{args.runs}: {SCENARIOS[args.scenario]['desc']} =====")
        run = Run(args.scenario, args)
        try:
            run.execute()
        except Exception as e:
            # Rodada inválida: guarda o que der para diagnóstico, limpa e segue para a próxima.
            run.events.emit("RUN_FAILED", error=repr(e))
            run.teardown()
            run.dir.rename(run.dir.with_name("_failed-" + run.dir.name))
