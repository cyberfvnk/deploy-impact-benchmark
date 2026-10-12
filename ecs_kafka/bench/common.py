"""Utilidades compartilhadas pelos scripts do benchmark (SQS local, docker, tempo)."""

import json
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional

import boto3

# Raiz do projeto (sandbox/kafka): onde ficam docker-compose.yml e a pasta runs/.
ROOT = Path(__file__).resolve().parent.parent
RUNS_DIR = ROOT / "runs"

# Rede criada pelo docker compose (projeto "kafka-bench").
NETWORK = "kafka-bench_default"

# ElasticMQ visto do Mac. A URL da fila é montada à mão porque a URL que o
# ElasticMQ devolve usa o host "elasticmq", que só existe dentro da rede docker.
SQS_ENDPOINT = "http://localhost:9324"
INPUT_QUEUE = "bench-input"
FINAL_QUEUE = "bench-final"


def now_ms() -> int:
    return int(time.time() * 1000)


def sqs_client():
    """Cliente SQS apontando para o ElasticMQ (credenciais fake, não são validadas)."""
    return boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="x",
        aws_secret_access_key="x",
    )


def queue_url(name: str) -> str:
    return f"{SQS_ENDPOINT}/000000000000/{name}"


def docker(*args: str, check: bool = True, env: Optional[dict] = None) -> str:
    """Executa um comando docker e devolve o stdout. `env` sobrescreve variáveis (ex.: interpolação do compose)."""
    full_env = {**os.environ, **env} if env else None
    result = subprocess.run(["docker", *args], capture_output=True, text=True, check=False, env=full_env)
    if check and result.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} falhou: {result.stderr.strip()}")
    return result.stdout.strip()


class EventLog:
    """Linha do tempo do orquestrador (JSON por linha). É a "verdade" dos horários do deploy."""

    def __init__(self, path: Path):
        self._file = path.open("a")
        self._lock = threading.Lock()

    def emit(self, type_: str, **fields):
        record = {"ts": now_ms(), "type": type_, **fields}
        with self._lock:
            self._file.write(json.dumps(record) + "\n")
            self._file.flush()
        print(f"[{time.strftime('%H:%M:%S')}] {type_} {fields if fields else ''}")

    def close(self):
        self._file.close()
