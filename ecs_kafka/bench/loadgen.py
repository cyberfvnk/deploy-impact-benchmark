"""
Gerador de carga e coletor da fila final.

LoadGenerator  -> publica N msg/s na fila de entrada (bench-input) e grava cada id em sent.csv.
FinalCollector -> consome a fila final (bench-final) e grava cada conclusão em final.csv.

Comparando sent.csv com final.csv a análise mede latência, perdas e duplicadas.
Uso avulso (sem orquestrador):  python loadgen.py --rate 7 --seconds 60
"""

import argparse
import csv
import json
import threading
import time
import uuid
from pathlib import Path

from common import FINAL_QUEUE, INPUT_QUEUE, now_ms, queue_url, sqs_client


class LoadGenerator(threading.Thread):
    """Taxa constante: uma msg a cada 1/rate segundos (sem rajadas)."""

    def __init__(self, rate: float, out_path: Path):
        super().__init__(daemon=True, name="loadgen")
        self.rate = rate
        self.out_path = out_path
        self.stop_event = threading.Event()
        self.sent = 0
        self.errors = 0

    def run(self):
        sqs = sqs_client()
        url = queue_url(INPUT_QUEUE)
        interval = 1.0 / self.rate
        next_tick = time.monotonic()
        with self.out_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "createdAtMs"])
            while not self.stop_event.is_set():
                # createdAtMs é o início do relógio do SLA de 26s.
                env = {"id": uuid.uuid4().hex, "createdAtMs": now_ms()}
                try:
                    sqs.send_message(QueueUrl=url, MessageBody=json.dumps(env))
                    writer.writerow([env["id"], env["createdAtMs"]])
                    self.sent += 1
                except Exception:
                    self.errors += 1
                next_tick += interval
                time.sleep(max(0.0, next_tick - time.monotonic()))

    def stop(self):
        self.stop_event.set()
        self.join()


class FinalCollector(threading.Thread):
    """Lê a fila final continuamente. last_receive_ms permite esperar a fila "secar"."""

    def __init__(self, out_path: Path):
        super().__init__(daemon=True, name="collector")
        self.out_path = out_path
        self.stop_event = threading.Event()
        self.received = 0
        self.last_receive_ms = now_ms()

    def run(self):
        sqs = sqs_client()
        url = queue_url(FINAL_QUEUE)
        with self.out_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "createdAtMs", "completedAtMs", "collectedAtMs",
                             "originTask", "completedBy", "receiveCount"])
            while not self.stop_event.is_set():
                resp = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
                msgs = resp.get("Messages", [])
                for m in msgs:
                    env = json.loads(m["Body"])
                    writer.writerow([env.get("id"), env.get("createdAtMs"), env.get("completedAtMs"),
                                     now_ms(), env.get("originTask"), env.get("completedBy"),
                                     env.get("receiveCount")])
                if msgs:
                    f.flush()
                    sqs.delete_message_batch(QueueUrl=url, Entries=[
                        {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)])
                    self.received += len(msgs)
                    self.last_receive_ms = now_ms()

    def stop(self):
        self.stop_event.set()
        self.join()


def purge_queues():
    """Começa cada rodada com as filas vazias (sobras da rodada anterior distorcem tudo)."""
    sqs = sqs_client()
    for name in (INPUT_QUEUE, FINAL_QUEUE):
        sqs.purge_queue(QueueUrl=queue_url(name))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gera carga avulsa na fila de entrada.")
    parser.add_argument("--rate", type=float, default=7.0, help="msgs por segundo")
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--out", type=Path, default=Path("sent.csv"))
    args = parser.parse_args()

    gen = LoadGenerator(args.rate, args.out)
    gen.start()
    time.sleep(args.seconds)
    gen.stop()
    print(f"enviadas={gen.sent} erros={gen.errors}")
