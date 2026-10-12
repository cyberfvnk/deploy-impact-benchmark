# Benchmark: impacto de deploy (SQS → Kafka → Dynamo)

> Última atualização: 2026-10-11

## 1. Resumo

| | |
|---|---|
| **Problema** | ~2 min de erros/timeouts (SLA 26s) a cada deploy em produção |
| **Causa (confirmada nos logs)** | Entrypoint em *shell form*: o Java não recebe o SIGTERM → morre por SIGKILL após 30s → as partições de resposta das antigas **congelam por 45s** (`session.timeout.ms`) e as msgs SQS em voo só voltam após o visibility (30s), já expiradas |
| **Agravante** | `[Cooperative, Range]` roda o rebalance em modo **eager**: toda task que entra/sai para o grupo inteiro (3.2) |
| **Recomendação** | Graceful shutdown **incluindo** `exec java`; manter maxPercent 200; remover o Range é opcional (seção 7) |
| **Resultado** | Falhas de SLA por deploy: **~80 hoje → 0 com graceful**, e **~1.150 → 0 no pico**. Também 0 com dependências 2× lentas. O ganho começa no **2º deploy** |
| **Status** | ✅ Concluído e confirmado em 2 baterias (52 rodadas válidas). Próximo passo: aplicar na app real e validar com o Apêndice A |

## 2. Produção

| Item | Valor |
|---|---|
| Stack | ECS, Java 11, Spring Boot 2.7 (kafka-clients 3.1), Spring Cloud AWS Messaging 2.4.4 |
| Escala | 60 tasks (1 vCPU / 4 GB), ~50M msgs/dia; ~70 msg/s no horário de deploy, ~600 no pico |
| Fluxo | SQS → API + Dynamo → Kafka request → Kafka response → Dynamo → SQS final. **Ack manual no fim** |
| SQS | 10 msgs/poll, polling e threads padrão, visibility **30s**, DLQ só para erros específicos |
| Kafka | **60 partições**, 1 consumer por task, mesmo group id para todas, `[CooperativeSticky, Range]`, `session.timeout.ms` 45s |
| ECS | `stopTimeout` 30s, minHealthy 100, **maxPercent 200**; health 10s/6s/start 60s/2 retries |
| Entrypoint | **shell form** (`java ...` sem `exec`) |
| SLA | 26s; estourou → fluxo de desfazimento (fora do escopo). Latência normal ~1s |

## 3. Diagnóstico

### 3.1 Um deploy hoje
| t | O que acontece |
|---|---|
| 0s | Sobem 60 tasks novas → rebalance **eager**: as 120 param, as 60 partições são redistribuídas ao acaso |
| ~60–80s | Novas healthy → SIGTERM nas 60 antigas ao mesmo tempo; o `sh` (PID 1) não repassa, elas seguem trabalhando |
| +30s | **SIGKILL** nas 60. Sem LeaveGroup, sem terminar o que estava em voo |
| +30 a +75s | Partições de resposta que estavam com as antigas (~metade) **congeladas** até o session timeout (45s) |
| +30 a +60s | Msgs SQS em voo nas mortas voltam após 30s de visibility → já passaram de 26s |

### 3.2 `[Cooperative, Range]` = assignor cooperative, protocolo eager
O grupo escolhe o **assignor** = 1º da lista que todos suportam → `cooperative-sticky` (é o que aparece no log). Mas o **protocolo de rebalance** é o mais avançado que *todos os assignors da lista* suportam, e o Range só suporta eager. Em eager, todo membro devolve todas as partições a cada rebalance. Por isso a migração oficial do Kafka tem dois passos: adicionar o cooperative e depois remover o Range.

Prova no simulador (mesmo kafka-clients 3.1): a antiga dona da `p0` registrou `Revoke previously assigned partitions bench-response-0` e recebeu a **mesma** `p0` de volta, o que não acontece no cooperativo.

### 3.3 Evidências em produção (Datadog)
| Evidência | Status |
|---|---|
| `session.timeout.ms = 45000` no startup | ✅ |
| Novas entram com `Assigned partitions: []` | ✅ |
| Antigas morrem no meio do processamento, sem logs de fechamento do Kafka | ✅ |
| Atraso entre a morte das antigas e o `Adding newly assigned partitions` nas novas | ✅ |
| Antigas revogando todas as partições quando as novas entram (eager) | ⏳ busca 5 do Apêndice A |

Descartado: resposta presa à task que enviou. Como o group id é o mesmo para todas, qualquer task processa a resposta (fluxo stateless).

## 4. Correções avaliadas

### 4.1 Entrypoint × graceful
O entrypoint resolve **se** o SIGTERM chega ao Java; o graceful define **o que** a app faz com ele, **em que ordem** e **em quanto tempo**.

| | Só `exec java` | Graceful (entrypoint + shutdown ordenado) |
|---|---|---|
| No SIGTERM | Shutdown padrão do Spring: para o listener SQS, fecha os consumers Kafka (LeaveGroup), fecha os producers | Para de ler o SQS, termina e dá ack no que está em voo, fecha o Kafka por último, tudo dentro do `stopTimeout` |
| Resultado (D1 × S1) | 83 → 4 falhas (−95%): sobram msgs abandonadas no meio do shutdown | 83 → 0 |

O graceful precisa de:
1. **Entrypoint** `exec java ...` (ou `ENTRYPOINT ["java", ...]`).
2. **Ordem**: SQS para de receber → msgs em voo terminam (ack) → consumers Kafka fecham (LeaveGroup) → producers fazem flush.
3. **Tempo**: soma < `stopTimeout`. O mais lento é esperar o long poll do SQS já aberto (até 20s, com fila vazia). Opcional: `waitTimeOut` do listener em **5s** e/ou `stopTimeout` de 60s.

### 4.2 maxPercent 200 → 125: não recomendado
Sem graceful, cada onda é um novo SIGKILL com 45s de congelamento (D2: 6× pior). Com graceful, não traz ganho e alonga o deploy em ~65% (S2).

### 4.3 Remover o Range: opcional, sem ganho medido
Ativa o protocolo cooperativo de verdade (só param as partições que mudam de dono).

| Prós | Contras |
|---|---|
| Sem parada geral a cada task que entra/sai: útil com autoscaling ou quedas frequentes (não testados) | Com graceful, nenhum ganho no SLA, p99 ou deploy; troca de partição ~1s mais lenta (2 rodadas de rebalance) |
| Config passa a dizer o que realmente roda | **Sem graceful piora 2×** (D3): as antigas ficam com todas as partições até o SIGKILL |

Se fizer, só depois do graceful em produção. Junto no 1º deploy (T2) ficou na variação de hoje, sem ganho.

### 4.4 Não testadas
| Ideia | Prós | Contras |
|---|---|---|
| Visibility timeout < SLA (ex.: 20s) | Msg abandonada volta a tempo de cumprir os 26s | Reentrega de msg lenta ainda em processamento |
| Static membership (`group.instance.id`) | Evita rebalance em restart | No ECS o ID muda a cada deploy |

## 5. Benchmark

### 5.1 Abordagem
Teste "cego" (sem a app real): um simulador fiel ao fluxo compara cenários entre si. Prova o **mecanismo** e a **melhora relativa**, não números absolutos da AWS.

| Componente | Escolha | Por quê |
|---|---|---|
| Runtime | Colima + Docker | `docker stop -t 30` = SIGTERM → 30s → SIGKILL, igual ao ECS; 1 container = 1 task |
| Kafka | Real (`apache/kafka` 3.8, KRaft) | É a variável sob teste |
| SQS | ElasticMQ | Reproduz visibility timeout; leve |
| API + Dynamo | Mock de latência na app | Não são a variável |
| Integração | Container `responder` fora do deploy | Na produção ela não reinicia junto |
| App | Spring Boot 2.7 + Java 11; consumo SQS imitando Spring Cloud AWS 2.4 (1 poll, lote de 10, stop 20s) | Mesmos defaults de produção |

### 5.2 Escala
| Item | Produção | Local |
|---|---|---|
| Tasks / partições | 60 / 60 | 6 / 6 |
| Carga | 70 msg/s (pico 600) | 7 msg/s (pico 60): mesma carga por task |
| Task | 1 vCPU / 4 GB | 0,5 vCPU / 512 MB |
| Latência | ~1s | API 250 + integração 500 + Dynamo 3×40 ms (+ jitter e cauda de 1%); pico com Dynamo 10ms |

### 5.3 Cenários
S e D simulam do 2º deploy em diante (antigas e novas com a mesma config); T simula o 1º deploy da correção.

| ID | Entrypoint | Shutdown | Assignors | maxPercent | Pergunta |
|---|---|---|---|---|---|
| **S0** | shell | — | Coop,Range | 200 | Baseline = produção hoje |
| **S1** | exec | graceful | Coop,Range | 200 | Quanto o graceful resolve? |
| S2 | exec | graceful | Coop,Range | 125 | O 125 ainda ajuda? |
| S3 | exec | graceful | Coop | 200 | Remover o Range ajuda com graceful? |
| D1 | exec | padrão Spring | Coop,Range | 200 | Só o entrypoint |
| D2 | shell | — | Coop,Range | 125 | Só o maxPercent |
| D3 | shell | — | Coop | 200 | Só remover o Range |
| T1 | shell → exec | — → graceful | Coop,Range | 200 | 1º deploy da correção |
| T2 | shell → exec | — → graceful | Coop,Range → Coop | 200 | 1º deploy com o Range removido junto |

Estresse: **pico** (60 msg/s = 600 TPS em prod), **dependências lentas** (API 500ms, integração 2s) e os dois juntos.

### 5.4 Métricas
| Métrica | Leitura |
|---|---|
| **Falhas de SLA** (principal) | Msgs não concluídas em 26s: atrasadas + expiradas (desfazimento) + perdidas |
| Saída → partição | Da morte da 1ª antiga até uma nova receber partição (hoje ≈ 45s) |
| Exit codes das antigas | 137 = SIGKILL; 143 = tratou o SIGTERM |
| Shutdown máx | SIGTERM → parada da antiga mais lenta (limite 30s) |
| Partições órfãs | Partições que eram das antigas quando morreram |
| Alerta "⚠️ saturado" | Latência subindo no aquecimento = rodada inválida (falta de capacidade, não efeito do deploy) |

### 5.5 Limitações
| Limitação | Efeito |
|---|---|
| 125% com 6 tasks = ondas de 1 (prod: 25%) | Rodado com ondas de 2 (33%) |
| Fração de partições órfãs em eager é aleatória (2 a 4 de 6) | Variação entre rodadas: hoje deu 62 a 89 falhas |
| Consumo SQS reimplementado (SDK v2), não o Spring Cloud AWS real | Imita os defaults; detalhes internos podem diferir. No shutdown padrão (D1) o simulador cancela o long poll na hora, e o Spring Cloud AWS real espera até 20s: o D1 local é mais rápido do que a produção seria |
| Task local menor, imagens arm64 | Irrelevante para o mecanismo |

## 6. Resultados

Falhas de SLA por deploy, média das rodadas (bateria 1 → bateria 2). Todas as rodadas válidas, nenhuma saturada, nenhuma msg perdida.

| Cenário | Normal | Pico | Lento | Pico + lento | Saída → partição | Exit |
|---|---|---|---|---|---|---|
| **S0** hoje | 83 → 73 | 1.175 → 1.134 | — | — | ~45s | 137 |
| **S1** graceful | **0 → 0** | **0 → 0** | **0 → 0** | **0 → 0** | 0,1–3s | 143 |
| S2 graceful + 125% | 0 → 0 | — | — | — | 0,3–1,4s | 143 |
| S3 graceful sem Range | 0 → 0 | 0 → 0 | 0 → 0 | — | 0,2–3s | 143 |
| D1 só entrypoint | 4 → 4,5 | — | — | — | 0,5–2s | 143 |
| D2 só 125% | 510 → 485 | — | — | — | 68–135s | 137 |
| D3 só remover Range | 162 → 166 | — | — | — | ~45s | 137 |
| T1 1º deploy | — → 76 | — | — | — | ~45s | 137 |
| T2 1º deploy sem Range | — → 85 | — | — | — | ~45s | 137 |

Conclusões:
1. **O simulador reproduz a produção**: SIGKILL em todas as antigas e ~45s até a partição ser reatribuída.
2. **Graceful zera as falhas** em todas as condições (normal, pico, lentidão, pico + lentidão). Shutdown máximo: 1–8s sob carga; até 18,7s com a fila vazia, por causa do long poll.
3. **O entrypoint sozinho resolve ~95%**; o shutdown ordenado elimina o resto.
4. **125% e remover o Range sozinhos pioram** (6× e 2×). Com graceful, nenhum dos dois traz ganho.
5. **No pico o problema escala com a carga**: ~14× mais falhas, porque os 45s de congelamento pegam 8× mais mensagens.
6. **O 1º deploy da correção dói como hoje** (T1 ≈ S0): quem é desligado ainda são as antigas sem graceful.

Detalhe por rodada: `.venv/bin/python bench/analyze.py runs/<cenário>-*`.

## 7. Recomendação

| Ordem | Ação | Base |
|---|---|---|
| 1 | **Graceful**: `exec java` + shutdown ordenado dentro do `stopTimeout` | S1: 0 falhas em todas as condições |
| — | **Manter** maxPercent 200 | D2 6× pior; S2 sem ganho e deploy mais longo |
| Opcional | `waitTimeOut` do SQS em 5s e/ou `stopTimeout` 60s | Folga no shutdown com fila vazia |
| Opcional | Remover o Range, só depois do graceful em produção | S3 = S1; útil com autoscaling ou quedas frequentes |

**Rollout:** o 1º deploy com a correção ainda tem o impacto de hoje (as antigas não têm graceful; `stopTimeout` e `waitTimeOut` só valem para as novas). Fazer no horário de menor volume. **Aceite no 2º deploy**: busca 2 do Apêndice A com um LeaveGroup por task antiga e busca 4 sem pico de timeouts/desfazimento.

## Apêndice A — Buscas no Datadog
Janela de ~10 min em volta de um deploy; conferir no painel lateral de um log as tags disponíveis (`task_arn`, `task_version`).

| # | Visualização | Busca | Leitura |
|---|---|---|---|
| 1 | Timeseries, count, group by `task_version`, 5s | `service:<app>` | Revisão antiga cai a zero de uma vez = morte das antigas (**M1**) |
| 2 | List | `service:<app> ("LeaveGroup" OR "consumer is being closed" OR "Metrics scheduler closed")` | Hoje: zero nas antigas (SIGKILL). Com graceful: **1 por task antiga** (aceite) |
| 3 | List, por tempo | `service:<app> ("Successfully joined group" OR "Assigned partitions" OR "Adding newly assigned partitions")` | 1º `Adding newly assigned partitions` com partição numa nova (**M2**). Hoje M2 − M1 ≈ 45s |
| 4 | Timeseries, count, 10s | `service:<app> (timeout OR desfaz*)` (ajustar ao log real) | Pico entre M1 e M2. Com graceful: sem pico (aceite) |
| 5 | List, só tasks antigas | `service:<app> "Revoke previously assigned partitions"` | Antigas revogando e recebendo as mesmas partições quando as novas entram = eager |

## Apêndice B — Como rodar
```bash
cd sandbox/kafka
colima start --cpu 6 --memory 8
python3 -m venv .venv && .venv/bin/pip install -r bench/requirements.txt          # uma vez
docker compose build responder && docker compose --profile build-only build image-shell   # imagens exec e shell

.venv/bin/python bench/deploy.py S1 --runs 2                                      # um cenário
.venv/bin/python bench/deploy.py S1 --runs 2 --rate 60 --dynamo-ms 10 --tag pico  # pico
TAG=b3 caffeinate -ims bench/batch-full.sh                                        # bateria completa (~2h30)
.venv/bin/python bench/analyze.py runs/S1-*                                       # relatório

colima stop
```
Durante um lote o Mac não pode dormir: na tomada, tampa aberta e `caffeinate`. Fechar a tampa só com `sudo pmset -a disablesleep 1` (desfazer com `0` depois).

| Opção do `deploy.py` (padrão) | Uso |
|---|---|
| `--runs` (1), `--tasks` (6), `--rate` (7) | Repetições, tasks, msg/s total |
| `--warmup` (60), `--observe` (90), `--drain` (35) | Janelas da rodada, em segundos (~5 min por rodada) |
| `--wave-size` (regra do ECS) | Força o tamanho da onda (125% usou 2) |
| `--api-ms` (250), `--integration-ms` (500), `--dynamo-ms` (40) | Latências. Lentidão: `--api-ms 500 --integration-ms 2000`. Pico: `--dynamo-ms 10` |
| `--tag` | Rótulo no nome da pasta |
| `--task-cpus` (0.5), `--task-memory` (512m) | Recursos de cada task |

Saída de cada rodada em `runs/<cenário>[-tag]-<data-hora>/`: `logs/v1-*.log` (antigas), `logs/v2-*.log` (novas), `orchestrator.jsonl` (SIGTERM, parada e exit code de cada task), `sent.csv` e `final.csv`. Andamento de um lote em `runs/<tag>-console.log`.

```bash
grep -E "Revoke previously|Adding newly assigned" logs/v1-0.log   # rebalances (eager: revoga e recebe a mesma de volta)
tail -5 logs/v1-0.log                                              # antiga termina sem LeaveGroup = SIGKILL
grep -h "type=EXPIRED" logs/*.log | head                           # msgs > 26s enviadas ao desfazimento
```

## Apêndice C — Arquivos
```
sandbox/kafka/
├── .env                    infra do cenário (partições, visibility, latência da integração)
├── docker-compose.yml      kafka, kafka-init, elasticmq, responder (+ build da imagem shell)
├── infra/elasticmq.conf    filas bench-input e bench-final
├── bench/                  deploy.py (orquestrador), loadgen.py (carga + coletor), analyze.py,
│                           common.py, batch-full.sh (bateria completa)
├── runs/                   saída das rodadas (fora do git)
└── simulator/              app Spring Boot 2.7 / Java 11
    ├── Dockerfile          ENTRYPOINT_MODE=exec|shell
    └── src/main/java/com/bench/sim/
        ├── config/         SimProperties, KafkaConfig (log de rebalance), SqsConfig
        ├── flow/           SqsPoller (consumo SQS + shutdown), Processor, ReplyListener, SqsGateway
        ├── metrics/Events  eventos EVT no log
        └── responder/      integração simulada
```

Variáveis do simulador (ver `application.yml`): `SIM_SHUTDOWN_MODE` (`DEFAULT` | `GRACEFUL`), `KAFKA_ASSIGNORS`, `KAFKA_LISTENER_CONCURRENCY` (1), `SQS_*` (1 poller, 11 workers, lote 10, long poll 20s, stop 20s), `LAT_*`.
Eventos `EVT`: `READY`, `RECEIVED`, `SENT_TO_KAFKA`, `COMPLETED`, `EXPIRED`, `ABORTED`, `REBALANCE_ASSIGNED/REVOKED/LOST`, `SHUTDOWN_BEGIN/DONE`.

## Apêndice D — Decisões
| Decisão | Motivo |
|---|---|
| Teste cego via simulador, resultado relativo | Sem acesso à app |
| Colima + Docker; Kafka e SQS reais; API/Dynamo mock; ElasticMQ | `docker stop` imita o ECS; isolar as variáveis; pouca RAM |
| Simulador com defaults de produção (Spring Cloud AWS 2.4, 1 consumer/task, ack no fim, msg > 26s vai ao desfazimento) | Fidelidade |
| Escala 6 tasks / 6 partições / mesma carga por task | Rodar local mantendo a proporção |
| Métrica principal = falhas de SLA; "partições órfãs" no lugar de "maior pausa" | Congelamento parcial não aparece numa pausa global |
| Range: "inofensivo" → corrigido para eager → opcional (sem ganho medido) | Logs do simulador; S1 × S3 |
| Pico com Dynamo 10ms + alerta de saturação | A 40ms o simulador saturava antes do deploy (1ª tentativa descartada) |
| Cenários de transição (T1, T2) | O ganho só aparece no 2º deploy; testar o 1º e o Range junto |
| Rodadas descartadas: ciclo de reprocessamento, Mac dormindo, saturação | Corrigidos no simulador/orquestrador; dados em `runs/_*` |
