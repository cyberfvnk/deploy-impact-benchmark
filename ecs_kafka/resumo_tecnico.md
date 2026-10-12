# Deploy sem impacto — resumo técnico

Oct 11, 2026 · @mônica

O graceful shutdown com entrypoint `exec java` zera as falhas de SLA no deploy: \~80 → 0 por deploy na carga do horário de deploy e \~1.150 → 0 no pico, em duas baterias com simulador fiel ao fluxo.

## Causa

O entrypoint em shell form (`java ...` sem `exec`) impede o Java de receber o SIGTERM do ECS. Cada task antiga morre por SIGKILL após o `stopTimeout` (30s), sem LeaveGroup.

1. As partições de resposta que eram das antigas ficam congeladas até o `session.timeout.ms` (45s).
2. Mensagens SQS em voo nas antigas, sem ack, só voltam após o visibility timeout (30s), já acima do SLA de 26s.
3. Agravante: `[CooperativeSticky, Range]` usa o assignor cooperative, mas o protocolo de rebalance é **eager**, porque o Range só suporta eager. Toda task que entra ou sai para o grupo inteiro.

Evidências no Datadog: `session.timeout.ms = 45000`, tasks novas entrando com `Assigned partitions: []`, antigas morrendo sem logs de fechamento do Kafka e atraso até as novas receberem partições. A confirmação do modo eager em produção ainda está pendente: buscar `Revoke previously assigned partitions` nas tasks antigas.

## Opções avaliadas

Simulador com as versões de produção (Java 11, Spring Boot 2.7, kafka-clients 3.1, consumo SQS igual ao Spring Cloud AWS 2.4), 6 tasks, 6 partições, 7 msg/s (1,2 por task, como no horário de deploy). Média de 2 rodadas por cenário; resultados confirmados numa 2ª bateria completa.

| Cenário | Falhas de SLA por deploy | Saída → partição na nova | Exit code das antigas | Duração do deploy |
| --- | --- | --- | --- | --- |
| Hoje (shell, Coop+Range, max 200%) | 83 | 44,4s | 137 (SIGKILL) | 62s |
| Graceful (exec + shutdown ordenado) | **0** | 1,3s | 143 (SIGTERM) | 42s |
| Graceful + remover Range (sem ganho sobre o graceful) | **0** | 2,1s | 143 | 44s |
| Graceful + maxPercent 125% | **0** | 1,0s | 143 | 69s |
| Só entrypoint `exec` | 4 (−95%) | 1,3s | 143 | 27s |
| Só remover Range | 162 (2× pior) | 44,9s | 137 | 60s |
| Só maxPercent 125% | 510 (6× pior) | 101s | 137 | 108s |

Nenhuma mensagem perdida em nenhum cenário. Sem graceful, remover o Range deixa 100% das partições com as antigas até o SIGKILL, e o 125% gera várias ondas de SIGKILL, cada uma com 45s de congelamento.

## Estresse

O graceful manteve **zero falhas** no pico (600 TPS, 10 msg/s por task), com integrações 2× lentas (API 500ms, integração via Kafka 2s) e com os dois juntos. Hoje, no pico, são \~1.150 falhas por deploy (\~14× mais), porque os 45s de congelamento pegam 8× mais mensagens.

Shutdown mais longo com graceful: 1–8s sob carga e até 18,7s com a fila **vazia**, porque o graceful espera o long poll do SQS já aberto (até 20s).

## Recomendação

1. **Graceful shutdown**: `exec java` (ou `ENTRYPOINT ["java", ...]`). Ordem: parar o listener SQS, terminar as mensagens em voo com ack, fechar os consumers Kafka (LeaveGroup), flush dos producers. Tudo dentro do `stopTimeout`.
2. **Remover o Range** é opcional: com graceful não mostrou ganho (troca de partição \~1s mais lenta); só compensa com autoscaling ou quedas frequentes. Se fizer, só depois do graceful em produção.
3. **Manter maxPercent em 200%**: o 125% não traz ganho com graceful e alonga o deploy.
4. Opcional: `waitTimeOut` do listener SQS de 20s para 5s (shutdown ≤ \~6s com fila vazia) e/ou `stopTimeout` de 60s.

O 1º deploy com a correção ainda tem o impacto de hoje, porque desliga tasks antigas sem graceful (medido: 76 falhas, igual a hoje): fazer no horário de menor volume. Aceite no 2º deploy (Datadog): um `LeaveGroup` por task antiga, nenhum pico de timeout ou desfazimento, e as tasks novas recebendo partições em segundos.

Limitação: os números são relativos ao simulador, em escala de 6 tasks. A direção e a ordem de grandeza valem; o graceful da app real precisa ser validado em homologação.
