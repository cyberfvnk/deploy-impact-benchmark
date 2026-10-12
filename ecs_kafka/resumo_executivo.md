# Deploy sem impacto — resumo executivo

Oct 11, 2026 · @mônica

Uma correção de baixo esforço na forma como a aplicação desliga elimina os erros que hoje acontecem em todo deploy, inclusive no horário de pico.

## O problema

Em todo deploy, o sistema passa cerca de 2 minutos com parte das transações estourando o limite de 26 segundos e caindo no fluxo de desfazimento.

A causa: as instâncias antigas são desligadas à força, sem terminar o que estavam fazendo e sem avisar as demais. Por cerca de 45 segundos, parte das respostas fica parada até o sistema perceber que elas saíram.

No teste em escala reduzida, cada deploy gerou cerca de 80 transações fora do prazo com o volume do horário atual de deploy e cerca de 1.150 com o volume de pico.

## O que testamos

Cada opção foi testada sozinha e em combinação, num ambiente que reproduz o fluxo de produção. Nenhuma transação foi perdida em nenhum teste, e uma segunda bateria completa confirmou todos os resultados.

| Opção | Transações fora do prazo por deploy | Veredito |
| --- | --- | --- |
| Como está hoje | \~80 | Problema reproduzido |
| **Desligamento ordenado (graceful)** | **0** | **Resolve** |
| Graceful + ajuste do Kafka (remover Range) | 0 | Resolve, mas sem ganho sobre o graceful sozinho |
| Graceful + deploy em ondas menores (125%) | 0 | Sem ganho extra; deploy 65% mais longo |
| Só ajuste do Kafka (remover Range) | 162 | Piora 2× se feito sozinho |
| Só deploy em ondas menores (125%) | 510 | Piora 6× se feito sozinho |

O graceful também manteve zero falhas no pico de 600 TPS e com as integrações externas 2× mais lentas.

## Decisão proposta

Adotar o desligamento ordenado (graceful) e manter o deploy como está (200%).

|  |  |
| --- | --- |
| Esforço | Baixo: ajustes de configuração e de inicialização |
| Risco | Baixo: muda só a forma de desligar, sem mudança de arquitetura |
| Efeito colateral | Cada instância leva alguns segundos a mais para desligar; o deploy como um todo fica mais rápido |
| Ganho esperado | Deploys sem transações fora do prazo, inclusive no pico |

Próximos passos:

- [ ] Aplicar o graceful e validar em homologação
- [ ] Primeiro deploy em produção no horário de menor volume: ele ainda tem o impacto de hoje, porque desliga as instâncias antigas; o ganho aparece a partir do segundo deploy
- [ ] Segundo deploy: confirmar zero timeouts. Ajuste do Kafka (remover Range) só se houver autoscaling ou quedas frequentes

Os números vêm de um teste em escala reduzida e indicam direção e ordem de grandeza, não valores exatos de produção.
