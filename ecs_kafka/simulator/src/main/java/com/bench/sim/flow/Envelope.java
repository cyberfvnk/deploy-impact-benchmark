package com.bench.sim.flow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Mensagem que percorre todo o fluxo (SQS -> Kafka -> Kafka -> SQS).
 * Carrega os timestamps necessários para medir a latência fim a fim.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Envelope {

    /** ID único, gerado pelo gerador de carga. */
    public String id;
    /** Momento em que a mensagem entrou na fila. O SLA de 26s conta a partir daqui. */
    public long createdAtMs;
    public long receivedAtMs;
    /** Task que recebeu do SQS e enviou o request. */
    public String originTask;
    /** Quantas vezes o SQS já entregou essa mensagem (>1 = reprocessamento). */
    public int receiveCount;
    /** Handle para apagar a msg da fila de entrada (qualquer task pode usar). */
    public String receiptHandle;
    /** Task que concluiu o fluxo. */
    public String completedBy;
    public long completedAtMs;
}
