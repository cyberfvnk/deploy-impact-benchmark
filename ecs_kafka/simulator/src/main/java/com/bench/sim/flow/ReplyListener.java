package com.bench.sim.flow;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.bench.sim.config.SimProperties;
import com.bench.sim.metrics.Events;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Etapa 2 do fluxo: resposta da integração (tópico de response)
 *   -> Dynamo (mock) -> publica na fila SQS final.
 *
 * É este consumer que sofre com o rebalance: durante um rebalance eager
 * (Range) TODAS as tasks param de consumir respostas ao mesmo tempo.
 */
@Component
@Profile("!responder")
public class ReplyListener {

    private final SqsGateway sqs;
    private final ObjectMapper mapper;
    private final SimProperties props;
    private final Events events;

    public ReplyListener(SqsGateway sqs, ObjectMapper mapper, SimProperties props, Events events) {
        this.sqs = sqs;
        this.mapper = mapper;
        this.props = props;
        this.events = events;
    }

    // Mesmo group id para todas as tasks, como em produção.
    @KafkaListener(id = "reply-listener", topics = "${sim.topics.response}", groupId = "${sim.kafka.reply-group-id}")
    public void onReply(ConsumerRecord<String, String> record) throws Exception {
        Envelope env = mapper.readValue(record.value(), Envelope.class);

        // Qualquer task continua o fluxo (stateless): busca o contexto no Dynamo e grava o resultado.
        Latency.sleep(props.getLatency().getDynamoRead());
        Latency.sleep(props.getLatency().getDynamoWrite());
        env.completedBy = props.getTaskId();
        env.completedAtMs = System.currentTimeMillis();
        sqs.publishFinal(env);
        // Ack falho não pode virar exceção: o Spring Kafka repetiria o registro
        // (republicando na fila final e travando a partição).
        boolean acked = sqs.tryDeleteInput(env.receiptHandle);

        long e2e = env.completedAtMs - env.createdAtMs;
        events.emit("COMPLETED", env.id, "e2eMs=" + e2e
                + " slaBreached=" + (e2e > props.getSlaTimeout().toMillis())
                + " acked=" + acked
                + " origin=" + env.originTask + " receiveCount=" + env.receiveCount);
    }
}
