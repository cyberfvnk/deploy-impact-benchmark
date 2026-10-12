package com.bench.sim.flow;

import java.util.concurrent.TimeUnit;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.bench.sim.config.SimProperties;
import com.bench.sim.metrics.Events;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

/**
 * Etapa 1 do fluxo (roda nas threads worker do SqsPoller):
 *   msg SQS -> API (mock) -> Dynamo (mock) -> publica no tópico de request.
 */
@Component
@Profile("!responder")
public class Processor {

    private final KafkaTemplate<String, String> kafka;
    private final SqsGateway sqs;
    private final ObjectMapper mapper;
    private final SimProperties props;
    private final Events events;

    public Processor(KafkaTemplate<String, String> kafka, SqsGateway sqs, ObjectMapper mapper,
                     SimProperties props, Events events) {
        this.kafka = kafka;
        this.sqs = sqs;
        this.mapper = mapper;
        this.props = props;
        this.events = events;
    }

    public void process(Message m) {
        Envelope env;
        try {
            env = mapper.readValue(m.body(), Envelope.class);
        } catch (Exception e) {
            events.emit("BAD_MESSAGE", m.messageId(), null);
            return;
        }
        env.receivedAtMs = System.currentTimeMillis();
        env.originTask = props.getTaskId();
        env.receiptHandle = m.receiptHandle();
        env.receiveCount = Integer.parseInt(
                m.attributes().getOrDefault(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT, "1"));
        long ageMs = env.receivedAtMs - env.createdAtMs;
        events.emit("RECEIVED", env.id, "receiveCount=" + env.receiveCount + " ageMs=" + ageMs);

        // Como em produção: msg que já passou do SLA não é reprocessada, vai para o
        // fluxo de desfazimento (fora do escopo) e recebe ack.
        if (ageMs > props.getSlaTimeout().toMillis()) {
            events.emit("EXPIRED", env.id, "ageMs=" + ageMs + " receiveCount=" + env.receiveCount);
            sqs.tryDeleteInput(env.receiptHandle);
            return;
        }

        try {
            Latency.sleep(props.getLatency().getApi());
            Latency.sleep(props.getLatency().getDynamoWrite());

            // Envio síncrono: só seguimos quando o broker confirmou.
            kafka.send(props.getTopics().getRequest(), env.id, mapper.writeValueAsString(env))
                    .get(10, TimeUnit.SECONDS);
            // Sem ack aqui: como em produção, o ack é no fim do fluxo (ReplyListener).
            events.emit("SENT_TO_KAFKA", env.id, null);
        } catch (InterruptedException e) {
            // Shutdown não-graceful interrompeu a thread no meio do processamento.
            // A msg não foi apagada: volta para a fila depois do visibility timeout.
            Thread.currentThread().interrupt();
            events.emit("ABORTED", env.id, "stage=processing");
        } catch (Exception e) {
            events.emit("ERROR", env.id, "stage=processing error=" + e.getClass().getSimpleName());
        }
    }
}
