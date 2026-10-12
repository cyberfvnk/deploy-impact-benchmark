package com.bench.sim.responder;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.bench.sim.config.SimProperties;
import com.bench.sim.flow.Latency;

/**
 * Simula a INTEGRAÇÃO externa (profile "responder"):
 *   lê o tópico de request e, após a latência da integração, publica no tópico de response.
 *
 * Agenda o envio em vez de dormir na thread do listener: assim uma resposta lenta
 * não segura as próximas (a integração real atende requisições em paralelo).
 * Mesma chave (id) -> a resposta vai sempre para a mesma partição do request.
 */
@Component
@Profile("responder")
public class Responder {

    private final KafkaTemplate<String, String> kafka;
    private final SimProperties props;
    private final ScheduledExecutorService scheduler;

    public Responder(KafkaTemplate<String, String> kafka, SimProperties props) {
        this.kafka = kafka;
        this.props = props;
        this.scheduler = Executors.newScheduledThreadPool(props.getResponder().getSchedulerThreads());
    }

    @KafkaListener(id = "responder", topics = "${sim.topics.request}", groupId = "sim-responder")
    public void onRequest(ConsumerRecord<String, String> record) {
        long delay = Latency.sample(props.getLatency().getIntegration());
        scheduler.schedule(
                () -> kafka.send(props.getTopics().getResponse(), record.key(), record.value()),
                delay, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    void stop() {
        scheduler.shutdown();
    }
}
