package com.bench.sim.flow;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.bench.sim.config.SimProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.ReceiptHandleIsInvalidException;

/** Operações SQS usadas pelo fluxo: apagar da fila de entrada e publicar na fila final. */
@Component
@Profile("!responder")
public class SqsGateway {

    private final SqsClient sqs;
    private final ObjectMapper mapper;
    private final SimProperties props;
    private volatile String inputUrl;
    private volatile String finalUrl;

    public SqsGateway(SqsClient sqs, ObjectMapper mapper, SimProperties props) {
        this.sqs = sqs;
        this.mapper = mapper;
        this.props = props;
    }

    /** Resolve as URLs das filas, tentando por até ~60s (o ElasticMQ pode subir depois da app). */
    public void resolveQueues() throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            try {
                inputUrl = url(props.getSqs().getInputQueue());
                finalUrl = url(props.getSqs().getFinalQueue());
                return;
            } catch (RuntimeException e) {
                if (attempt >= 60) {
                    throw e;
                }
                Thread.sleep(1000);
            }
        }
    }

    public String inputUrl() {
        return inputUrl;
    }

    public void deleteInput(String receiptHandle) {
        sqs.deleteMessage(b -> b.queueUrl(inputUrl).receiptHandle(receiptHandle));
    }

    /**
     * Ack que não derruba o fluxo. Falha esperada: a msg já foi reentregue pelo SQS
     * (visibility expirou) e o receipt handle antigo deixou de valer.
     */
    public boolean tryDeleteInput(String receiptHandle) {
        try {
            deleteInput(receiptHandle);
            return true;
        } catch (ReceiptHandleIsInvalidException e) {
            return false;
        }
    }

    public void publishFinal(Envelope env) throws JsonProcessingException {
        String body = mapper.writeValueAsString(env);
        sqs.sendMessage(b -> b.queueUrl(finalUrl).messageBody(body));
    }

    private String url(String queue) {
        return sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queue).build()).queueUrl();
    }
}
