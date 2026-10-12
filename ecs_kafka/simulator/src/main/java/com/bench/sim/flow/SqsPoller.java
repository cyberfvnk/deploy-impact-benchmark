package com.bench.sim.flow;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.bench.sim.config.SimProperties;
import com.bench.sim.metrics.Events;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * Consumidor da fila SQS de entrada + o ponto central do GRACEFUL SHUTDOWN.
 *
 * Ordem de parada do Spring: quanto MAIOR a phase, mais CEDO o stop() é chamado.
 *   PHASE (este)                 = Integer.MAX_VALUE - 10   -> para primeiro
 *   containers @KafkaListener    = Integer.MAX_VALUE - 100  -> param depois
 * Assim, no graceful, paramos de ler o SQS e terminamos o que está em voo
 * ENQUANTO o consumer de respostas Kafka ainda está vivo.
 */
@Component
@Profile("!responder")
public class SqsPoller implements SmartLifecycle {

    static final int PHASE = Integer.MAX_VALUE - 10;

    private final SqsClient sqs;
    private final SqsGateway gateway;
    private final Processor processor;
    private final SimProperties props;
    private final Events events;

    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile boolean running;
    private ExecutorService pollers;
    private ExecutorService workers;
    /** Limita msgs recebidas e ainda não concluídas ao nº de workers (backpressure). */
    private Semaphore capacity;

    public SqsPoller(SqsClient sqs, SqsGateway gateway, Processor processor, SimProperties props, Events events) {
        this.sqs = sqs;
        this.gateway = gateway;
        this.processor = processor;
        this.props = props;
        this.events = events;
    }

    @Override
    public void start() {
        try {
            gateway.resolveQueues();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        SimProperties.Sqs cfg = props.getSqs();
        capacity = new Semaphore(cfg.getWorkerThreads());
        workers = Executors.newFixedThreadPool(cfg.getWorkerThreads());
        pollers = Executors.newFixedThreadPool(cfg.getPollerThreads());
        running = true;
        for (int i = 0; i < cfg.getPollerThreads(); i++) {
            pollers.execute(this::pollLoop);
        }
    }

    private void pollLoop() {
        SimProperties.Sqs cfg = props.getSqs();
        while (running) {
            int permits = 0;
            try {
                // Só pede ao SQS quantas msgs há worker livre para processar.
                capacity.acquire();
                permits = 1;
                while (permits < cfg.getMaxMessages() && capacity.tryAcquire()) {
                    permits++;
                }
                List<Message> msgs = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(gateway.inputUrl())
                        .maxNumberOfMessages(permits)
                        .waitTimeSeconds(cfg.getWaitSeconds())
                        .messageSystemAttributeNames(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)
                        .build()).messages();
                capacity.release(permits - msgs.size());
                permits = 0;
                CountDownLatch batch = new CountDownLatch(msgs.size());
                for (Message m : msgs) {
                    inFlight.incrementAndGet();
                    workers.execute(() -> {
                        try {
                            processor.process(m);
                        } finally {
                            inFlight.decrementAndGet();
                            capacity.release();
                            batch.countDown();
                        }
                    });
                }
                // Spring Cloud AWS 2.x: o próximo poll só sai quando o lote inteiro terminou
                // (uma msg lenta segura as outras 9 vagas).
                if (cfg.isBatchWait()) {
                    batch.await();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                capacity.release(permits);
                if (running) {
                    events.emit("SQS_POLL_ERROR", null, "error=" + e.getClass().getSimpleName());
                    sleepQuietly(1000);
                }
            }
        }
    }

    /** Chamado pelo Spring ao receber SIGTERM (via shutdown hook da JVM). */
    @Override
    public void stop() {
        events.emit("SHUTDOWN_BEGIN", null, "mode=" + props.getShutdownMode()
                + " inFlight=" + inFlight.get());
        if (props.getShutdownMode() == SimProperties.ShutdownMode.GRACEFUL) {
            stopGracefully();
        } else {
            stopDefault();
        }
    }

    /**
     * Comportamento PADRÃO do Spring Cloud AWS 2.x (sem nenhuma config de graceful),
     * que só acontece se o Java RECEBER o SIGTERM (entrypoint exec):
     *   para de fazer poll, espera as msgs em voo até queueStopTimeout (20s) e
     *   interrompe o que sobrar. Msgs interrompidas não foram apagadas do SQS
     *   -> voltam só após o visibility timeout.
     * Depois o Spring fecha os consumers Kafka normalmente (envia LeaveGroup).
     */
    private void stopDefault() {
        running = false;
        try {
            pollers.shutdownNow(); // interrompe o long poll
            workers.shutdown();
            workers.awaitTermination(props.getSqs().getQueueStopTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            int abandoned = inFlight.get();
            workers.shutdownNow();
            events.emit("SHUTDOWN_DONE", null, "abandonedInFlight=" + abandoned);
        }
    }

    /**
     * App COM graceful, dentro de um orçamento de tempo (graceful-timeout):
     *   1. para de pedir msgs novas ao SQS
     *   2. espera os workers terminarem o que está em voo
     *   3. retorna -> Spring segue e fecha os consumers Kafka (LeaveGroup imediato)
     */
    private void stopGracefully() {
        long deadline = System.currentTimeMillis() + props.getGracefulTimeout().toMillis();
        running = false;
        try {
            // Pollers podem estar presos num long poll (até wait-seconds).
            // As msgs recebidas nesse intervalo ainda são entregues aos workers.
            pollers.shutdown();
            pollers.awaitTermination(remaining(deadline), TimeUnit.MILLISECONDS);
            workers.shutdown();
            workers.awaitTermination(remaining(deadline), TimeUnit.MILLISECONDS);
            events.emit("SHUTDOWN_DONE", null, "inFlightLeft=" + inFlight.get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            pollers.shutdownNow();
            workers.shutdownNow();
        }
    }

    private static long remaining(long deadline) {
        return Math.max(0, deadline - System.currentTimeMillis());
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
