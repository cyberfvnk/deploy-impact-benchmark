package com.bench.sim.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Todos os parâmetros "sim.*" do application.yml.
 */
@Component
@ConfigurationProperties(prefix = "sim")
public class SimProperties {

    public enum ShutdownMode { DEFAULT, GRACEFUL }

    private String taskId;
    private ShutdownMode shutdownMode = ShutdownMode.DEFAULT;
    private Duration slaTimeout = Duration.ofSeconds(26);
    private Duration gracefulTimeout = Duration.ofSeconds(25);
    private Sqs sqs = new Sqs();
    private Topics topics = new Topics();
    private Latencies latency = new Latencies();
    private Responder responder = new Responder();

    public static class Sqs {
        private String endpoint;
        private String region;
        private String inputQueue;
        private String finalQueue;
        private int pollerThreads = 2;
        private int workerThreads = 20;
        private int maxMessages = 10;
        private int waitSeconds = 20;
        /** true = só faz novo poll quando o lote anterior inteiro terminou (Spring Cloud AWS 2.x). */
        private boolean batchWait = true;
        /** Quanto o stop DEFAULT espera as msgs em voo (queueStopTimeout do Spring Cloud AWS 2.x). */
        private Duration queueStopTimeout = Duration.ofSeconds(20);

        public boolean isBatchWait() { return batchWait; }
        public void setBatchWait(boolean batchWait) { this.batchWait = batchWait; }
        public Duration getQueueStopTimeout() { return queueStopTimeout; }
        public void setQueueStopTimeout(Duration queueStopTimeout) { this.queueStopTimeout = queueStopTimeout; }
        public String getEndpoint() { return endpoint; }
        public void setEndpoint(String endpoint) { this.endpoint = endpoint; }
        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }
        public String getInputQueue() { return inputQueue; }
        public void setInputQueue(String inputQueue) { this.inputQueue = inputQueue; }
        public String getFinalQueue() { return finalQueue; }
        public void setFinalQueue(String finalQueue) { this.finalQueue = finalQueue; }
        public int getPollerThreads() { return pollerThreads; }
        public void setPollerThreads(int pollerThreads) { this.pollerThreads = pollerThreads; }
        public int getWorkerThreads() { return workerThreads; }
        public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
        public int getMaxMessages() { return maxMessages; }
        public void setMaxMessages(int maxMessages) { this.maxMessages = maxMessages; }
        public int getWaitSeconds() { return waitSeconds; }
        public void setWaitSeconds(int waitSeconds) { this.waitSeconds = waitSeconds; }
    }

    public static class Topics {
        private String request;
        private String response;

        public String getRequest() { return request; }
        public void setRequest(String request) { this.request = request; }
        public String getResponse() { return response; }
        public void setResponse(String response) { this.response = response; }
    }

    /** Latência simulada: base + aleatório(0..jitter) + slowMs com probabilidade slowRate (cauda/p99). */
    public static class LatencyProfile {
        private long baseMs;
        private long jitterMs;
        private double slowRate;
        private long slowMs;

        public long getBaseMs() { return baseMs; }
        public void setBaseMs(long baseMs) { this.baseMs = baseMs; }
        public long getJitterMs() { return jitterMs; }
        public void setJitterMs(long jitterMs) { this.jitterMs = jitterMs; }
        public double getSlowRate() { return slowRate; }
        public void setSlowRate(double slowRate) { this.slowRate = slowRate; }
        public long getSlowMs() { return slowMs; }
        public void setSlowMs(long slowMs) { this.slowMs = slowMs; }
    }

    public static class Latencies {
        private LatencyProfile api = new LatencyProfile();
        private LatencyProfile dynamoWrite = new LatencyProfile();
        private LatencyProfile dynamoRead = new LatencyProfile();
        private LatencyProfile integration = new LatencyProfile();

        public LatencyProfile getApi() { return api; }
        public void setApi(LatencyProfile api) { this.api = api; }
        public LatencyProfile getDynamoWrite() { return dynamoWrite; }
        public void setDynamoWrite(LatencyProfile dynamoWrite) { this.dynamoWrite = dynamoWrite; }
        public LatencyProfile getDynamoRead() { return dynamoRead; }
        public void setDynamoRead(LatencyProfile dynamoRead) { this.dynamoRead = dynamoRead; }
        public LatencyProfile getIntegration() { return integration; }
        public void setIntegration(LatencyProfile integration) { this.integration = integration; }
    }

    public static class Responder {
        private int schedulerThreads = 4;

        public int getSchedulerThreads() { return schedulerThreads; }
        public void setSchedulerThreads(int schedulerThreads) { this.schedulerThreads = schedulerThreads; }
    }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public ShutdownMode getShutdownMode() { return shutdownMode; }
    public void setShutdownMode(ShutdownMode shutdownMode) { this.shutdownMode = shutdownMode; }
    public Duration getSlaTimeout() { return slaTimeout; }
    public void setSlaTimeout(Duration slaTimeout) { this.slaTimeout = slaTimeout; }
    public Duration getGracefulTimeout() { return gracefulTimeout; }
    public void setGracefulTimeout(Duration gracefulTimeout) { this.gracefulTimeout = gracefulTimeout; }
    public Sqs getSqs() { return sqs; }
    public void setSqs(Sqs sqs) { this.sqs = sqs; }
    public Topics getTopics() { return topics; }
    public void setTopics(Topics topics) { this.topics = topics; }
    public Latencies getLatency() { return latency; }
    public void setLatency(Latencies latency) { this.latency = latency; }
    public Responder getResponder() { return responder; }
    public void setResponder(Responder responder) { this.responder = responder; }
}
