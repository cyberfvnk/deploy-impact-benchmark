package com.bench.sim.flow;

import java.util.concurrent.ThreadLocalRandom;

import com.bench.sim.config.SimProperties.LatencyProfile;

/** Mock das dependências externas (API, Dynamo, integração): só consome tempo. */
public final class Latency {

    private Latency() {
    }

    /** Sorteia uma latência: base + aleatório(0..jitter) + cauda lenta ocasional. */
    public static long sample(LatencyProfile p) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long ms = p.getBaseMs() + (p.getJitterMs() > 0 ? rnd.nextLong(p.getJitterMs() + 1) : 0);
        if (p.getSlowRate() > 0 && rnd.nextDouble() < p.getSlowRate()) {
            ms += p.getSlowMs();
        }
        return ms;
    }

    /** Bloqueia a thread pela latência sorteada (como uma chamada síncrona real). */
    public static void sleep(LatencyProfile p) throws InterruptedException {
        long ms = sample(p);
        if (ms > 0) {
            Thread.sleep(ms);
        }
    }
}
