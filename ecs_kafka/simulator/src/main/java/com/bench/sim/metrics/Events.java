package com.bench.sim.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.bench.sim.config.SimProperties;

/**
 * Toda métrica sai como uma linha de log "chave=valor" no logger EVT:
 *
 *   EVT - type=COMPLETED task=t1 id=abc ts=1700000000000 e2eMs=812 slaBreached=false
 *
 * Por que log e não Prometheus: custo zero de RAM e o script de análise
 * reconstrói tudo depois (latência, rebalances, perdas) a partir do `docker logs`.
 */
@Component
public class Events {

    private static final Logger EVT = LoggerFactory.getLogger("EVT");

    private final String taskId;
    private final SimProperties props;

    public Events(SimProperties props) {
        this.props = props;
        this.taskId = props.getTaskId();
    }

    public void emit(String type, String id, String extra) {
        EVT.info("type={} task={} id={} ts={} {}", type, taskId, id == null ? "-" : id,
                System.currentTimeMillis(), extra == null ? "" : extra);
    }

    /** Marca o início da task e registra a configuração do cenário no próprio log. */
    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        emit("READY", null, "shutdownMode=" + props.getShutdownMode());
    }
}
