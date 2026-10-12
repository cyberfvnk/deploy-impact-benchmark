package com.bench.sim;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Simulador da aplicação de produção. Um único jar com dois papéis (Spring profile):
 *
 * <pre>
 *  [task]       SQS input -> api+dynamo (mock) -> Kafka request
 *               Kafka response -> dynamo (mock) -> SQS final
 *  [responder]  Kafka request -> latência da integração -> Kafka response
 * </pre>
 *
 * O "responder" faz o papel da integração externa e nunca é redeployado.
 */
@SpringBootApplication
public class SimApplication {

    public static void main(String[] args) {
        SpringApplication.run(SimApplication.class, args);
    }
}
