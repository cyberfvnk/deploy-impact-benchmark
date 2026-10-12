package com.bench.sim.config;

import java.net.URI;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

/** Cliente SQS apontando para o ElasticMQ (credenciais fake, o ElasticMQ não valida). */
@Configuration
@Profile("!responder")
public class SqsConfig {

    @Bean(destroyMethod = "close")
    public SqsClient sqsClient(SimProperties props) {
        return SqsClient.builder()
                .endpointOverride(URI.create(props.getSqs().getEndpoint()))
                .region(Region.of(props.getSqs().getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
                .build();
    }
}
