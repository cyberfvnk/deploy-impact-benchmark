package com.bench.sim.config;

import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

import com.bench.sim.metrics.Events;

/**
 * Container factory padrão do Boot + um listener de rebalance que registra
 * cada revoke/assign. É assim que medimos a diferença entre Range (eager:
 * revoga TODAS as partições) e CooperativeSticky (revoga só as que mudam de dono).
 */
@Configuration
public class KafkaConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> consumerFactory,
            KafkaProperties kafkaProperties,
            Events events) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        // Aplica tudo de spring.kafka.* (concurrency, ack mode, etc.), como o Boot faria.
        configurer.configure(factory, consumerFactory.getIfAvailable(
                () -> new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties())));
        factory.getContainerProperties().setConsumerRebalanceListener(new RebalanceLogger(events));
        return factory;
    }

    static class RebalanceLogger implements ConsumerAwareRebalanceListener {

        private final Events events;

        RebalanceLogger(Events events) {
            this.events = events;
        }

        @Override
        public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            events.emit("REBALANCE_REVOKED", null, describe(consumer, partitions));
        }

        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            events.emit("REBALANCE_ASSIGNED", null, describe(consumer, partitions));
        }

        /** Partições perdidas sem revoke limpo (ex.: sessão expirou). */
        @Override
        public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            events.emit("REBALANCE_LOST", null, describe(consumer, partitions));
        }

        private static String describe(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            String list = partitions.stream().map(TopicPartition::toString).sorted().collect(Collectors.joining(","));
            return "group=" + consumer.groupMetadata().groupId() + " count=" + partitions.size() + " partitions=[" + list + "]";
        }
    }
}
