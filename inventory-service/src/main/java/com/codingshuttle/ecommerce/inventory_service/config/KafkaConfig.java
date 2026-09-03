package com.codingshuttle.ecommerce.inventory_service.config;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Value("${kafka.topic.OrderCreatedItemsTopicName}")
    private String orderCreatedItemsTopicName;

    @Bean
    public NewTopic orderCreatedItemTopic() {
        // 3 partitions allow multiple consumers to process messages in parallel,
        // increasing the potential throughput of the topic.
        // Replication factor is 1 because our setup has only a single Kafka broker.
        return new NewTopic(orderCreatedItemsTopicName, 3, (short) 1);
    }

}
