package com.shubh.ecommerce.inventory_service.config;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Value("${kafka.topic.OrderCreatedItemsTopicName}")
    private String orderCreatedItemsTopicName;

    @Value("${kafka.topic.OrderConfirmedTopicName}")
    private String orderConfirmedTopicName;

    @Bean
    public NewTopic orderCreatedItemTopic() {
        return new NewTopic(orderCreatedItemsTopicName, 3, (short) 1);
    }

    @Bean
    public NewTopic orderConfirmedTopic() {
        return new NewTopic(orderConfirmedTopicName, 3, (short) 1);
    }

}
