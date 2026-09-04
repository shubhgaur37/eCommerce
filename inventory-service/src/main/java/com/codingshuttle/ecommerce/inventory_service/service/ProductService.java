package com.codingshuttle.ecommerce.inventory_service.service;

import com.codingshuttle.ecommerce.inventory_service.dto.OrderRequestDto;
import com.codingshuttle.ecommerce.inventory_service.dto.OrderRequestItemDto;
import com.codingshuttle.ecommerce.inventory_service.dto.ProductDto;
import com.codingshuttle.ecommerce.inventory_service.entity.Product;
import com.codingshuttle.ecommerce.inventory_service.events.OrderConfirmedEvent;
import com.codingshuttle.ecommerce.inventory_service.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
@RefreshScope
public class ProductService {

    private final ProductRepository productRepository;
    private final ModelMapper modelMapper;

    @Value("${features.event_driven_order_flow.enabled}")
    private boolean eventDrivenFlowEnabled;

    // key and value type for the message
    private final KafkaTemplate<String,String> kafkaTemplate;
    private final KafkaTemplate<Long,OrderConfirmedEvent> kafkaTemplateOrderConfirmed;

    // kafka topic to publish message
    @Value("${kafka.topic.OrderCreatedItemsTopicName}")
    private String orderCreatedItemsTopicName;

    @Value("${kafka.topic.OrderConfirmedTopicName}")
    private String orderConfirmedTopicName;

    public List<ProductDto> getAllInventory() {
        log.info("Fetching all inventory items");
        List<Product> inventories = productRepository.findAll();
        return inventories.stream()
                .map(product -> modelMapper.map(product, ProductDto.class))
                .toList();
    }

    public ProductDto getProductById(Long id) {
        log.info("Fetching Product with ID: {}", id);
        Optional<Product> inventory = productRepository.findById(id);
        return inventory.map(item -> modelMapper.map(item, ProductDto.class))
                .orElseThrow(() -> new RuntimeException("Inventory not found"));
    }

    @Transactional
    public Double reduceStocks(OrderRequestDto orderRequestDto) {
        log.info("Reducing the stocks");
        Double totalPrice = 0.0;
        List<String> productNames = new ArrayList<>();
        for(OrderRequestItemDto orderRequestItemDto: orderRequestDto.getItems()) {
            Long productId = orderRequestItemDto.getProductId();
            Integer quantity = orderRequestItemDto.getQuantity();

            Product product = productRepository.findById(productId).orElseThrow(() ->
                    new RuntimeException("Product not found with id: "+productId));
            // for kafka message
            productNames.add(product.getName());
            if(product.getStock() < quantity) {
                throw new RuntimeException("Product cannot be fulfilled for given quantity");
            }

            product.setStock(product.getStock()-quantity);
            productRepository.save(product);
            totalPrice += quantity*product.getPrice();
        }
        if (eventDrivenFlowEnabled){
            OrderConfirmedEvent orderConfirmedEvent = modelMapper.map(orderRequestDto, OrderConfirmedEvent.class);
            orderConfirmedEvent.setTotalPrice(totalPrice);
            sendOrderConfirmedMessage(orderConfirmedEvent);
        }

        // Observation: While debugging, the Kafka message was not visible to the consumer
        // until this @Transactional method completed and returned.
        // KafkaTemplate.send() is asynchronous, so calling send() does not mean the message
        // has already been successfully published and acknowledged by Kafka. The send returns
        // a CompletableFuture and the actual Kafka operation may complete instantly or later.
        // Therefore, this observation alone does not prove that Kafka publishing was deferred
        // by the DB transaction or that the DB and Kafka operations are atomic.
        // If Kafka fails asynchronously after the DB transaction commits, the stock reduction
        // may remain committed while the OrderConfirmed event is never published.
        // The Outbox Pattern avoids this inconsistency by saving the business change and the
        // event in the same DB transaction and publishing the event to Kafka separately.
        sendOrderSuccessfulMessage(productNames);
        return totalPrice;
    }

    // Kafka Demo Order Created Message
    private void sendOrderConfirmedMessage(OrderConfirmedEvent orderConfirmedEvent) {

        // Kafka can auto-create the topic if it does not already exist,
        // but relying on auto-creation is error-prone and not recommended
        // for production. Topics should be created and configured explicitly.
        kafkaTemplateOrderConfirmed.send(orderConfirmedTopicName,orderConfirmedEvent);
    }

    // Kafka Demo Order Created Message
    private void sendOrderSuccessfulMessage(List<String> productNames) {

        // Kafka can auto-create the topic if it does not already exist,
        // but relying on auto-creation is error-prone and not recommended
        // for production. Topics should be created and configured explicitly.
        kafkaTemplate.send(
                orderCreatedItemsTopicName,
                "Order Created for items: " + productNames
        );
    }

}













