package com.shubh.ecommerce.inventory_service.service;

import com.shubh.ecommerce.events.OrderConfirmedEvent;
import com.shubh.ecommerce.inventory_service.dto.OrderRequestDto;
import com.shubh.ecommerce.inventory_service.dto.OrderRequestItemDto;
import com.shubh.ecommerce.inventory_service.dto.ProductDto;
import com.shubh.ecommerce.inventory_service.entity.Product;
import com.shubh.ecommerce.inventory_service.repository.ProductRepository;
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
import java.util.stream.IntStream;

@Service
@Slf4j
@RequiredArgsConstructor
@RefreshScope
public class ProductService {

    private final ProductRepository productRepository;
    private final ModelMapper modelMapper;

    @Value("${features.event_driven_order_flow.enabled}")
    private boolean eventDrivenFlowEnabled;

    private final KafkaTemplate<String,String> kafkaTemplate;
    private final KafkaTemplate<Long, OrderConfirmedEvent> kafkaTemplateOrderConfirmed;

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

            IntStream.range(0, productNames.size())
                    .forEach(i -> orderConfirmedEvent.getItems().get(i).setName(productNames.get(i)));

            orderConfirmedEvent.setTotalPrice(totalPrice);
            sendOrderConfirmedMessage(orderConfirmedEvent);
        }

        sendOrderSuccessfulMessage(productNames);
        return totalPrice;
    }

    private void sendOrderConfirmedMessage(OrderConfirmedEvent orderConfirmedEvent) {

        kafkaTemplateOrderConfirmed.send(orderConfirmedTopicName,orderConfirmedEvent);
    }

    private void sendOrderSuccessfulMessage(List<String> productNames) {

        kafkaTemplate.send(
                orderCreatedItemsTopicName,
                "Order Created for items: " + productNames
        );
    }

}













