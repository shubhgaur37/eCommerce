package com.shubh.ecommerce.order_service.service;

import com.shubh.ecommerce.events.OrderConfirmedEvent;
import com.shubh.ecommerce.order_service.clients.InventoryOpenFeignClient;
import com.shubh.ecommerce.order_service.dto.OrderRequestDto;
import com.shubh.ecommerce.order_service.entity.OrderItem;
import com.shubh.ecommerce.order_service.entity.OrderStatus;
import com.shubh.ecommerce.order_service.entity.Orders;
import com.shubh.ecommerce.order_service.repoitory.OrdersRepository;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.modelmapper.ModelMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrdersService {

    private final OrdersRepository orderRepository;
    private final ModelMapper modelMapper;
    private final InventoryOpenFeignClient inventoryOpenFeignClient;

    public List<OrderRequestDto> getAllOrders() {
        log.info("Fetching all orders");
        List<Orders> orders = orderRepository.findAll();
        return orders.stream().map(order -> modelMapper.map(order, OrderRequestDto.class)).toList();
    }

    public OrderRequestDto getOrderById(Long id) {
        log.info("Fetching order with ID: {}", id);
        Orders order = orderRepository.findById(id).orElseThrow(() -> new RuntimeException("Order not found"));
        return modelMapper.map(order, OrderRequestDto.class);
    }

    @CircuitBreaker(name = "inventoryCircuitBreaker", fallbackMethod = "createOrderFallback")
    public OrderRequestDto createOrder(OrderRequestDto orderRequestDto) {
        log.info("Calling the createOrder method");
        Double totalPrice = reserveInventory(orderRequestDto);

        Orders orders = modelMapper.map(orderRequestDto, Orders.class);
        for(OrderItem orderItem: orders.getItems()) {
            orderItem.setOrder(orders);
        }
        orders.setTotalPrice(totalPrice);
        orders.setOrderStatus(OrderStatus.CONFIRMED);

        Orders savedOrder = orderRepository.save(orders);

        return modelMapper.map(savedOrder, OrderRequestDto.class);
    }

    public Double reserveInventory(OrderRequestDto orderRequestDto){
        log.info("Making a synchronous call to inventory service reduce-stocks method to reserve inventory");
        return inventoryOpenFeignClient.reduceStocks(orderRequestDto);
    }

    @KafkaListener(topics = {"${kafka.topic.OrderConfirmedTopic}"}, groupId = "${kafka.consumer.order_creation.group.id}")
    public void createOrderFromInventoryReductionEvent(OrderConfirmedEvent orderConfirmedEvent){
        Orders orders = modelMapper.map(orderConfirmedEvent, Orders.class);
        for(OrderItem orderItem: orders.getItems()) {
            orderItem.setId(null);

            orderItem.setOrder(orders);
        }
        orders.setTotalPrice(orderConfirmedEvent.getTotalPrice());
        orders.setOrderStatus(OrderStatus.CONFIRMED);

        orderRepository.save(orders);
        log.info("Order Created From Event: {}",orderConfirmedEvent);
    }

    public OrderRequestDto createOrderFallback(OrderRequestDto orderRequestDto, Throwable throwable) {
        log.error("Fallback occurred due to : {}", throwable.getMessage());

        return new OrderRequestDto();
    }

    @KafkaListener(
            topics = {"${kafka.topic.OrderCreatedItemsTopic}"},
            groupId = "order-service-logger"
    )
    public void logOrderCreatedItemsMessage(String message) {
        log.info("OrderCreated Logger Message: " + message);
    }

    @KafkaListener(topics = {"${kafka.topic.OrderCreatedItemsTopic}"})
    public void printConsoleOrderCreatedItemsMessage(ConsumerRecord<String, String> record) {
        System.out.println("OrderCreated Console Consumer Message: "+ record.value());

        record.headers().forEach(header ->
                log.info("KAFKA Header: {} = {}", header.key(), new String(header.value()))
        );

    }
}










