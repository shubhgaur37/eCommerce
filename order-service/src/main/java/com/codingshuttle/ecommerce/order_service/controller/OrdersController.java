package com.codingshuttle.ecommerce.order_service.controller;

import com.codingshuttle.ecommerce.order_service.config.FeaturesEnableConfig;
import com.codingshuttle.ecommerce.order_service.dto.OrderRequestDto;
import com.codingshuttle.ecommerce.order_service.service.OrdersService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/core")
@RequiredArgsConstructor
@Slf4j
// @RefreshScope is not required here because FeaturesEnableConfig is already
// refresh-scoped. The controller uses that bean to access the latest
// configuration values after /actuator/refresh.
public class OrdersController {

    private final OrdersService orderService;

    @Value("${my.variable}")
    private String myVariable;

    private final FeaturesEnableConfig featuresEnableConfig;

    @GetMapping("/helloOrders")
    public String helloOrders() {

        if (featuresEnableConfig.isEventDrivenOrderFlowEnabled()) {
            return "Event Driven enabled wohoo, my variable is: "+ myVariable;
        } else {
           return "EventDriven disabled awww, my variable is: "+ myVariable;
        }
    }

    @PostMapping("/create-order")
    public ResponseEntity<OrderRequestDto> createOrder(@RequestBody OrderRequestDto orderRequestDto) {
        OrderRequestDto orderResponse;

        if (featuresEnableConfig.isEventDrivenOrderFlowEnabled()) {
            orderService.reserveInventory(orderRequestDto);
        } else {
            orderResponse = orderService.createOrder(orderRequestDto);
            return ResponseEntity.ok(orderResponse);
        }
        return ResponseEntity.ok(null);
    }

    @GetMapping
    public ResponseEntity<List<OrderRequestDto>> getAllOrders(HttpServletRequest httpServletRequest) {
        log.info("Fetching all orders via controller");
        List<OrderRequestDto> orders = orderService.getAllOrders();
        return ResponseEntity.ok(orders);  // Returns 200 OK with the list of orders
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderRequestDto> getOrderById(@PathVariable Long id) {
        log.info("Fetching order with ID: {} via controller", id);
        OrderRequestDto order = orderService.getOrderById(id);
        return ResponseEntity.ok(order);  // Returns 200 OK with the order
    }
}
