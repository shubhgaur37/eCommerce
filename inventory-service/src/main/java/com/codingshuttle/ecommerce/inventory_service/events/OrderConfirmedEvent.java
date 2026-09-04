package com.codingshuttle.ecommerce.inventory_service.events;

import com.codingshuttle.ecommerce.inventory_service.dto.OrderRequestItemDto;
import lombok.Data;

import java.util.List;

@Data
public class OrderConfirmedEvent{
    private Long id;
    List<OrderRequestItemDto> items;
    Double totalPrice;
}
