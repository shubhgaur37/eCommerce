package com.shubh.ecommerce.order_service.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Configuration;

@Configuration
@RefreshScope
@Data
public class FeaturesEnableConfig {

    @Value("${features.event_driven_order_flow.enabled}")
    private boolean eventDrivenOrderFlowEnabled;

}
