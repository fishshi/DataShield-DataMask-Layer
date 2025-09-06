package com.datashield.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.datashield.service.DataMaskService;

/**
 * Kafka 组件类
 */
@Component
public class KafkaComponent {
    @Autowired
    private DataMaskService dataMaskService;

    @KafkaListener(topics = "task-topic")
    public void onTaskMessage(String payload) {
        dataMaskService.startTask(Long.parseLong(payload));
    }

    @KafkaListener(topics = "identify-topic")
    public void onIdentifyMessage(String payload) {
        dataMaskService.startIdentify(Long.parseLong(payload));
    }
}