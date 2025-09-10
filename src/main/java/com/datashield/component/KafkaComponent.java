package com.datashield.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.datashield.service.DataMaskService;
import com.datashield.service.IdentifyService;

/**
 * Kafka 组件类
 */
@Component
public class KafkaComponent {
    @Autowired
    private DataMaskService dataMaskService;
    @Autowired
    private IdentifyService identifyService;

    /**
     * 监听 task-topic 主题
     */
    @KafkaListener(topics = "task-topic")
    public void onTaskMessage(String payload) {
        dataMaskService.startTask(Long.parseLong(payload));
    }

    /**
     * 监听 identify-topic 主题
     */
    @KafkaListener(topics = "identify-topic")
    public void onIdentifyMessage(String payload) {
        identifyService.startIdentify(Long.parseLong(payload));
    }
}