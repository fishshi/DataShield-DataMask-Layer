package com.datashield.service;

/**
 * 数据脱敏服务接口
 */
public interface DataMaskService {
    /**
     * 启动数据脱敏任务
     *
     * @param taskId 任务 ID
     */
    void startTask(Long taskId);
}
