package com.datashield.service;

import com.datashield.entity.Task;

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

    /**
     * 执行远程数据脱敏任务
     *
     * @param task 脱敏任务
     */
    void maskRemoteData(Task task);

    /**
     * 执行本地数据脱敏任务
     *
     * @param task 脱敏任务
     */
    void maskLocalData(Task task);
}
