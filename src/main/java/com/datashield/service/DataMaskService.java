package com.datashield.service;

import com.datashield.entity.Identify;
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
     * 启动敏感数据识别任务
     *
     * @param identifyId 识别 ID
     */
    void startIdentify(Long identifyId);

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

    /**
     * 执行远程数据识别任务
     *
     * @param identify 识别任务
     */
    void identifyRemoteData(Identify identify);

    /**
     * 执行本地数据识别任务
     *
     * @param identify 识别任务
     */
    void identifyLocalData(Identify identify);
}
