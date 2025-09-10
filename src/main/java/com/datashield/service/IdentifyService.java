package com.datashield.service;

import com.datashield.entity.Identify;

/**
 * 敏感数据识别服务接口
 */
public interface IdentifyService {
    /**
     * 启动敏感数据识别任务
     *
     * @param identifyId 识别 ID
     */
    void startIdentify(Long identifyId);

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
