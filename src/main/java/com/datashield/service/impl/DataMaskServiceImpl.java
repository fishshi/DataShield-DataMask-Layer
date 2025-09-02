package com.datashield.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.datashield.entity.Task;
import com.datashield.mapper.TaskMapper;
import com.datashield.service.DataMaskService;
import com.datashield.util.VirtualThreadPoolUtil;

@Service
public class DataMaskServiceImpl implements DataMaskService {
    @Autowired
    private TaskMapper taskMapper;

    @Override
    public void startTask(Long taskId) {
        Runnable runnable = () -> {
            Task task = taskMapper.selectById(taskId);
            if (task.getIsRemote() == 0) {
                maskLocalData(task);
            } else {
                maskRemoteData(task);
            }
        };
        VirtualThreadPoolUtil.submit(runnable);
    }

    @Override
    public void maskRemoteData(Task task) {
    }

    @Override
    public void maskLocalData(Task task) {
    }
}
