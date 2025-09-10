package com.datashield.service.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.datashield.entity.Identify;
import com.datashield.entity.UserRemoteDatabase;
import com.datashield.enums.TaskStatusEnum;
import com.datashield.mapper.IdentifyMapper;
import com.datashield.mapper.RemoteDataMapper;
import com.datashield.service.IdentifyService;
import com.datashield.util.UserSqlConnectionUtil;
import com.datashield.util.VirtualThreadPoolUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * 敏感数据识别服务实现类
 */
@Slf4j
@Service
public class IdentifyServiceImpl implements IdentifyService {
    @Autowired
    private IdentifyMapper identifyMapper;
    @Autowired
    private RemoteDataMapper remoteDataMapper;

    @Override
    public void startIdentify(Long identifyId) {
        Runnable runnable = () -> {
            Identify identify = identifyMapper.selectById(identifyId);
            if (identify.getIsRemote() == 0) {
                identifyLocalData(identify);
            } else {
                identifyRemoteData(identify);
            }
        };
        VirtualThreadPoolUtil.submit(runnable);
    }

    @Override
    public void identifyRemoteData(Identify identify) {
        identify.setStatus(TaskStatusEnum.RUNNING.getCode());
        identifyMapper.updateById(identify);
        UserRemoteDatabase remoteDatabase = remoteDataMapper.selectOne(new QueryWrapper<UserRemoteDatabase>()
                .eq("user_id", identify.getUserId()).eq("db_name", identify.getDbName()));
        if (remoteDatabase == null) {
            identify.setStatus(TaskStatusEnum.ERROR.getCode());
            identifyMapper.updateById(identify);
            return;
        }
        try (Connection conn = UserSqlConnectionUtil.getConnection(remoteDatabase)) {
            String columns = "";
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getColumns(identify.getDbName(), null, identify.getTbName(), "%")) {
                while (rs.next()) {
                    columns += ("," + rs.getString("COLUMN_NAME"));
                }
            }
            columns = columns.substring(1);
            identify.setColumns(columns);
            identify.setStatus(TaskStatusEnum.DONE.getCode());
            identifyMapper.updateById(identify);
        } catch (Exception e) {
            identify.setStatus(TaskStatusEnum.ERROR.getCode());
            identifyMapper.updateById(identify);
        }
    }

    @Override
    public void identifyLocalData(Identify identify) {
        identify.setStatus(TaskStatusEnum.RUNNING.getCode());
        identifyMapper.updateById(identify);

        String fullDbName = identify.getUserId() + "_" + identify.getDbName();
        try (Connection conn = UserSqlConnectionUtil.getConnection(fullDbName)) {
            String columns = "";
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getColumns(fullDbName, null, identify.getTbName(), "%")) {
                while (rs.next()) {
                    columns += ("," + rs.getString("COLUMN_NAME"));
                }
            }
            columns = columns.substring(1);
            identify.setColumns(columns);
            identify.setStatus(TaskStatusEnum.DONE.getCode());
            identifyMapper.updateById(identify);

        } catch (Exception e) {
            identify.setStatus(TaskStatusEnum.ERROR.getCode());
            identifyMapper.updateById(identify);
        }
    }
}
