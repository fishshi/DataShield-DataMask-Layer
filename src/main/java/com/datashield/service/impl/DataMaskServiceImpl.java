package com.datashield.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.datashield.entity.Task;
import com.datashield.enums.DataMaskRuleEnum;
import com.datashield.enums.TaskStatusEnum;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.datashield.entity.UserRemoteDatabase;
import com.datashield.mapper.RemoteDataMapper;
import com.datashield.mapper.TaskMapper;
import com.datashield.service.DataMaskService;
import com.datashield.util.DataMaskUtil;
import com.datashield.util.UserSqlConnectionUtil;
import com.datashield.util.VirtualThreadPoolUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * 数据脱敏服务实现类
 */
@Slf4j
@Service
public class DataMaskServiceImpl implements DataMaskService {
    @Autowired
    private TaskMapper taskMapper;
    @Autowired
    private RemoteDataMapper remoteDataMapper;

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
        // 1. 更新任务状态为执行中
        task.setStatus(TaskStatusEnum.RUNNING.getCode());
        task.setUpdateTime(new Timestamp(System.currentTimeMillis()));
        taskMapper.updateById(task);

        // 2. 根据用户ID和数据库名获取远程数据库信息
        UserRemoteDatabase remoteDatabase = remoteDataMapper.selectOne(new QueryWrapper<UserRemoteDatabase>()
                .eq("user_id", task.getUserId()).eq("db_name", task.getDbName()));
        if (remoteDatabase == null) {
            task.setStatus(TaskStatusEnum.ERROR.getCode());
            task.setUpdateTime(new Timestamp(System.currentTimeMillis()));
            taskMapper.updateById(task);
            return;
        }

        // 3. 连接远程数据库
        try (Connection connection = UserSqlConnectionUtil.getConnection(remoteDatabase)) {

            // 4. 解析需要脱敏的字段
            List<String> columnList = Arrays.asList(task.getDbColumns().split(","));

            // 5. 获取脱敏规则
            DataMaskRuleEnum maskRule = DataMaskRuleEnum.getDataMaskRule(task.getMaskRule());
            if (maskRule == null) {
                throw new RuntimeException("无效的脱敏规则: " + task.getMaskRule());
            }

            // 6. 查询原始数据
            String selectSql = "SELECT * FROM " + task.getDbTable();
            try (PreparedStatement selectStmt = connection.prepareStatement(selectSql);
                    ResultSet resultSet = selectStmt.executeQuery()) {

                // 7. 重新创建目标表
                String targetTable = task.getTargetTable();
                String dropSql = "DROP TABLE IF EXISTS " + targetTable;
                try (Statement stmt = connection.createStatement()) {
                    stmt.executeUpdate(dropSql);
                }
                String createSql = DataMaskUtil.getBuildTableSql(connection, task.getDbTable(), targetTable);
                try (Statement stmt = connection.createStatement()) {
                    stmt.executeUpdate(createSql);
                }

                // 8. 执行数据脱敏并插入目标表
                ResultSetMetaData rsMetaData = resultSet.getMetaData();
                int columnCount = rsMetaData.getColumnCount();

                StringBuilder insertSql = new StringBuilder("INSERT INTO " + targetTable + " VALUES (");
                for (int i = 0; i < columnCount; i++) {
                    insertSql.append("?");
                    if (i < columnCount - 1) {
                        insertSql.append(",");
                    }
                }
                insertSql.append(")");

                try (PreparedStatement insertStmt = connection.prepareStatement(insertSql.toString())) {
                    while (resultSet.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = rsMetaData.getColumnName(i);
                            String value = resultSet.getString(i);

                            if (columnList.contains(columnName)) {
                                String maskedValue = DataMaskUtil.executeDataMask(value, maskRule);
                                insertStmt.setString(i, maskedValue);
                            } else {
                                insertStmt.setString(i, value);
                            }
                        }
                        insertStmt.addBatch();
                    }
                    insertStmt.executeBatch();
                }
            }

            // 9. 更新任务状态为成功
            task.setStatus(TaskStatusEnum.DONE.getCode());
            task.setUpdateTime(new Timestamp(System.currentTimeMillis()));
            taskMapper.updateById(task);
        } catch (Exception e) {
            log.error("远程数据脱敏任务执行失败, 任务ID: " + task.getId(), e);
            // 更新任务状态为失败
            task.setStatus(TaskStatusEnum.ERROR.getCode());
            task.setUpdateTime(new Timestamp(System.currentTimeMillis()));
            taskMapper.updateById(task);
        }
    }

    /**
     * 本地 MySQL 脱敏
     */
    @Override
    public void maskLocalData(Task task) {
        // 1. 更新任务状态为运行中
        task.setStatus(TaskStatusEnum.RUNNING.getCode());
        task.setUpdateTime(new Timestamp(System.currentTimeMillis()));
        taskMapper.updateById(task);

        String fullDbName = task.getUserId() + "_" + task.getDbName();
        // 3. 连接本地数据库
        try (Connection connection = UserSqlConnectionUtil.getConnection(fullDbName)) {

            // 4. 解析需要脱敏的字段
            List<String> columnList = Arrays.asList(task.getDbColumns().split(","));

            // 5. 获取脱敏规则
            DataMaskRuleEnum maskRule = DataMaskRuleEnum.getDataMaskRule(task.getMaskRule());
            if (maskRule == null) {
                throw new RuntimeException("无效的脱敏规则: " + task.getMaskRule());
            }

            // 6. 查询原始数据
            String selectSql = "SELECT * FROM " + task.getDbTable();
            try (PreparedStatement selectStmt = connection.prepareStatement(selectSql);
                    ResultSet resultSet = selectStmt.executeQuery()) {

                // 7. 重新创建目标表
                String targetTable = task.getTargetTable();
                String dropSql = "DROP TABLE IF EXISTS " + targetTable;
                try (Statement stmt = connection.createStatement()) {
                    stmt.executeUpdate(dropSql);
                }
                String createSql = DataMaskUtil.getBuildTableSql(connection, task.getDbTable(), targetTable);
                try (Statement stmt = connection.createStatement()) {
                    stmt.executeUpdate(createSql);
                }

                // 8. 执行数据脱敏并插入目标表
                ResultSetMetaData rsMetaData = resultSet.getMetaData();
                int columnCount = rsMetaData.getColumnCount();

                StringBuilder insertSql = new StringBuilder("INSERT INTO " + targetTable + " VALUES (");
                for (int i = 0; i < columnCount; i++) {
                    insertSql.append("?");
                    if (i < columnCount - 1) {
                        insertSql.append(",");
                    }
                }
                insertSql.append(")");

                try (PreparedStatement insertStmt = connection.prepareStatement(insertSql.toString())) {
                    while (resultSet.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = rsMetaData.getColumnName(i);
                            String value = resultSet.getString(i);

                            if (columnList.contains(columnName)) {
                                String maskedValue = DataMaskUtil.executeDataMask(value, maskRule);
                                insertStmt.setString(i, maskedValue);
                            } else {
                                insertStmt.setString(i, value);
                            }
                        }
                        insertStmt.addBatch();
                    }
                    insertStmt.executeBatch();
                }
            }

            // 9. 更新任务状态为成功
            task.setStatus(TaskStatusEnum.DONE.getCode());
            task.setUpdateTime(new Timestamp(System.currentTimeMillis()));
            taskMapper.updateById(task);
        } catch (Exception e) {
            log.error("远程数据脱敏任务执行失败, 任务ID: " + task.getId(), e);
            // 更新任务状态为失败
            task.setStatus(TaskStatusEnum.ERROR.getCode());
            task.setUpdateTime(new Timestamp(System.currentTimeMillis()));
            taskMapper.updateById(task);
        }
    }
}