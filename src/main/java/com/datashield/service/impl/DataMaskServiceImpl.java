package com.datashield.service.impl;

import cn.hutool.core.util.StrUtil;
import com.datashield.entity.Task;
import com.datashield.enums.DataMaskRuleEnum;
import com.datashield.enums.TaskStatusEnum;
import com.datashield.mapper.TaskMapper;
import com.datashield.service.DataMaskService;
import com.datashield.util.VirtualThreadPoolUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Slf4j
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
        // TODO 远程库脱敏实现
    }

    /**
     * 本地 MySQL 脱敏
     */
    @Override
    public void maskLocalData(Task task) {
        log.info("开始本地脱敏，taskId={}", task.getId());
        task.setStatus(TaskStatusEnum.RUNNING.getCode());
        taskMapper.updateById(task);
        String dbName      = task.getDbName();
        String srcTable    = task.getDbTable();
        String targetTable = task.getTargetTable();
        String columnsStr  = task.getDbColumns();
        DataMaskRuleEnum ruleEnum = DataMaskRuleEnum.of(task.getMaskRule());

        if (StrUtil.isBlank(columnsStr) || ruleEnum == null) {
            log.error("脱敏字段或规则为空，taskId={}", task.getId());
            fail(task);
            return;
        }

        // 解析字段
        String[] columns = columnsStr.split(",");
        List<String> columnList = new ArrayList<>();
        for (String c : columns) {
            columnList.add(c.trim());
        }

        // 1) 构建 select 语句
        StringBuilder selectCols = new StringBuilder();
        for (String col : columnList) {
            selectCols.append(col).append(",");
        }
        selectCols.setLength(selectCols.length() - 1);
        String selectSql = "SELECT " + selectCols + " FROM " + dbName + "." + srcTable;

        // 2) 构建 insert 语句
        StringBuilder insertCols = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        for (String col : columnList) {
            insertCols.append(col).append(",");
            placeholders.append("?,");
        }
        insertCols.setLength(insertCols.length() - 1);
        placeholders.setLength(placeholders.length() - 1);
        String insertSql = "INSERT INTO " + dbName + "." + targetTable + "(" + insertCols + ") VALUES (" + placeholders + ")";

        try (Connection conn = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/" + dbName + "?useSSL=false&serverTimezone=UTC",
                "root", "123456");
             PreparedStatement selectPs = conn.prepareStatement(selectSql,
                     ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             PreparedStatement insertPs = conn.prepareStatement(insertSql)) {

            // 流式读取
            selectPs.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = selectPs.executeQuery();

            conn.setAutoCommit(false);
            int batch = 0;

            while (rs.next()) {
                // 对每一列应用脱敏规则
                for (int i = 0; i < columnList.size(); i++) {
                    String origin = rs.getString(columnList.get(i));
                    String masked = mask(origin, ruleEnum);
                    insertPs.setString(i + 1, masked);
                }
                insertPs.addBatch();
                batch++;

                if (batch % 1000 == 0) {
                    insertPs.executeBatch();
                    conn.commit();
                }
            }
            insertPs.executeBatch();
            conn.commit();

            task.setStatus(TaskStatusEnum.FINISHED.getCode());
            taskMapper.updateById(task);
            log.info("本地脱敏完成，taskId={}", task.getId());

        } catch (Exception e) {
            log.error("本地脱敏异常，taskId=" + task.getId(), e);
            fail(task);
        }
    }

    /**
     * 根据规则脱敏
     */
    private String mask(String origin, DataMaskRuleEnum rule) {
        if (origin == null) return null;
        switch (rule) {
            case MASK_NAME:
                // 姓名：保留姓，其余*
                if (origin.length() <= 1) return origin;
                return origin.charAt(0) + "*".repeat(origin.length() - 1);
            case MASK_PHONE:
                // 手机：前三后四
                if (origin.length() < 11) return origin;
                return origin.substring(0, 3) + "****" + origin.substring(7);
            case MASK_ID_CARD:
                // 身份证：前6后4
                if (origin.length() < 18) return origin;
                return origin.substring(0, 6) + "********" + origin.substring(14);
            default:
                return origin;
        }
    }

    private void fail(Task task) {
        task.setStatus(TaskStatusEnum.FAILED.getCode());
        taskMapper.updateById(task);
    }
}