package com.datashield.service.impl;

import cn.hutool.core.util.StrUtil;
import com.datashield.entity.Task;
import com.datashield.enums.DataMaskRuleEnum;
import com.datashield.enums.TaskStatusEnum;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.datashield.entity.Task;
import com.datashield.entity.UserRemoteDatabase;
import com.datashield.enums.DataMaskRuleEnum;
import com.datashield.enums.TaskStatusEnum;
import com.datashield.mapper.RemoteDataMapper;
import com.datashield.mapper.TaskMapper;
import com.datashield.service.DataMaskService;
import com.datashield.util.UserSqlConnectionUtil;
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
        // TODO 远程库脱敏实现
        System.out.println("开始执行远程数据脱敏任务，任务ID: " + task.getId());

        try {
            // 1. 更新任务状态为执行中
            task.setStatus(TaskStatusEnum.RUNNING.getCode());
            taskMapper.updateById(task);
            System.out.println("任务状态已更新为执行中");

            // 2. 根据用户ID和数据库名获取远程数据库信息
            UserRemoteDatabase remoteDatabase = getUserRemoteDatabase(task.getUserId(), task.getDbName());
            if (remoteDatabase == null) {
                throw new RuntimeException("未找到远程数据库配置信息");
            }
            System.out.println("获取到远程数据库信息: " + remoteDatabase.getDbHost() + ":" + remoteDatabase.getDbPort());

            // 3. 连接远程数据库
            Connection connection = null;
            try {
                connection = UserSqlConnectionUtil.getConnection(remoteDatabase);
                System.out.println("成功连接到远程数据库");

                // 4. 解析需要脱敏的字段
                List<String> columnList = Arrays.asList(task.getDbColumns().split(","));
                System.out.println("需要脱敏的字段: " + columnList);

                // 5. 获取脱敏规则
                DataMaskRuleEnum maskRule = DataMaskRuleEnum.getDataMaskRule(task.getMaskRule());
                if (maskRule == null) {
                    throw new RuntimeException("无效的脱敏规则: " + task.getMaskRule());
                }
                System.out.println("使用的脱敏规则: " + maskRule.getDescription());

                // 6. 查询原始数据
                String selectSql = "SELECT * FROM " + task.getDbTable();
                PreparedStatement selectStmt = connection.prepareStatement(selectSql);
                ResultSet resultSet = selectStmt.executeQuery();
                System.out.println("查询原始数据完成");

                // 7. 检查目标表是否存在
                if (!checkTargetTableExists(connection, task.getTargetTable())) {
                    throw new RuntimeException("目标表 " + task.getTargetTable() + " 不存在，请先创建目标表");
                }

                // 8. 执行数据脱敏并插入目标表
                int processedCount = 0;
                while (resultSet.next()) {
                    // 模拟脱敏处理
                    for (String column : columnList) {
                        String originalValue = resultSet.getString(column);
                        String maskedValue = performMasking(originalValue, maskRule);
                        System.out.println("字段 " + column + " 原值: " + originalValue + " -> 脱敏后: " + maskedValue);
                    }
                    processedCount++;
                }

                // 模拟插入脱敏后的数据到目标表
                System.out.println("执行了脱敏算法" + maskRule.getDescription() + "，共处理 " + processedCount + " 条记录");
                System.out.println("脱敏数据已插入目标表: " + task.getTargetTable());

                // 9. 更新任务状态为成功
                task.setStatus(TaskStatusEnum.DONE.getCode());
                taskMapper.updateById(task);
                System.out.println("任务执行成功，状态已更新");

            } catch (SQLException e) {
                throw new RuntimeException("数据库操作失败: " + e.getMessage(), e);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                        System.out.println("数据库连接已关闭");
                    } catch (SQLException e) {
                        System.err.println("关闭数据库连接失败: " + e.getMessage());
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("远程数据脱敏任务执行失败: " + e.getMessage());
            e.printStackTrace();

            // 更新任务状态为失败
            task.setStatus(TaskStatusEnum.ERROR.getCode());
            taskMapper.updateById(task);
            System.out.println("任务状态已更新为失败");
        }
    }

    /**
     * 根据用户ID和数据库名获取远程数据库信息
     */
    private UserRemoteDatabase getUserRemoteDatabase(Long userId, String dbName) {
        // 查询条件：用户ID和数据库名
        return remoteDataMapper.selectList(null).stream()
                .filter(db -> db.getUserId().equals(userId) && db.getDbName().equals(dbName))
                .findFirst()
                .orElse(null);
    }

    /**
     * 检查目标表是否存在
     */
    private boolean checkTargetTableExists(Connection connection, String targetTable) throws SQLException {
        // 模拟检查目标表是否存在的逻辑
        System.out.println("检查目标表 " + targetTable + " 是否存在");

        // 这里应该执行实际的SQL查询来检查表是否存在
        // 例如：SHOW TABLES LIKE 'targetTable' 或 SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'targetTable'

        // 模拟返回false，表示目标表不存在
        // 在实际项目中，这里应该执行真正的数据库查询
        System.out.println("目标表 " + targetTable + " 不存在");
        return false;
    }

    /**
     * 执行具体的脱敏算法
     */
    private String performMasking(String originalValue, DataMaskRuleEnum maskRule) {
        if (originalValue == null) {
            return null;
        }

        // 模拟不同的脱敏算法
        switch (maskRule) {
            case EMAIL_MASK:
                return "执行了脱敏算法" + maskRule.getDescription() + " - 原值: " + originalValue;
            case PHONE_MASK:
                return "执行了脱敏算法" + maskRule.getDescription() + " - 原值: " + originalValue;
            case ID_CARD_MASK:
                return "执行了脱敏算法" + maskRule.getDescription() + " - 原值: " + originalValue;
            case NAME_MASK:
                return "执行了脱敏算法" + maskRule.getDescription() + " - 原值: " + originalValue;
            case CREDIT_CARD_MASK:
                return "执行了脱敏算法" + maskRule.getDescription() + " - 原值: " + originalValue;
            default:
                return "执行了脱敏算法未知规则 - 原值: " + originalValue;
        }
    }

    /**
     * 本地 MySQL 脱敏
     */
    @Override
public void maskLocalData(Task task) {
    log.info("开始执行本地数据脱敏任务，任务ID: {}", task.getId());

    Connection connection = null;
    try {
        // 1. 更新任务状态为运行中
        task.setStatus(TaskStatusEnum.RUNNING.getCode());
        taskMapper.updateById(task);
        log.info("任务状态已更新为 RUNNING");

        // 2. 拿到本地 JDBC 连接（复用 MyBatis 的 DataSource）
        connection = UserSqlConnectionUtil.getConnection();
        connection.setAutoCommit(false);      // 手动事务控制
        log.info("成功获取本地数据库连接，关闭自动提交");

        // 3. 解析要脱敏的列
        List<String> columnList = Arrays.asList(task.getDbColumns().split(","));
        log.info("需要脱敏的字段: {}", columnList);

        // 4. 解析脱敏规则
        DataMaskRuleEnum maskRule = DataMaskRuleEnum.getDataMaskRule(task.getMaskRule());
        if (maskRule == null) {
            throw new IllegalArgumentException("无效的脱敏规则: " + task.getMaskRule());
        }
        log.info("使用的脱敏规则: {}", maskRule.getDescription());

        // 5. 检查目标表是否存在
        if (!checkTargetTableExists(connection, task.getTargetTable())) {
            throw new IllegalStateException("目标表 " + task.getTargetTable() + " 不存在，请先创建");
        }

        // 6. 查询源表
        String selectSql = "SELECT * FROM " + task.getDbTable();
        try (PreparedStatement selectStmt = connection.prepareStatement(selectSql);
             ResultSet rs = selectStmt.executeQuery()) {

            // 7. 构造动态插入语句：INSERT INTO targetTable (col1,col2,...) VALUES (?,?,...)
            String insertSql = buildInsertSql(task.getTargetTable(), columnList, rs);
            try (PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {

                int processed = 0;

                while (rs.next()) {
                    // 逐列脱敏
                    for (int i = 0; i < columnList.size(); i++) {
                        String column = columnList.get(i);
                        String original = rs.getString(column);
                        String masked = performMasking(original, maskRule);

                        // 这里简单把脱敏后的值直接放到对应占位符，实际可映射所有列
                        insertStmt.setString(i + 1, masked);
                    }
                    insertStmt.addBatch();

                    processed++;
                    // 每 1000 条批量提交一次，防止内存膨胀
                    if (processed % 1000 == 0) {
                        insertStmt.executeBatch();
                        connection.commit();
                        log.info("已提交 {} 条脱敏记录", processed);
                    }
                }
                // 提交剩余
                insertStmt.executeBatch();
                connection.commit();
                log.info("本地脱敏完成，共处理 {} 条记录，已写入目标表 {}", processed, task.getTargetTable());
            }
        }

        // 8. 更新任务状态为成功
        task.setStatus(TaskStatusEnum.DONE.getCode());
        taskMapper.updateById(task);
        log.info("本地脱敏任务执行成功，状态已更新为 DONE");

    } catch (Exception e) {
        log.error("本地数据脱敏任务执行失败", e);
        rollbackQuietly(connection);
        task.setStatus(TaskStatusEnum.ERROR.getCode());
        taskMapper.updateById(task);
    } finally {
        closeQuietly(connection);
    }
}

/* ---------- 以下为辅助私有方法 ---------- */

/** 构建 INSERT 语句，列名从 ResultSetMetaData 动态获取（示例简化版） */
private String buildInsertSql(String targetTable, List<String> maskColumns, ResultSet rs) throws SQLException {
    StringBuilder sb = new StringBuilder("INSERT INTO ")
            .append(targetTable)
            .append(" (")
            .append(String.join(",", maskColumns))
            .append(") VALUES (")
            .append(String.join(",", java.util.Collections.nCopies(maskColumns.size(), "?")))
            .append(")");
    return sb.toString();
}

/** 静默回滚 */
private void rollbackQuietly(Connection conn) {
    if (conn != null) {
        try {
            conn.rollback();
        } catch (SQLException e) {
            log.warn("回滚失败", e);
        }
    }
}

/** 静默关闭连接 */
private void closeQuietly(Connection conn) {
    if (conn != null) {
        try {
            conn.close();
        } catch (SQLException e) {
            log.warn("关闭连接失败", e);
        }
    }
}
}