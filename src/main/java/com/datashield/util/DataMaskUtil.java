package com.datashield.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.datashield.enums.DataMaskRuleEnum;

/**
 * 数据脱敏工具类
 */
public class DataMaskUtil {
    /**
     * 执行数据脱敏算法
     *
     * @param originalValue 原始值
     * @param maskRule 脱敏规则
     *
     * @return 脱敏后的值
     */
    public static String executeDataMask(String originalValue, DataMaskRuleEnum maskRule) {
        if (originalValue == null) {
            return null;
        }
        return "脱敏算法" + maskRule.getDescription() + " - 原值: " + originalValue;
    }

    /**
     * 获取建数据脱敏目标表语句
     *
     * @param conn 数据库连接
     * @param sourceTable 源表名
     * @param targetTable 目标表名
     *
     * @return 建数据脱敏目标表语句
     */
    public static String getBuildTableSql(Connection conn, String sourceTable, String targetTable) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        try (ResultSet rs = metaData.getColumns(conn.getCatalog(), null, sourceTable, null)) {
            StringBuilder sql = new StringBuilder();
            sql.append("CREATE TABLE ").append(targetTable).append(" AS SELECT ");

            boolean first = true;
            while (rs.next()) {
                if (!first) {
                    sql.append(", ");
                }
                String colName = rs.getString("COLUMN_NAME");
                sql.append("CAST(").append(colName).append(" AS CHAR(255)) AS ").append(colName);
                first = false;
            }
            sql.append(" FROM ").append(sourceTable).append(" WHERE 0=1");
            return sql.toString();
        }
    }
}
