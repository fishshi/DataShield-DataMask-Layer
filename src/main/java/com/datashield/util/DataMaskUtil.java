package com.datashield.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.datashield.enums.DataMaskRuleEnum;

public class DataMaskUtil {
    public static String executeDataMask(String originalValue, DataMaskRuleEnum maskRule) {
        if (originalValue == null) {
            return null;
        }
        return "脱敏算法" + maskRule.getDescription() + " - 原值: " + originalValue;
    }

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
