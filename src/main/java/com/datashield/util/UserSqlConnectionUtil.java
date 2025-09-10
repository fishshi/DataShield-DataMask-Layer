package com.datashield.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.datashield.entity.UserRemoteDatabase;
import com.datashield.enums.DatabaseTypeEnum;

/**
 * 用户数据库连接工具类
 */
public class UserSqlConnectionUtil {
    /**
     * 获取用户本地 MySQL 数据库连接
     *
     * @return 数据库连接
     */
    public static Connection getConnection(String dbName) throws SQLException {
        String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/" + dbName;
        String username = "root";
        String password = "123456";
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    /**
     * 获取用户远程 MySQL 数据库连接
     * 
     * @return 数据库连接, 不支持的数据库类型返回 null
     */
    public static Connection getConnection(UserRemoteDatabase userRemoteDatabase) throws SQLException {
        String jdbcUrl;
        if (userRemoteDatabase.getDbType() == DatabaseTypeEnum.MYSQL.getCode()) {
            jdbcUrl = "jdbc:mysql://" + userRemoteDatabase.getDbHost() + ":" + userRemoteDatabase.getDbPort()
                    + "/" + userRemoteDatabase.getDbName();
        } else if (userRemoteDatabase.getDbType() == DatabaseTypeEnum.POSTGRESQL.getCode()) {
            jdbcUrl = "jdbc:postgresql://" + userRemoteDatabase.getDbHost() + ":" + userRemoteDatabase.getDbPort()
                    + "/" + userRemoteDatabase.getDbName();
        } else if (userRemoteDatabase.getDbType() == DatabaseTypeEnum.ORACLE.getCode()) {
            jdbcUrl = "jdbc:oracle:thin:@" + userRemoteDatabase.getDbHost() + ":" + userRemoteDatabase.getDbPort()
                    + ":" + userRemoteDatabase.getDbName();
        } else if (userRemoteDatabase.getDbType() == DatabaseTypeEnum.SQLSERVER.getCode()) {
            jdbcUrl = "jdbc:sqlserver://" + userRemoteDatabase.getDbHost() + ":" + userRemoteDatabase.getDbPort()
                    + ";databaseName=" + userRemoteDatabase.getDbName();
        } else {
            return null;
        }
        return DriverManager.getConnection(jdbcUrl, userRemoteDatabase.getDbUsername(),
                userRemoteDatabase.getDbPassword());
    }
}
