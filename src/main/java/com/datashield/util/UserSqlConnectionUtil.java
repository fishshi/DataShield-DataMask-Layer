package com.datashield.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.datashield.entity.UserRemoteDatabase;
import com.datashield.enums.DatabaseTypeEnum;

/**
 * 用户数据库连接工具类
 * TODO: 本地改用用户 MySQL 服务器, 与业务服务器分开
 */
public class UserSqlConnectionUtil {
    /**
     * 获取用户本地 MySQL 数据库连接
     * 
     * @return 数据库连接
     */
    public static Connection getConnection() throws SQLException {
        String jdbcUrl = "jdbc:mysql://127.0.0.1:3306";
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
        } else {
            return null;
        }
        return DriverManager.getConnection(jdbcUrl, userRemoteDatabase.getDbUsername(),
                userRemoteDatabase.getDbPassword());
    }
}
