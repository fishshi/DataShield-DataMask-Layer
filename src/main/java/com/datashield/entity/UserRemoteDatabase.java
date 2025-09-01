package com.datashield.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import com.datashield.enums.DatabaseTypeEnum;

/**
 * 用户远程数据库实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@TableName("tb_user_remote_database")
public class UserRemoteDatabase {
    /**
     * 主键 ID, 雪花算法生成
     */
    @TableId(type = IdType.ASSIGN_ID)
    Long id;

    /**
     * 用户 ID, 对应 tb_user 表的主键 ID
     */
    Long userId;

    /**
     * 数据库的 IP 地址
     */
    String dbHost;

    /**
     * 数据库的端口号
     */
    Integer dbPort;

    /**
     * 数据库的类型 {@link DatabaseTypeEnum}
     */
    Integer dbType;

    /**
     * 数据库的名称
     */
    String dbName;

    /**
     * 连接数据库使用的用户名
     */
    String dbUsername;

    /**
     * 连接数据库使用的密码
     */
    String dbPassword;
}
