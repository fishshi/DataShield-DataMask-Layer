package com.datashield.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 数据库类型枚举类
 */
@Getter
@AllArgsConstructor
public enum DatabaseTypeEnum {
    MYSQL(1),
    POSTGRESQL(2),
    ORACLE(3),
    SQLSERVER(4);

    private final int code;

    /**
     * 根据 code 获取对应的数据库类型枚举类对象
     *
     * @param code 数据库类型代码
     *
     * @return 数据库类型枚举类对象, 如果不存在则返回 null
     */
    public static DatabaseTypeEnum getDatabaseType(int code) {
        for (DatabaseTypeEnum databaseType : DatabaseTypeEnum.values()) {
            if (databaseType.code == code) {
                return databaseType;
            }
        }
        return null;
    }
}
