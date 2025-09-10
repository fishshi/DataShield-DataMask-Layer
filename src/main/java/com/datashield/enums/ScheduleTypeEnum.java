package com.datashield.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 周期性任务类型枚举
 */
@Getter
@AllArgsConstructor
public enum ScheduleTypeEnum {
    ONE_TIME(0),
    DAILY(1),
    WEEKLY(2);

    private final int value;

    /**
     * 根据 code 获取对应的周期性任务类型枚举类对象
     *
     * @param code 周期性任务类型代码
     *
     * @return 对应的周期性任务类型枚举类对象, 如果不存在则返回 null
     */
    public static ScheduleTypeEnum getEnum(int value) {
        for (ScheduleTypeEnum type : ScheduleTypeEnum.values()) {
            if (type.getValue() == value) {
                return type;
            }
        }
        return null;
    }
}
