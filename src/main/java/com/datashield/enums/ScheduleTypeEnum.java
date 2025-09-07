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

    public static ScheduleTypeEnum getEnum(int value) {
        for (ScheduleTypeEnum type : ScheduleTypeEnum.values()) {
            if (type.getValue() == value) {
                return type;
            }
        }
        return null;
    }
}
