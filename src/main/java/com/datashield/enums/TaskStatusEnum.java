package com.datashield.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 脱敏任务状态枚举类
 */
@Getter
@AllArgsConstructor
public enum TaskStatusEnum {
    WAITING(0),
    RUNNING(1),
    DONE(2),
    ERROR(3);

    private final int code;

    /**
     * 根据 code 获取对应的脱敏任务状态枚举类对象
     *
     * @param code 脱敏任务状态代码
     *
     * @return 对应的脱敏任务状态枚举类对象, 如果不存在则返回 null
     */
    public static TaskStatusEnum getTaskStatus(int code) {
        for (TaskStatusEnum taskStatusEnum : TaskStatusEnum.values()) {
            if (taskStatusEnum.getCode() == code) {
                return taskStatusEnum;
            }
        }
        return null;
    }
}
