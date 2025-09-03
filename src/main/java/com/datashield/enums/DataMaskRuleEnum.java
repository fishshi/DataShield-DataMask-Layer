package com.datashield.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 0-待执行, 1-执行中, 2-执行成功, 3-执行失败
 */

/**
 * 数据脱敏规则枚举类
 */
@Getter
@AllArgsConstructor
public enum DataMaskRuleEnum {
    EMAIL_MASK(1, "邮箱脱敏"),
    PHONE_MASK(2, "手机号脱敏"),
    ID_CARD_MASK(3, "身份证脱敏"),
    NAME_MASK(4, "姓名脱敏"),
    CREDIT_CARD_MASK(5, "银行卡脱敏");

    private final int code;
    private final String description;

    /**
     * 根据 code 获取对应的数据脱敏规则枚举类对象
     *
     * @param code 数据脱敏规则代码
     *
     * @return 对应的数据脱敏规则枚举类对象, 如果不存在则返回 null
     */
    public static DataMaskRuleEnum getDataMaskRule(int code) {
        for (DataMaskRuleEnum dataMaskRuleEnum : DataMaskRuleEnum.values()) {
            if (dataMaskRuleEnum.getCode() == code) {
                return dataMaskRuleEnum;
            }
        }
        return null;
    }
}
