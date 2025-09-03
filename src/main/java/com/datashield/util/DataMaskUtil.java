package com.datashield.util;

import com.datashield.enums.DataMaskRuleEnum;

public class DataMaskUtil {
    public static String executeDataMask(String originalValue, DataMaskRuleEnum maskRule) {
        if (originalValue == null) {
            return null;
        }

        // 模拟不同的脱敏算法
        switch (maskRule) {
            case EMAIL_MASK:
                return "执行了脱敏算法" + maskRule.getDescription() + " - 原值: " + originalValue;
            case PHONE_MASK:
                return "执行了脱敏算法" + maskRule.getDescription() + " - 原值: " + originalValue;
            case ID_CARD_MASK:
                return "执行了脱敏算法" + maskRule.getDescription() + " - 原值: " + originalValue;
            case NAME_MASK:
                return "执行了脱敏算法" + maskRule.getDescription() + " - 原值: " + originalValue;
            case CREDIT_CARD_MASK:
                return "执行了脱敏算法" + maskRule.getDescription() + " - 原值: " + originalValue;
            default:
                return "执行了脱敏算法未知规则 - 原值: " + originalValue;
        }
    }
}
