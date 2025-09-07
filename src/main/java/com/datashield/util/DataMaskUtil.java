package com.datashield.util;

import com.datashield.enums.DataMaskRuleEnum;

public class DataMaskUtil {
    public static String executeDataMask(String originalValue, DataMaskRuleEnum maskRule) {
        if (originalValue == null) {
            return null;
        }
        return "脱敏算法" + maskRule.getDescription() + " - 原值: " + originalValue;
    }
}
