package com.datashield.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 识别工具类（基于规则的敏感数据识别）
 */
public class IdentifyUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 根据字段名识别敏感数据类型和级别
     *
     * @param columns 字段名列表
     *
     * @return JSON 字符串，格式：[{"column":"xxx","type":"xxx","level":"xxx"}, ...]
     */
    public static String identifyData(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return "[]";
        }
        List<Map<String, String>> results = new ArrayList<>();
        for (String column : columns) {
            Map<String, String> fieldInfo = new HashMap<>();
            fieldInfo.put("column", column);
            String lower = column.toLowerCase();
            if (lower.contains("password") || lower.contains("pass")) {
                fieldInfo.put("type", "密码");
                fieldInfo.put("level", "高");
            } else if (lower.contains("id_card") || lower.contains("identity")) {
                fieldInfo.put("type", "身份证号");
                fieldInfo.put("level", "高");
            } else if (lower.contains("phone") || lower.contains("mobile")) {
                fieldInfo.put("type", "手机号");
                fieldInfo.put("level", "高");
            } else if (lower.contains("email")) {
                fieldInfo.put("type", "邮箱");
                fieldInfo.put("level", "中");
            } else if (lower.contains("username") || lower.contains("name")) {
                fieldInfo.put("type", "用户名");
                fieldInfo.put("level", "中");
            } else if (lower.contains("address")) {
                fieldInfo.put("type", "地址");
                fieldInfo.put("level", "低");
            } else {
                fieldInfo.put("type", "无");
                fieldInfo.put("level", "无");
            }
            results.add(fieldInfo);
        }
        try {
            return objectMapper.writeValueAsString(results);
        } catch (Exception e) {
            return "[]";
        }
    }
}
