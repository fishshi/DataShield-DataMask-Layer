package com.datashield.util;

import java.util.List;
import java.util.Map;

import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

import com.fasterxml.jackson.databind.JsonNode;

import lombok.extern.slf4j.Slf4j;

/**
 * AI 工具类
 */
@Slf4j
public class AIUtil {
    private static String BASE_URL = "http://localhost:11434";

    public static String identifyData(List<String> columns) {
        String prompt = buildPrompt(columns);

        Map<String, Object> request = Map.of(
                "model", "deepseek-r1:1.5b",
                "prompt", prompt,
                "stream", false);

        String response = WebClient.builder().baseUrl(BASE_URL).build().post()
                .uri("/api/generate")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(json -> json.get("response").asText())
                .block();
        log.info("[AIUtil] Ollama response: {}", response);
        response = response.split("</think>")[1];
        response = response.trim();
        if (response.startsWith("```json")) {
            response = response.replace("```json", "");
            response = response.split("```")[0];
            response = response.trim();
        }
        return response;
    }

    private static String buildPrompt(List<String> columns) {
        return "以下是数据库表的字段名，请识别其中的敏感数据字段，并严格输出 JSON 数组，格式为：\n" +
                "[{\"column\":\"字段名\", \"type\":\"敏感数据类型\", \"level\":\"敏感级别\"}]\n\n" +
                "要求：\n" +
                "1. 只输出 JSON，不要解释，不要思考过程。\n" +
                "2. \"type\" 表示敏感数据的具体类型，例如：身份证号、手机号、邮箱、密码、用户名、地址、银行卡号。\n" +
                "3. \"level\" 表示敏感程度，只能是：高、中、低。\n" +
                "字段名列表：" + String.join(", ", columns);
    }
}
