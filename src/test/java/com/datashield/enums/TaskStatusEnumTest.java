package com.datashield.enums;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;



class TaskStatusEnumTest {

    @Test
    void testGetCode() {
        assertEquals(0, TaskStatusEnum.WAITING.getCode());
        assertEquals(1, TaskStatusEnum.RUNNING.getCode());
        assertEquals(2, TaskStatusEnum.DONE.getCode());
        assertEquals(3, TaskStatusEnum.ERROR.getCode());
    }

    @Test
    void testGetTaskStatus_ValidCodes() {
        assertEquals(TaskStatusEnum.WAITING, TaskStatusEnum.getTaskStatus(0));
        assertEquals(TaskStatusEnum.RUNNING, TaskStatusEnum.getTaskStatus(1));
        assertEquals(TaskStatusEnum.DONE, TaskStatusEnum.getTaskStatus(2));
        assertEquals(TaskStatusEnum.ERROR, TaskStatusEnum.getTaskStatus(3));
    }

    @Test
    void testGetTaskStatus_InvalidCode() {
        assertNull(TaskStatusEnum.getTaskStatus(-1));
        assertNull(TaskStatusEnum.getTaskStatus(4));
        assertNull(TaskStatusEnum.getTaskStatus(100));
    }
}