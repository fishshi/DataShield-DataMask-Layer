package com.datashield.enums;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


class ScheduleTypeEnumTest {

    @Test
    void testGetValue() {
        assertEquals(0, ScheduleTypeEnum.ONE_TIME.getValue());
        assertEquals(1, ScheduleTypeEnum.DAILY.getValue());
        assertEquals(2, ScheduleTypeEnum.WEEKLY.getValue());
    }

    @Test
    void testGetEnumValidValues() {
        assertEquals(ScheduleTypeEnum.ONE_TIME, ScheduleTypeEnum.getEnum(0));
        assertEquals(ScheduleTypeEnum.DAILY, ScheduleTypeEnum.getEnum(1));
        assertEquals(ScheduleTypeEnum.WEEKLY, ScheduleTypeEnum.getEnum(2));
    }

    @Test
    void testGetEnumInvalidValue() {
        assertNull(ScheduleTypeEnum.getEnum(-1));
        assertNull(ScheduleTypeEnum.getEnum(3));
        assertNull(ScheduleTypeEnum.getEnum(100));
    }
}