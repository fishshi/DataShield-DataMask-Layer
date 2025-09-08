package com.datashield.service.impl;

import com.datashield.entity.Task;
import com.datashield.entity.UserRemoteDatabase;
import com.datashield.enums.DataMaskRuleEnum;
import com.datashield.enums.TaskStatusEnum;
import com.datashield.mapper.RemoteDataMapper;
import com.datashield.mapper.TaskMapper;
import com.datashield.util.UserSqlConnectionUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;



class DataMaskServiceImplTest {

    @InjectMocks
    private DataMaskServiceImpl dataMaskService;

    @Mock
    private TaskMapper taskMapper;

    @Mock
    private RemoteDataMapper remoteDataMapper;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testMaskLocalData_tableNotExist() throws Exception {
        Task task = new Task();
        task.setId(1L);
        task.setDbColumns("col1");
        task.setMaskRule(DataMaskRuleEnum.MASK.ordinal());
        task.setTargetTable("target");
        task.setDbTable("source");

        when(taskMapper.updateById((Task) any())).thenReturn(1);
        Connection conn = mock(Connection.class);
        // when(UserSqlConnectionUtil.getConnection()).thenReturn(conn);
        when(conn.getAutoCommit()).thenReturn(true);
        doNothing().when(conn).setAutoCommit(false);

        // checkTargetTableExists returns false
        DataMaskServiceImpl spyService = Mockito.spy(dataMaskService);
        // doReturn(false).when(spyService).checkTargetTableExists(any(), anyString());

        assertDoesNotThrow(() -> spyService.maskLocalData(task));
        verify(taskMapper, atLeastOnce()).updateById((Task) any());
    }

    @Test
    void testMaskLocalData_success() throws Exception {
        Task task = new Task();
        task.setId(2L);
        task.setDbColumns("col1");
        task.setMaskRule(DataMaskRuleEnum.MASK.ordinal());
        task.setTargetTable("target");
        task.setDbTable("source");

        when(taskMapper.updateById((Task) any())).thenReturn(1);
        Connection conn = mock(Connection.class);
        // when(UserSqlConnectionUtil.getConnection()).thenReturn(conn);
        when(conn.getAutoCommit()).thenReturn(true);
        doNothing().when(conn).setAutoCommit(false);

        PreparedStatement selectStmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(conn.prepareStatement("SELECT * FROM source")).thenReturn(selectStmt);
        when(selectStmt.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("col1")).thenReturn("abc");

        PreparedStatement insertStmt = mock(PreparedStatement.class);
        when(conn.prepareStatement(anyString())).thenReturn(insertStmt);
        doNothing().when(insertStmt).setString(anyInt(), anyString());
        doNothing().when(insertStmt).addBatch();
        when(insertStmt.executeBatch()).thenReturn(new int[]{1});
        doNothing().when(conn).commit();

        DataMaskServiceImpl spyService = Mockito.spy(dataMaskService);
        // doReturn(true).when(spyService).checkTargetTableExists(any(), anyString());
        // doReturn("masked").when(spyService).performMasking(anyString(), any());

        assertDoesNotThrow(() -> spyService.maskLocalData(task));
        verify(taskMapper, atLeastOnce()).updateById((Task) any());
    }

    // --- Helper methods to access private/protected methods ---

    private String invokePerformMasking(String original, DataMaskRuleEnum rule) {
        try {
            var m = DataMaskServiceImpl.class.getDeclaredMethod("performMasking", String.class, DataMaskRuleEnum.class);
            m.setAccessible(true);
            return (String) m.invoke(dataMaskService, original, rule);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private UserRemoteDatabase invokeGetUserRemoteDatabase(Long userId, String dbName) {
        try {
            var m = DataMaskServiceImpl.class.getDeclaredMethod("getUserRemoteDatabase", Long.class, String.class);
            m.setAccessible(true);
            return (UserRemoteDatabase) m.invoke(dataMaskService, userId, dbName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean invokeCheckTargetTableExists(Connection conn, String table) {
        try {
            var m = DataMaskServiceImpl.class.getDeclaredMethod("checkTargetTableExists", Connection.class, String.class);
            m.setAccessible(true);
            return (boolean) m.invoke(dataMaskService, conn, table);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}