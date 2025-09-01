package com.datashield.mapper;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;
import java.util.Map;

/**
 * 用户上传的数据映射接口
 */
@Mapper
public interface DataMapper {
    /**
     * 执行SQL语句
     */
    @Update("#{sql}")
    void executeSql(@Param("sql") String sql);

    /**
     * 查询所有数据库名
     *
     * @return 数据库名列表
     */
    @Select("SHOW DATABASES LIKE '${startsWith}%'")
    List<String> getAllDatabases(String startsWith);

    /**
     * 查询数据库中所有表名
     *
     * @param dbName 数据库名
     *
     * @return 表名列表
     */
    @Select("SHOW TABLES FROM ${dbName}")
    List<String> getAllTables(String dbName);

    /**
     * 查询表中所有字段名
     *
     * @param dbName 数据库名
     * @param tbName 表名
     *
     * @return 字段名列表
     */
    @Select("SHOW COLUMNS FROM ${dbName}.${tbName}")
    List<String> getColumns(@Param("dbName") String dbName, @Param("tbName") String tbName);

    /**
     * 查询表中所有记录
     *
     * @param dbName 数据库名
     * @param tbName 表名
     *
     * @return 记录列表
     */
    @Select("SELECT * FROM ${dbName}.${tbName}")
    List<Map<String, Object>> getRecords(@Param("dbName") String dbName, @Param("tbName") String tbName);

    /**
     * 删除数据库
     *
     * @param dbName 数据库名
     */
    @Delete("DROP DATABASE ${dbName}")
    void dropDatabase(@Param("dbName") String dbName);
}
