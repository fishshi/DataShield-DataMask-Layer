package com.datashield.entity;

import java.sql.Timestamp;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import com.datashield.enums.TaskStatusEnum;

/**
 * 识别任务实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@TableName("tb_identify")
public class Identify {
    /**
     * 主键 id, 雪花算法生成
     */
    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    /**
     * 用户 id
     */
    private Long userId;

    /**
     * 识别名称
     */
    private String identifyName;

    /**
     * 是否远程数据库
     */
    private Integer isRemote;

    /**
     * 数据库名称
     */
    private String dbName;

    /**
     * 表名称
     */
    private String tbName;

    /**
     * 字段名称
     */
    private String columns;

    /**
     * 状态, {@link TaskStatusEnum}
     */
    private Integer status;

    /**
     * 最后更新时间
     */
    private Timestamp updateTime;
}
