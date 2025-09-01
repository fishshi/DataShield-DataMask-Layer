package com.datashield.mapper;

import org.apache.ibatis.annotations.Mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.datashield.entity.Task;

/**
 * 脱敏任务映射接口
 */
@Mapper
public interface TaskMapper extends BaseMapper<Task> {
}
