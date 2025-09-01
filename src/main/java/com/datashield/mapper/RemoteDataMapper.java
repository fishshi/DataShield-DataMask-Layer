package com.datashield.mapper;

import org.apache.ibatis.annotations.Mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.datashield.entity.UserRemoteDatabase;

/**
 * 用户远程数据库映射接口
 */
@Mapper
public interface RemoteDataMapper extends BaseMapper<UserRemoteDatabase> {
}
