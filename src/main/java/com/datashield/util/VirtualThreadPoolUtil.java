package com.datashield.util;

import java.util.concurrent.*;

/**
 * 虚拟线程池工具类
 */
public class VirtualThreadPoolUtil {
    private static final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /**
     * 提交任务
     */
    public static Future<?> submit(Runnable task) {
        return executor.submit(task);
    }

    /**
     * 提交一个有返回值的任务
     */
    public static <T> Future<T> submit(Callable<T> task) {
        return executor.submit(task);
    }
}