package com.founder.config;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class AsyncConfig {

    @Bean("taskExecutor")
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);       // 核心线程数
        executor.setMaxPoolSize(20);       // 最大线程数
        executor.setQueueCapacity(100);    // 队列容量
        executor.setKeepAliveSeconds(60);  // 线程空闲时间
        executor.setThreadNamePrefix("compare-task-");
        executor.setRejectedExecutionHandler((r, exe) -> {
            // 任务拒绝策略
            throw new RuntimeException("任务队列已满，暂时无法处理");
        });
        return executor;
    }
}