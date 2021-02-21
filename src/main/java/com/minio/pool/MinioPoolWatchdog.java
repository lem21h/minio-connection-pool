package com.minio.pool;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MinioPoolWatchdog implements Runnable {
    private static final int EXECUTE_EVERY_SECONDS = 60;

    private final MinioPool minioPool;
    private final ScheduledExecutorService executorService;

    public MinioPoolWatchdog(MinioPool minioPool) {
        this.minioPool = minioPool;
        this.executorService = Executors.newScheduledThreadPool(1);
    }

    public void shutdown() {
        executorService.shutdown();
    }

    public void start() {
        executorService.scheduleWithFixedDelay(this, 0, EXECUTE_EVERY_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        minioPool.checkPool();
    }
}
