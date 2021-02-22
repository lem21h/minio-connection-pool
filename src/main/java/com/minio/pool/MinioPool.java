package com.minio.pool;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MinioPool {
    private static final int DEFAULT_CONNECTION_TIMEOUT = 2500;
    private static final int DEFAULT_READ_TIMEOUT = 5000;

    private static final int MAX_CONNECTION_LOOP_TIMES = 2;

    private final List<MinioConnection> pool = new ArrayList<>();
    private final AtomicInteger counter = new AtomicInteger();

    private final int connectionTimeout;
    private final int readTimeout;

    public MinioPool() {
        this(DEFAULT_CONNECTION_TIMEOUT, DEFAULT_READ_TIMEOUT);
    }

    public MinioPool(int connectionTimeout, int readTimeout) {
        this.connectionTimeout = connectionTimeout;
        this.readTimeout = readTimeout;
    }

    public List<MinioConnection> getConnectionPool() {
        return pool;
    }

    public void addToPool(MinioConnectionConfig minioConnectionConfig) throws MalformedURLException {
        pool.add(new MinioConnection(minioConnectionConfig));
    }

    public MinioConnection getNextConnection() {
        final var poolSize = this.pool.size();
        int i = 0;
        while (i++ < MAX_CONNECTION_LOOP_TIMES * poolSize) {
            final var connection = pool.get(Math.abs(counter.incrementAndGet() % poolSize));
            if (connection.isActive()) {
                return connection;
            }
        }
        return null;
    }

    public void checkPool() {
        for (final var connection : pool) {
            connection.healthCheck(connectionTimeout, readTimeout);
        }
    }
}
