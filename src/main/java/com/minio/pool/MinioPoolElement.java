package com.minio.pool;

import java.util.concurrent.atomic.AtomicBoolean;

public class MinioPoolElement {
    private final MinioConnection minio;
    private final AtomicBoolean active = new AtomicBoolean();

    public MinioPoolElement(MinioConnection connection) {
        this.minio = connection;
    }

    public void markNotActive() {
        this.active.setPlain(false);
    }

    void markActive() {
        this.active.setPlain(true);
    }

    public boolean isActive() {
        return this.active.get();
    }

    public MinioConnection getMinioConnection() {
        return minio;
    }
}