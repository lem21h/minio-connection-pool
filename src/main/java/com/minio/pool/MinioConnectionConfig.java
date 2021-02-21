package com.minio.pool;

import java.net.URI;
import java.util.Objects;

public class MinioConnectionConfig {
    private final String endpoint;
    private final String login;
    private final String password;

    private final String host;
    private final int port;

    public MinioConnectionConfig(String endpoint, String login, String password) {
        this.endpoint = endpoint;
        this.login = login;
        this.password = password;

        final var uri = URI.create(endpoint);
        this.host = uri.getHost();
        this.port = uri.getPort();
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getLogin() {
        return login;
    }

    public String getPassword() {
        return password;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isValid() {
        return endpoint != null && !endpoint.isBlank() &&
                login != null && !login.isBlank() &&
                password != null &&
                port > 0 && port < 65535 && host != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MinioConnectionConfig that = (MinioConnectionConfig) o;
        return port == that.port && endpoint.equals(that.endpoint) && host.equals(that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoint, host, port);
    }
}