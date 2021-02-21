package com.minio.pool;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.MinioException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public class MinioConnection {
    private static final String HEALTH_ENDPOINT = "/minio/health/live";

    private final MinioConnectionConfig connectionDetails;
    private final InetSocketAddress inetAddress;
    private final URL urlAddress;

    private MinioClient client;

    public MinioConnection(MinioConnectionConfig connectionDetails) throws MalformedURLException {
        this.connectionDetails = connectionDetails;
        this.inetAddress = new InetSocketAddress(connectionDetails.getHost(), connectionDetails.getPort());
        this.urlAddress = new URL(connectionDetails.getEndpoint() + HEALTH_ENDPOINT);
    }

    public String getEndpoint() {
        return connectionDetails.getEndpoint();
    }

    public boolean checkConnection(int timeout) {
        try (final var socket = new Socket()) {
            socket.connect(inetAddress, timeout);
            return true;
        } catch (Exception exception) {
            return false;
        }
    }

    private MinioClient getClient() {
        if (client == null) {
            connect();
        }
        return client;
    }

    public void connect() {
        this.client = MinioClient.builder()
                .endpoint(connectionDetails.getEndpoint())
                .credentials(connectionDetails.getLogin(), connectionDetails.getPassword())
                .build();
    }

    public boolean healthCheck(int connectionTimeout, int readTimeout) {
        try {
            final var connection = (HttpURLConnection) urlAddress.openConnection();
            connection.setConnectTimeout(connectionTimeout);
            connection.setReadTimeout(readTimeout);
            connection.setRequestMethod("GET");

            return connection.getResponseCode() == 200;
        } catch (IOException exception) {
            return false;
        }
    }

    public boolean checkBucketExists(String bucket) throws MinioException, IOException {
        try {
            return getClient().bucketExists(BucketExistsArgs.builder().bucket(bucket).build());
        } catch (MinioException | InvalidKeyException | NoSuchAlgorithmException exception) {
            throw new MinioException(exception.getMessage());
        }
    }

    public void createBucket(String bucket) throws MinioException, IOException {
        try {
            getClient().makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
        } catch (ErrorResponseException | XmlParserException | ServerException | NoSuchAlgorithmException | InvalidResponseException | InvalidKeyException | InternalException | InsufficientDataException exception) {
            throw new MinioException(exception.getMessage());
        }
    }

    public Map<String, String> fetchFileMetadata(String bucket, String resourcePath) throws MinioException, IOException {
        try {
            final var stat = getClient().statObject(
                    StatObjectArgs.builder()
                            .object(resourcePath)
                            .bucket(bucket)
                            .build()
            );
            return stat.userMetadata();
        } catch (NoSuchAlgorithmException | InvalidKeyException | MinioException exception) {
            throw new MinioException(exception.getMessage());
        }
    }

    public InputStream fetchFile(String bucket, String resourcePath) throws MinioException, IOException {
        try {
            return getClient().getObject(
                    GetObjectArgs.builder()
                            .object(resourcePath)
                            .bucket(bucket)
                            .build()
            );
        } catch (NoSuchAlgorithmException | InvalidKeyException | MinioException exception) {
            throw new MinioException(exception.getMessage());
        }
    }

    public ObjectWriteResponse storeStream(String bucket, String path, InputStream stream, Map<String, String> metadata, long totalSize) throws MinioException, IOException {
        try {
            return getClient().putObject(PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(path)
                    .userMetadata(metadata)
                    .stream(stream, totalSize, -1)
                    .build());
        } catch (ErrorResponseException | InsufficientDataException | InternalException |
                InvalidKeyException | InvalidResponseException | NoSuchAlgorithmException |
                ServerException | XmlParserException exception) {
            throw new MinioException(exception.getMessage());
        }
    }
}