/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.control.nc.io.cloud.clients.aws.s3;

import static org.apache.hyracks.control.nc.io.cloud.clients.aws.s3.S3Utils.listS3Objects;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.control.nc.io.cloud.clients.ICloudBufferedWriter;
import org.apache.hyracks.control.nc.io.cloud.clients.ICloudClient;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3CloudClient implements ICloudClient {

    private static final String ACCESS_KEY_ID_FIELD = "accessKeyId";
    private static final String SECRET_ACCESS_KEY_FIELD = "secretAccessKey";
    private static final String REGION_FIELD = "region";
    private final static String ENDPOINT_FIELD = "endpoint";

    private final S3Client s3Client;

    // TODO fix the throws exception
    public S3CloudClient(Map<String, String> clientConfiguration) throws HyracksDataException {
        setClientConfig(clientConfiguration); // TODO: remove later, this is temporary
        s3Client = buildClient(clientConfiguration);
    }

    private S3Client buildClient(Map<String, String> clientConfiguration) throws HyracksDataException {
        String accessKeyId = clientConfiguration.get(ACCESS_KEY_ID_FIELD);
        String secretAccessKey = clientConfiguration.get(SECRET_ACCESS_KEY_FIELD);
        String region = clientConfiguration.get(REGION_FIELD);
        String endpoint = clientConfiguration.get(ENDPOINT_FIELD);

        AwsCredentialsProvider credentialsProvider =
                StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey));
        S3ClientBuilder builder = S3Client.builder();
        builder.credentialsProvider(credentialsProvider);
        builder.region(Region.of(region));

        if (endpoint != null) {
            try {
                URI uri = new URI(endpoint);
                builder.endpointOverride(uri);
            } catch (Exception ex) {
                throw HyracksDataException.create(ex);
            }
        }
        return builder.build();
    }

    // TODO: temporarily setting the client config, this should be provided
    private void setClientConfig(Map<String, String> clientConfiguration) throws HyracksDataException {
        if (!clientConfiguration.isEmpty()) {
            return;
        }

        try {
            List<String> lines = FileUtils.readLines(new File("/etc/s3"), "UTF-8");
            String accessKeyId = lines.get(1);
            String secretAccessKey = lines.get(2);
            String region = lines.get(3);

            clientConfiguration.put(ACCESS_KEY_ID_FIELD, accessKeyId);
            clientConfiguration.put(SECRET_ACCESS_KEY_FIELD, secretAccessKey);
            clientConfiguration.put(REGION_FIELD, region);

            if (lines.size() > 4) {
                String serviceEndpoint = lines.get(4);
                clientConfiguration.put(ENDPOINT_FIELD, serviceEndpoint);
            }
        } catch (IOException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    @Override
    public ICloudBufferedWriter createBufferedWriter(String bucket, String path) {
        return new S3BufferedWriter(s3Client, bucket, path);
    }

    @Override
    public Set<String> listObjects(String bucket, String path, FilenameFilter filter) {
        return filterAndGet(listS3Objects(s3Client, bucket, path), filter);
    }

    @Override
    public int read(String bucket, String path, long offset, ByteBuffer buffer) throws HyracksDataException {
        long readTo = offset + buffer.remaining();
        GetObjectRequest rangeGetObjectRequest =
                GetObjectRequest.builder().range("bytes=" + offset + "-" + readTo).bucket(bucket).key(path).build();

        int totalRead = 0;
        int read = 0;

        // TODO(htowaileb): add retry logic here
        try (ResponseInputStream<GetObjectResponse> response = s3Client.getObject(rangeGetObjectRequest)) {
            while (buffer.remaining() > 0) {
                read = response.read(buffer.array(), buffer.position(), buffer.remaining());
                buffer.position(buffer.position() + read);
                totalRead += read;
            }
        } catch (IOException ex) {
            throw HyracksDataException.create(ex);
        }

        if (buffer.remaining() != 0) {
            throw new IllegalStateException("Expected buffer remaining = 0, found: " + buffer.remaining());
        }
        return totalRead;
    }

    @Override
    public byte[] readAllBytes(String bucket, String path) throws HyracksDataException {
        GetObjectRequest getReq = GetObjectRequest.builder().bucket(bucket).key(path).build();
        try {
            ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(getReq);
            return stream.readAllBytes();
        } catch (NoSuchKeyException e) {
            return null;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public InputStream getObjectStream(String bucket, String path) {
        GetObjectRequest getReq = GetObjectRequest.builder().bucket(bucket).key(path).build();
        try {
            return s3Client.getObject(getReq);
        } catch (NoSuchKeyException e) {
            // This should not happen at least from the only caller of this method
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void write(String bucket, String path, byte[] data) {
        PutObjectRequest putReq = PutObjectRequest.builder().bucket(bucket).key(path).build();

        // TODO(htowaileb): add retry logic here
        s3Client.putObject(putReq, RequestBody.fromBytes(data));
    }

    @Override
    public void copy(String bucket, String srcPath, FileReference destPath) {
        List<S3Object> objects = listS3Objects(s3Client, bucket, srcPath);
        for (S3Object object : objects) {
            String srcKey = object.key();
            String destKey = destPath.getChildPath(IoUtil.getFileNameFromPath(srcKey));
            CopyObjectRequest copyReq = CopyObjectRequest.builder().sourceBucket(bucket).sourceKey(srcKey)
                    .destinationBucket(bucket).destinationKey(destKey).build();
            s3Client.copyObject(copyReq);
        }
    }

    @Override
    public void deleteObject(String bucket, String path) {
        Set<String> fileList = listObjects(bucket, path, IoUtil.NO_OP_FILTER);
        if (fileList.isEmpty()) {
            return;
        }

        List<ObjectIdentifier> objectIdentifiers = new ArrayList<>();
        for (String file : fileList) {
            objectIdentifiers.add(ObjectIdentifier.builder().key(file).build());
        }
        Delete delete = Delete.builder().objects(objectIdentifiers).build();
        DeleteObjectsRequest deleteReq = DeleteObjectsRequest.builder().bucket(bucket).delete(delete).build();
        s3Client.deleteObjects(deleteReq);
    }

    @Override
    public long getObjectSize(String bucket, String path) {
        List<S3Object> objects = listS3Objects(s3Client, bucket, path);
        if (objects.isEmpty()) {
            return 0;
        }
        return objects.get(0).size();
    }

    @Override
    public boolean exists(String bucket, String path) {
        List<S3Object> objects = listS3Objects(s3Client, bucket, path);
        return !objects.isEmpty();
    }

    private Set<String> filterAndGet(List<S3Object> contents, FilenameFilter filter) {
        Set<String> files = new HashSet<>();
        for (S3Object s3Object : contents) {
            String path = s3Object.key();
            if (filter.accept(null, IoUtil.getFileNameFromPath(path))) {
                files.add(path);
            }
        }
        return files;
    }
}
