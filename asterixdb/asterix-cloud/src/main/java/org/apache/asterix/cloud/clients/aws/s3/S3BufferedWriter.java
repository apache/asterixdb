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
package org.apache.asterix.cloud.clients.aws.s3;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.cloud.clients.ICloudBufferedWriter;
import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.profiler.IRequestProfilerLimiter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.cloud.util.CloudRetryableRequestUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

public class S3BufferedWriter implements ICloudBufferedWriter {
    private static final String PUT_UPLOAD_ID = "putUploadId";

    private static final Logger LOGGER = LogManager.getLogger();
    private final S3Client s3Client;
    private final IRequestProfilerLimiter profiler;
    private final ICloudGuardian guardian;
    private final String bucket;
    private final String path;
    private final List<CompletedPart> partQueue;

    private String uploadId;
    private int partNumber;

    public S3BufferedWriter(S3Client s3client, IRequestProfilerLimiter profiler, ICloudGuardian guardian, String bucket,
            String path) {
        this.s3Client = s3client;
        this.profiler = profiler;
        this.guardian = guardian;
        this.bucket = bucket;
        this.path = path;
        partQueue = new ArrayList<>();
    }

    @Override
    public void upload(InputStream stream, int length) {
        guardian.checkIsolatedWriteAccess(bucket, path);
        profiler.objectMultipartUpload();
        setUploadId();
        UploadPartRequest upReq =
                UploadPartRequest.builder().uploadId(uploadId).partNumber(partNumber).bucket(bucket).key(path).build();
        String etag = s3Client.uploadPart(upReq, RequestBody.fromInputStream(stream, length)).eTag();
        partQueue.add(CompletedPart.builder().partNumber(partNumber).eTag(etag).build());
        partNumber++;
    }

    @Override
    public void uploadLast(InputStream stream, ByteBuffer buffer) {
        if (uploadId == null) {
            profiler.objectWrite();
            PutObjectRequest request = PutObjectRequest.builder().bucket(bucket).key(path).build();
            s3Client.putObject(request, RequestBody.fromByteBuffer(buffer));
            // Only set the uploadId if the putObject succeeds
            uploadId = PUT_UPLOAD_ID;
        } else {
            upload(stream, buffer.limit());
        }
    }

    @Override
    public boolean isEmpty() {
        return uploadId == null;
    }

    @Override
    public void finish() throws HyracksDataException {
        if (uploadId == null) {
            throw new IllegalStateException("Cannot finish without writing any bytes");
        } else if (PUT_UPLOAD_ID.equals(uploadId)) {
            LOGGER.debug("FINISHED multipart upload as PUT for {}", path);
            return;
        }

        // Finishing a multipart file. Proceed with completing the multipart upload
        CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(partQueue).build();
        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucket).key(path).uploadId(uploadId).multipartUpload(completedMultipartUpload).build();
        // This will be interrupted and the interruption will be followed by a halt
        CloudRetryableRequestUtil
                .runWithNoRetryOnInterruption(() -> completeMultipartUpload(completeMultipartUploadRequest));
        log("FINISHED");
    }

    @Override
    public void abort() throws HyracksDataException {
        if (uploadId == null || PUT_UPLOAD_ID.equals(uploadId)) {
            return;
        }
        s3Client.abortMultipartUpload(
                AbortMultipartUploadRequest.builder().bucket(bucket).key(path).uploadId(uploadId).build());
        LOGGER.warn("Multipart upload for {} was aborted", path);
    }

    private void completeMultipartUpload(CompleteMultipartUploadRequest request) throws HyracksDataException {
        guardian.checkWriteAccess(bucket, path);
        profiler.objectMultipartUpload();
        try {
            s3Client.completeMultipartUpload(request);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private void setUploadId() {
        if (uploadId == null) {
            CreateMultipartUploadRequest uploadRequest =
                    CreateMultipartUploadRequest.builder().bucket(bucket).key(path).build();
            CreateMultipartUploadResponse uploadResp = s3Client.createMultipartUpload(uploadRequest);
            uploadId = uploadResp.uploadId();
            partNumber = 1;
            log("STARTED");
        }
    }

    private void log(String op) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} multipart upload for {}", op, path);
        }
    }
}
