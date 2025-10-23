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

package org.apache.asterix.cloud.clients.azure.blobstorage;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.cloud.clients.ICloudBufferedWriter;
import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.profiler.IRequestProfilerLimiter;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.implementation.Constants;

public class AzBlobStorageBufferedWriter implements ICloudBufferedWriter {
    private static final String PUT_UPLOAD_ID = "putUploadId";
    private static final int MAX_RETRIES = 3;
    private static final Logger LOGGER = LogManager.getLogger();
    private final List<String> blockIDArrayList;
    private final ICloudGuardian guardian;
    private final String path;
    private final AccessTier accessTier;
    private String uploadID;

    private final BlobContainerClient blobContainerClient;

    private final IRequestProfilerLimiter profiler;

    private final String bucket;

    public AzBlobStorageBufferedWriter(BlobContainerClient blobContainerClient, IRequestProfilerLimiter profiler,
            ICloudGuardian guardian, String bucket, String path, AccessTier accessTier) {
        this.blobContainerClient = blobContainerClient;
        this.profiler = profiler;
        this.guardian = guardian;
        this.bucket = bucket;
        this.path = path;
        this.blockIDArrayList = new ArrayList<>();
        this.accessTier = accessTier;
    }

    @Override
    public void upload(InputStream stream, int length) {
        profiler.objectMultipartUpload();
        if (length <= 0) {
            String errMsg = String.format("A block with size %d cannot be staged for upload", length);
            LOGGER.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }
        guardian.checkIsolatedWriteAccess(bucket, path);
        try {
            BlockBlobClient blockBlobClient = blobContainerClient.getBlobClient(path).getBlockBlobClient();
            BufferedInputStream bufferedInputStream = IOUtils.buffer(stream, length);
            String blockID =
                    Base64.getEncoder().encodeToString(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
            initBlockBlobUploads(blockID);
            blockIDArrayList.add(blockID);
            blockBlobClient.stageBlock(blockID, bufferedInputStream, length);
        } catch (Exception e) {
            LOGGER.error("Error while uploading blocks of data: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void initBlockBlobUploads(String blockID) {
        if (this.uploadID == null) {
            this.uploadID = blockID;
        }
    }

    @Override
    public void uploadLast(InputStream stream, ByteBuffer buffer) throws HyracksDataException {
        if (uploadID == null) {
            profiler.objectWrite();
            BlobClient blobClient = blobContainerClient.getBlobClient(path);
            BlobParallelUploadOptions options =
                    new BlobParallelUploadOptions(new ByteArrayInputStream(getDataFromBuffer(buffer)))
                            .setTier(accessTier);
            blobClient.uploadWithResponse(options, null, null);
            uploadID = PUT_UPLOAD_ID; // uploadID should be updated if the put-object operation succeeds
        } else {
            upload(stream, buffer.limit());
        }
    }

    private byte[] getDataFromBuffer(ByteBuffer buffer) {
        byte[] data = new byte[buffer.limit()];
        buffer.get(data, 0, buffer.limit());
        return data;
    }

    @Override
    public boolean isEmpty() {
        return this.uploadID == null;
    }

    @Override
    public void finish() throws HyracksDataException {
        if (this.uploadID == null) {
            throw new IllegalStateException("Cannot finish without writing any bytes");
        } else if (PUT_UPLOAD_ID.equals(uploadID)) {
            return;
        }
        int currRetryAttempt = 0;
        BlockBlobClient blockBlobClient = blobContainerClient.getBlobClient(path).getBlockBlobClient();
        while (true) {
            try {
                guardian.checkWriteAccess(bucket, path);
                profiler.objectMultipartUpload();
                blockBlobClient.commitBlockListWithResponse(blockIDArrayList, null, null, accessTier,
                        new BlobRequestConditions().setIfNoneMatch(Constants.HeaderConstants.ETAG_WILDCARD), null,
                        Context.NONE);
                break;
            } catch (BlobStorageException e) {
                currRetryAttempt++;
                if (currRetryAttempt == MAX_RETRIES) {
                    throw HyracksDataException.create(e);
                }
                LOGGER.info(() -> "AzBlob storage write retry, encountered: " + e.getMessage());

                // Backoff for 1 sec for the first 2 retries, and 2 seconds from there onward
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(currRetryAttempt < 2 ? 1 : 2));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw HyracksDataException.create(ex);
                }
            }
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        // Todo: As of the current Azure Java SDK, it does not support aborting a staged or under-upload block.
        // https://github.com/Azure/azure-sdk-for-java/issues/31150
        LOGGER.warn("Multipart upload for {} was aborted", path);
    }
}
