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

import static org.apache.asterix.cloud.clients.azure.blobstorage.AzBlobStorageClientConfig.DELETE_BATCH_SIZE;

import java.io.ByteArrayOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.cloud.CloudResettableInputStream;
import org.apache.asterix.cloud.IWriteBufferProvider;
import org.apache.asterix.cloud.clients.CloudFile;
import org.apache.asterix.cloud.clients.ICloudBufferedWriter;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.ICloudWriter;
import org.apache.asterix.cloud.clients.IParallelDownloader;
import org.apache.asterix.cloud.clients.profiler.CountRequestProfilerLimiter;
import org.apache.asterix.cloud.clients.profiler.IRequestProfilerLimiter;
import org.apache.asterix.cloud.clients.profiler.RequestLimiterNoOpProfiler;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AzBlobStorageCloudClient implements ICloudClient {
    private static final String BUCKET_ROOT_PATH = "";
    public static final String AZURITE_ENDPOINT = "http://127.0.0.1:15055/devstoreaccount1/";
    private static final String AZURITE_ACCOUNT_NAME = "devstoreaccount1";
    private static final String AZURITE_ACCOUNT_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    private final ICloudGuardian guardian;
    private BlobContainerClient blobContainerClient;
    private AzBlobStorageClientConfig config;
    private IRequestProfilerLimiter profiler;
    private final BlobBatchClient blobBatchClient;
    private static final Logger LOGGER = LogManager.getLogger();

    public AzBlobStorageCloudClient(AzBlobStorageClientConfig config, ICloudGuardian guardian) {
        this(config, buildClient(config), guardian);
    }

    public AzBlobStorageCloudClient(AzBlobStorageClientConfig config, BlobContainerClient blobContainerClient,
            ICloudGuardian guardian) {
        this.blobContainerClient = blobContainerClient;
        this.config = config;
        this.guardian = guardian;
        long profilerInterval = config.getProfilerLogInterval();
        AzureRequestRateLimiter limiter = new AzureRequestRateLimiter(config);
        if (profilerInterval > 0) {
            profiler = new CountRequestProfilerLimiter(profilerInterval, limiter);
        } else {
            profiler = new RequestLimiterNoOpProfiler(limiter);
        }
        guardian.setCloudClient(this);
        blobBatchClient = new BlobBatchClientBuilder(blobContainerClient.getServiceClient()).buildClient();
    }

    @Override
    public int getWriteBufferSize() {
        return config.getWriteBufferSize();
    }

    @Override
    public IRequestProfilerLimiter getProfilerLimiter() {
        return profiler;
    }

    @Override
    public ICloudWriter createWriter(String bucket, String path, IWriteBufferProvider bufferProvider) {
        ICloudBufferedWriter bufferedWriter = new AzBlobStorageBufferedWriter(blobContainerClient, profiler, guardian,
                bucket, config.getPrefix() + path);
        return new CloudResettableInputStream(bufferedWriter, bufferProvider);
    }

    @Override
    public Set<CloudFile> listObjects(String bucket, String path, FilenameFilter filter) {
        guardian.checkReadAccess(bucket, path);
        profiler.objectsList();
        PagedIterable<BlobItem> blobItems = getBlobItems(bucket, config.getPrefix() + path);
        Stream<CloudFile> cloudFileStream = mapBlobItemsToStreamOfCloudFiles(blobItems);
        return filterCloudFiles(filter, cloudFileStream);
    }

    private Set<CloudFile> filterCloudFiles(FilenameFilter filter, Stream<CloudFile> cloudFileStream) {
        if (filter == null) {
            return cloudFileStream.map(this::removeCloudPrefixFromBlobName).collect(Collectors.toSet());
        }
        return cloudFileStream.filter(cloudFile -> filter.accept(null, cloudFile.getPath()))
                .map(this::removeCloudPrefixFromBlobName).collect(Collectors.toSet());
    }

    private CloudFile removeCloudPrefixFromBlobName(CloudFile cloudFile) {
        String fullyQualifiedBlobName = cloudFile.getPath();
        fullyQualifiedBlobName = fullyQualifiedBlobName.substring(config.getPrefix().length());
        return CloudFile.of(fullyQualifiedBlobName, cloudFile.getSize());
    }

    private Stream<CloudFile> mapBlobItemsToStreamOfCloudFiles(PagedIterable<BlobItem> blobItems) {
        return blobItems.stream()
                .map(blobItem -> CloudFile.of(blobItem.getName(), blobItem.getProperties().getContentLength()));
    }

    private PagedIterable<BlobItem> getBlobItems(String bucket, String path) {
        ListBlobsOptions options =
                new ListBlobsOptions().setPrefix(path).setDetails(new BlobListDetails().setRetrieveMetadata(true));
        return blobContainerClient.listBlobs(options, null);
    }

    @Override
    public int read(String bucket, String path, long offset, ByteBuffer buffer) throws HyracksDataException {
        guardian.checkReadAccess(bucket, path);
        profiler.objectGet();
        BlobClient blobClient = blobContainerClient.getBlobClient(config.getPrefix() + path);
        ByteArrayOutputStream blobStream = new ByteArrayOutputStream(buffer.capacity());
        long rem = buffer.remaining();
        BlobRange blobRange = new BlobRange(offset, rem);
        downloadBlob(blobClient, blobStream, blobRange);
        readBlobStreamIntoBuffer(buffer, blobStream);
        if (buffer.remaining() != 0)
            throw new IllegalStateException("Expected buffer remaining = 0, found: " + buffer.remaining());
        return ((int) rem - buffer.remaining());
    }

    private void readBlobStreamIntoBuffer(ByteBuffer buffer, ByteArrayOutputStream byteArrayOutputStream)
            throws HyracksDataException {
        byte[] byteArray = byteArrayOutputStream.toByteArray();
        try {
            buffer.put(byteArray);
            byteArrayOutputStream.close();
        } catch (BufferOverflowException | ReadOnlyBufferException | IOException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    private void downloadBlob(BlobClient blobClient, ByteArrayOutputStream byteArrayOutputStream, BlobRange blobRange)
            throws HyracksDataException {
        try {
            blobClient.downloadStreamWithResponse(byteArrayOutputStream, blobRange, null, null, false, null, null);
        } catch (BlobStorageException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    @Override
    public byte[] readAllBytes(String bucket, String path) throws HyracksDataException {
        guardian.checkReadAccess(bucket, path);
        profiler.objectGet();
        BlobClient blobClient = blobContainerClient.getBlobClient(config.getPrefix() + path);
        try {
            BinaryData binaryData = blobClient.downloadContent();
            return binaryData.toBytes();
        } catch (BlobStorageException ex) {
            BlobErrorCode errorCode = ex.getErrorCode();
            if (errorCode.equals(BlobErrorCode.BLOB_NOT_FOUND)) {
                LOGGER.warn("Blob not found on cloud: {}", path);
                return null;
            }
            throw HyracksDataException.create(ex);
        }
    }

    @Override
    public InputStream getObjectStream(String bucket, String path, long offset, long length) {
        guardian.checkReadAccess(bucket, path);
        profiler.objectGet();
        BlobRange blobRange = new BlobRange(offset, length);
        BlobClient blobClient = blobContainerClient.getBlobClient(config.getPrefix() + path);
        try {
            return blobClient.openInputStream(blobRange, null);
        } catch (BlobStorageException ex) {
            LOGGER.error("error getting object stream for path: {}. Exception: {}", path, ex.getMessage());
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void write(String bucket, String path, byte[] data) {
        guardian.checkWriteAccess(bucket, path);
        profiler.objectWrite();
        BinaryData binaryData = BinaryData.fromBytes(data);
        BlobClient blobClient = blobContainerClient.getBlobClient(config.getPrefix() + path);
        blobClient.upload(binaryData, true);
    }

    @Override
    public void copy(String bucket, String srcPath, FileReference destPath) {
        guardian.checkReadAccess(bucket, srcPath);
        profiler.objectGet();
        BlobClient srcBlobClient = blobContainerClient.getBlobClient(config.getPrefix() + srcPath);
        String srcBlobUrl = srcBlobClient.getBlobUrl();
        profiler.objectCopy();
        guardian.checkWriteAccess(bucket, destPath.getRelativePath());
        BlobClient destBlobClient = blobContainerClient.getBlobClient(destPath.getFile().getPath());
        destBlobClient.beginCopy(srcBlobUrl, null);
    }

    @Override
    public void deleteObjects(String bucket, Collection<String> paths) {
        if (paths.isEmpty())
            return;
        Set<BlobItem> blobsToDelete = getBlobsMatchingThesePaths(paths);
        List<String> blobURLs = getBlobURLs(blobsToDelete);
        if (blobURLs.isEmpty())
            return;
        Collection<List<String>> batchedBlobURLs = getBatchedBlobURLs(blobURLs);
        for (List<String> batch : batchedBlobURLs) {
            blobBatchClient.deleteBlobs(batch, null).stream().count();
        }
    }

    private Collection<List<String>> getBatchedBlobURLs(List<String> blobURLs) {
        int startIdx = 0;
        Collection<List<String>> batchedBLOBURLs = new ArrayList<>();
        Iterator<String> iterator = blobURLs.iterator();
        while (iterator.hasNext()) {
            List<String> batch = new ArrayList<>();
            while (startIdx < DELETE_BATCH_SIZE && iterator.hasNext()) {
                batch.add(iterator.next());
                startIdx++;
            }
            batchedBLOBURLs.add(batch);
            startIdx = 0;
        }
        return batchedBLOBURLs;
    }

    private Set<BlobItem> getBlobsMatchingThesePaths(Collection<String> paths) {
        List<String> pathWithPrefix =
                paths.stream().map(path -> config.getPrefix() + path).collect(Collectors.toList());
        PagedIterable<BlobItem> blobItems = blobContainerClient.listBlobs();
        return blobItems.stream().filter(blobItem -> pathWithPrefix.contains(blobItem.getName()))
                .collect(Collectors.toSet());
    }

    @Override
    public long getObjectSize(String bucket, String path) throws HyracksDataException {
        guardian.checkReadAccess(bucket, path);
        profiler.objectGet();
        try {
            BlobClient blobClient = blobContainerClient.getBlobClient(config.getPrefix() + path);
            return blobClient.getProperties().getBlobSize();
        } catch (BlobStorageException ex) {
            BlobErrorCode errorCode = ex.getErrorCode();
            if (errorCode.equals(BlobErrorCode.BLOB_NOT_FOUND)) {
                LOGGER.error("error while getting blob size; no such blob found: {} ", config.getPrefix() + path);
                return 0;
            }
            throw HyracksDataException.create(ex);
        } catch (Exception ex) {
            LOGGER.error("error getting size of the blob: {}. Exception: {}", path, ex.getMessage());
            throw HyracksDataException.create(ex);
        }
    }

    @Override
    public boolean exists(String bucket, String path) throws HyracksDataException {
        guardian.checkReadAccess(bucket, path);
        profiler.objectGet();
        try {
            BlobClient blobClient = blobContainerClient.getBlobClient(config.getPrefix() + path);
            return blobClient.exists();
        } catch (BlobStorageException ex) {
            BlobErrorCode errorCode = ex.getErrorCode();
            if (errorCode.equals(BlobErrorCode.BLOB_NOT_FOUND)) {
                return false;
            }
            throw HyracksDataException.create(ex);
        } catch (Exception ex) {
            throw HyracksDataException.create(ex);
        }
    }

    @Override
    public boolean isEmptyPrefix(String bucket, String path) throws HyracksDataException {
        profiler.objectsList();
        ListBlobsOptions listBlobsOptions = new ListBlobsOptions().setPrefix(config.getPrefix() + path);
        //MAX_VALUE below represents practically no timeout
        PagedIterable<BlobItem> blobItems =
                blobContainerClient.listBlobs(listBlobsOptions, Duration.ofDays(Long.MAX_VALUE));
        return blobItems.stream().findAny().isEmpty();
    }

    @Override
    public IParallelDownloader createParallelDownloader(String bucket, IOManager ioManager) {
        return new AzureParallelDownloader(ioManager, blobContainerClient, profiler, config);
    }

    @Override
    public JsonNode listAsJson(ObjectMapper objectMapper, String bucket) {
        profiler.objectsList();
        PagedIterable<BlobItem> blobItems = getBlobItems(bucket, BUCKET_ROOT_PATH);
        List<BlobItem> blobs = blobItems.stream().distinct().collect(Collectors.toList());
        blobs = sortBlobItemsByName(blobs);
        return mapBlobItemsToJson(blobs, objectMapper);
    }

    private List<BlobItem> sortBlobItemsByName(List<BlobItem> blobs) {
        return blobs.stream()
                .sorted((blob1, blob2) -> String.CASE_INSENSITIVE_ORDER.compare(blob1.getName(), blob2.getName()))
                .collect(Collectors.toList());
    }

    private ArrayNode mapBlobItemsToJson(List<BlobItem> blobs, ObjectMapper objectMapper) {
        ArrayNode objectsInfo = objectMapper.createArrayNode();
        for (BlobItem blob : blobs) {
            ObjectNode objectInfo = objectsInfo.addObject();
            objectInfo.put("path", blob.getName());
            objectInfo.put("size", blob.getProperties().getContentLength());
        }
        return objectsInfo;
    }

    @Override
    public void close() {
        // Closing Azure Blob Clients is not required as the underlying netty connection pool
        // handles the same for the apps.
        // Ref: https://github.com/Azure/azure-sdk-for-java/issues/17903
        // Hence this implementation is a no op.
    }

    private static BlobContainerClient buildClient(AzBlobStorageClientConfig config) {
        BlobContainerClientBuilder blobContainerClientBuilder =
                new BlobContainerClientBuilder().containerName(config.getBucket()).endpoint(getEndpoint(config));
        configCredentialsToAzClient(blobContainerClientBuilder, config);
        BlobContainerClient blobContainerClient = blobContainerClientBuilder.buildClient();
        blobContainerClient.createIfNotExists();
        return blobContainerClient;
    }

    private static void configCredentialsToAzClient(BlobContainerClientBuilder builder,
            AzBlobStorageClientConfig config) {
        if (config.isAnonymousAuth()) {
            StorageSharedKeyCredential creds =
                    new StorageSharedKeyCredential(AZURITE_ACCOUNT_NAME, AZURITE_ACCOUNT_KEY);
            builder.credential(creds);
        } else {
            builder.credential(config.createCredentialsProvider());
        }
    }

    private static String getEndpoint(AzBlobStorageClientConfig config) {
        return config.isAnonymousAuth() ? AZURITE_ENDPOINT + config.getBucket()
                : config.getEndpoint() + "/" + config.getBucket();
    }

    private List<String> getBlobURLs(Set<BlobItem> blobs) {
        final String blobURLPrefix = blobContainerClient.getBlobContainerUrl() + "/";
        return blobs.stream().map(BlobItem::getName).map(blobName -> blobURLPrefix + blobName)
                .collect(Collectors.toList());
    }
}
