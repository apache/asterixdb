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
package org.apache.asterix.cloud.clients.google.gcs;

import static org.apache.asterix.external.util.google.gcs.GCSConstants.DEFAULT_NO_RETRY_ON_THREAD_INTERRUPT_STRATEGY;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.cloud.clients.IParallelDownloader;
import org.apache.asterix.cloud.clients.profiler.IRequestProfilerLimiter;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.hyracks.control.nc.io.IOManager;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.transfermanager.DownloadJob;
import com.google.cloud.storage.transfermanager.DownloadResult;
import com.google.cloud.storage.transfermanager.ParallelDownloadConfig;
import com.google.cloud.storage.transfermanager.TransferManager;
import com.google.cloud.storage.transfermanager.TransferManagerConfig;
import com.google.cloud.storage.transfermanager.TransferStatus;

public class GCSParallelDownloader implements IParallelDownloader {

    private final String bucket;
    private final IOManager ioManager;
    private final Storage gcsClient;
    private final TransferManager transferManager;
    private final IRequestProfilerLimiter profiler;
    private final GCSClientConfig config;

    public GCSParallelDownloader(String bucket, IOManager ioManager, GCSClientConfig config,
            IRequestProfilerLimiter profiler) throws HyracksDataException {
        this.bucket = bucket;
        this.ioManager = ioManager;
        this.profiler = profiler;
        StorageOptions.Builder builder = StorageOptions.newBuilder();
        builder.setStorageRetryStrategy(DEFAULT_NO_RETRY_ON_THREAD_INTERRUPT_STRATEGY);
        if (config.getEndpoint() != null && !config.getEndpoint().isEmpty()) {
            builder.setHost(config.getEndpoint());
        }
        builder.setCredentials(config.createCredentialsProvider());
        this.gcsClient = builder.build().getService();
        this.transferManager =
                TransferManagerConfig.newBuilder().setStorageOptions(builder.build()).build().getService();
        this.config = config;
    }

    @Override
    public void downloadFiles(Collection<FileReference> toDownload) throws HyracksDataException {
        ParallelDownloadConfig.Builder downConfig =
                ParallelDownloadConfig.newBuilder().setBucketName(bucket).setStripPrefix(this.config.getPrefix());

        Map<Path, List<BlobInfo>> pathListMap = new HashMap<>();
        try {
            for (FileReference fileReference : toDownload) {
                profiler.objectGet();
                FileUtils.createParentDirectories(fileReference.getFile());
                addToMap(pathListMap, fileReference.getDeviceHandle().getMount().toPath(), BlobInfo
                        .newBuilder(BlobId.of(bucket, config.getPrefix() + fileReference.getRelativePath())).build());
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        List<DownloadJob> downloadJobs = new ArrayList<>(pathListMap.size());
        for (Map.Entry<Path, List<BlobInfo>> entry : pathListMap.entrySet()) {
            downloadJobs.add(transferManager.downloadBlobs(entry.getValue(),
                    downConfig.setDownloadDirectory(entry.getKey()).build()));
        }
        downloadJobs.forEach(DownloadJob::getDownloadResults);
    }

    @Override
    public Collection<FileReference> downloadDirectories(Collection<FileReference> toDownload)
            throws HyracksDataException {
        Set<FileReference> failedFiles = new HashSet<>();
        ParallelDownloadConfig.Builder config =
                ParallelDownloadConfig.newBuilder().setBucketName(bucket).setStripPrefix(this.config.getPrefix());

        Map<Path, List<BlobInfo>> pathListMap = new HashMap<>();
        for (FileReference fileReference : toDownload) {
            profiler.objectMultipartDownload();
            Page<Blob> blobs = gcsClient.list(bucket,
                    Storage.BlobListOption.prefix(this.config.getPrefix() + fileReference.getRelativePath()));
            for (Blob blob : blobs.iterateAll()) {
                addToMap(pathListMap, fileReference.getDeviceHandle().getMount().toPath(), blob.asBlobInfo());
            }
        }
        List<DownloadJob> downloadJobs = new ArrayList<>(pathListMap.size());
        for (Map.Entry<Path, List<BlobInfo>> entry : pathListMap.entrySet()) {
            ParallelDownloadConfig parallelDownloadConfig = config.setDownloadDirectory(entry.getKey()).build();
            downloadJobs.add(transferManager.downloadBlobs(entry.getValue(), parallelDownloadConfig));
        }
        List<DownloadResult> results;
        for (DownloadJob job : downloadJobs) {
            results = job.getDownloadResults();
            for (DownloadResult result : results) {
                if (result.getStatus() != TransferStatus.SUCCESS) {
                    FileReference failedFile = ioManager.resolve(result.getInput().getName());
                    failedFiles.add(failedFile);
                }
            }
        }
        return failedFiles;
    }

    @Override
    public void close() throws HyracksDataException {
        InvokeUtil.tryWithCleanupsAsHyracks(transferManager::close, gcsClient::close);
    }

    private <K, V> void addToMap(Map<K, List<V>> map, K key, V value) {
        map.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
