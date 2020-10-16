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
package org.apache.asterix.external.input.record.reader.azure;

import static org.apache.asterix.external.util.ExternalDataConstants.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.WarningUtil;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;

public class AzureBlobInputStreamFactory extends AbstractExternalInputStreamFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public AsterixInputStream createInputStream(IHyracksTaskContext ctx, int partition) throws HyracksDataException {
        return new AzureBlobInputStream(configuration, partitionWorkLoadsBasedOnSize.get(partition).getFilePaths());
    }

    @Override
    public void configure(IServiceContext ctx, Map<String, String> configuration, IWarningCollector warningCollector)
            throws AlgebricksException {
        this.configuration = configuration;
        ICcApplicationContext ccApplicationContext = (ICcApplicationContext) ctx.getApplicationContext();

        String container = configuration.get(AzureBlob.CONTAINER_NAME_FIELD_NAME);

        List<BlobItem> filesOnly = new ArrayList<>();

        // Ensure the validity of include/exclude
        ExternalDataUtils.validateIncludeExclude(configuration);

        BlobServiceClient blobServiceClient = ExternalDataUtils.Azure.buildAzureClient(configuration);
        BlobContainerClient blobContainer;
        try {
            blobContainer = blobServiceClient.getBlobContainerClient(container);

            // Get all objects in a container and extract the paths to files
            ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
            listBlobsOptions.setPrefix(ExternalDataUtils.getPrefix(configuration));
            Iterable<BlobItem> blobItems = blobContainer.listBlobs(listBlobsOptions, null);

            // Collect the paths to files only
            IncludeExcludeMatcher includeExcludeMatcher = getIncludeExcludeMatchers();
            collectAndFilterFiles(blobItems, includeExcludeMatcher.getPredicate(),
                    includeExcludeMatcher.getMatchersList(), filesOnly);

            // Warn if no files are returned
            if (filesOnly.isEmpty() && warningCollector.shouldWarn()) {
                Warning warning =
                        WarningUtil.forAsterix(null, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                warningCollector.warn(warning);
            }

            // Partition constraints
            partitionConstraint = ccApplicationContext.getClusterStateManager().getClusterLocations();
            int partitionsCount = partitionConstraint.getLocations().length;

            // Distribute work load amongst the partitions
            distributeWorkLoad(filesOnly, partitionsCount);
        } catch (Exception ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex.getMessage());
        }
    }

    /**
     * Collects and filters the files only, and excludes any folders
     *
     * @param items storage items
     * @param predicate predicate to test with for file filtration
     * @param matchers include/exclude matchers to test against
     * @param filesOnly List containing the files only (excluding folders)
     */
    private void collectAndFilterFiles(Iterable<BlobItem> items, BiPredicate<List<Matcher>, String> predicate,
            List<Matcher> matchers, List<BlobItem> filesOnly) {
        for (BlobItem item : items) {
            String uri = item.getName();

            // skip folders
            if (uri.endsWith("/")) {
                continue;
            }

            // No filter, add file
            if (predicate.test(matchers, uri)) {
                filesOnly.add(item);
            }
        }
    }

    /**
     * To efficiently utilize the parallelism, work load will be distributed amongst the partitions based on the file
     * size.
     *
     * Example:
     * File1 1mb, File2 300kb, File3 300kb, File4 300kb
     *
     * Distribution:
     * Partition1: [File1]
     * Partition2: [File2, File3, File4]
     *
     * @param items items
     * @param partitionsCount Partitions count
     */
    private void distributeWorkLoad(List<BlobItem> items, int partitionsCount) {
        // Prepare the workloads based on the number of partitions
        for (int i = 0; i < partitionsCount; i++) {
            partitionWorkLoadsBasedOnSize.add(new PartitionWorkLoadBasedOnSize());
        }

        for (BlobItem item : items) {
            PartitionWorkLoadBasedOnSize smallest = getSmallestWorkLoad();
            smallest.addFilePath(item.getName(), item.getProperties().getContentLength());
        }
    }
}
