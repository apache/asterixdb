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
package org.apache.asterix.external.input.record.reader.azure.datalake;

import static org.apache.asterix.external.util.ExternalDataConstants.Azure.RECURSIVE_FIELD_NAME;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;

public class AzureDataLakeInputStreamFactory extends AbstractExternalInputStreamFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public AsterixInputStream createInputStream(IHyracksTaskContext ctx, int partition) throws HyracksDataException {
        return new AzureDataLakeInputStream(configuration, partitionWorkLoadsBasedOnSize.get(partition).getFilePaths());
    }

    @Override
    public void configure(IServiceContext ctx, Map<String, String> configuration, IWarningCollector warningCollector)
            throws AlgebricksException {
        super.configure(ctx, configuration, warningCollector);

        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);

        List<PathItem> filesOnly = new ArrayList<>();

        // Ensure the validity of include/exclude
        ExternalDataUtils.validateIncludeExclude(configuration);

        DataLakeServiceClient client = ExternalDataUtils.Azure.buildAzureDatalakeClient(configuration);
        DataLakeFileSystemClient fileSystemClient;
        try {
            fileSystemClient = client.getFileSystemClient(container);

            // Get all objects in a container and extract the paths to files
            ListPathsOptions listOptions = new ListPathsOptions();
            boolean recursive = Boolean.parseBoolean(configuration.get(RECURSIVE_FIELD_NAME));
            listOptions.setRecursive(recursive);
            listOptions.setPath(ExternalDataUtils.getPrefix(configuration, false));
            PagedIterable<PathItem> pathItems = fileSystemClient.listPaths(listOptions, null);

            // Collect the paths to files only
            IncludeExcludeMatcher includeExcludeMatcher = ExternalDataUtils.getIncludeExcludeMatchers(configuration);
            collectAndFilterFiles(pathItems, includeExcludeMatcher.getPredicate(),
                    includeExcludeMatcher.getMatchersList(), filesOnly);

            // Warn if no files are returned
            if (filesOnly.isEmpty() && warningCollector.shouldWarn()) {
                Warning warning = Warning.of(null, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                warningCollector.warn(warning);
            }

            // Distribute work load amongst the partitions
            distributeWorkLoad(filesOnly, getPartitionsCount());
        } catch (Exception ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
        }
    }

    /**
     * Collects and filters the files only, and excludes any folders
     *
     * @param items     storage items
     * @param predicate predicate to test with for file filtration
     * @param matchers  include/exclude matchers to test against
     * @param filesOnly List containing the files only (excluding folders)
     */
    private void collectAndFilterFiles(Iterable<PathItem> items, BiPredicate<List<Matcher>, String> predicate,
            List<Matcher> matchers, List<PathItem> filesOnly) {
        for (PathItem item : items) {
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
     * <p>
     * Example:
     * File1 1mb, File2 300kb, File3 300kb, File4 300kb
     * <p>
     * Distribution:
     * Partition1: [File1]
     * Partition2: [File2, File3, File4]
     *
     * @param items           items
     * @param partitionsCount Partitions count
     */
    private void distributeWorkLoad(List<PathItem> items, int partitionsCount) {
        PriorityQueue<PartitionWorkLoadBasedOnSize> workloadQueue = new PriorityQueue<>(partitionsCount,
                Comparator.comparingLong(PartitionWorkLoadBasedOnSize::getTotalSize));

        // Prepare the workloads based on the number of partitions
        for (int i = 0; i < partitionsCount; i++) {
            workloadQueue.add(new PartitionWorkLoadBasedOnSize());
        }

        for (PathItem object : items) {
            PartitionWorkLoadBasedOnSize workload = workloadQueue.poll();
            workload.addFilePath(object.getName(), object.getContentLength());
            workloadQueue.add(workload);
        }
        partitionWorkLoadsBasedOnSize.addAll(workloadQueue);
    }
}
