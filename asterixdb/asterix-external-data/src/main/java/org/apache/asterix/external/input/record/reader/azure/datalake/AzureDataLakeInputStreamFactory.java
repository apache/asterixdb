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

import static org.apache.asterix.external.util.azure.blob_storage.AzureUtils.buildAzureDatalakeClient;
import static org.apache.asterix.external.util.azure.blob_storage.AzureUtils.listDatalakePathItems;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;

import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.PathItem;

public class AzureDataLakeInputStreamFactory extends AbstractExternalInputStreamFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public AsterixInputStream createInputStream(IExternalDataRuntimeContext context) throws HyracksDataException {
        IApplicationContext appCtx = (IApplicationContext) context.getTaskContext().getJobletContext()
                .getServiceContext().getApplicationContext();
        IExternalFilterValueEmbedder valueEmbedder = context.getValueEmbedder();
        int partition = context.getPartition();
        return new AzureDataLakeInputStream(appCtx, configuration,
                partitionWorkLoadsBasedOnSize.get(partition).getFilePaths(), valueEmbedder);
    }

    @Override
    public void configure(IServiceContext ctx, Map<String, String> configuration, IWarningCollector warningCollector,
            IExternalFilterEvaluatorFactory filterEvaluatorFactory) throws AlgebricksException, HyracksDataException {
        super.configure(ctx, configuration, warningCollector, filterEvaluatorFactory);

        IApplicationContext appCtx = (IApplicationContext) ctx.getApplicationContext();

        // get include/exclude matchers
        IncludeExcludeMatcher includeExcludeMatcher = ExternalDataUtils.getIncludeExcludeMatchers(configuration);

        DataLakeServiceClient client = buildAzureDatalakeClient(appCtx, configuration);
        List<PathItem> filesOnly =
                listDatalakePathItems(client, configuration, includeExcludeMatcher, warningCollector);

        // Distribute work load amongst the partitions
        distributeWorkLoad(filesOnly, getPartitionsCount());
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
