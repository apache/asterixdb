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
package org.apache.asterix.external.input.record.reader.aws;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Supplier;

import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataPrefix;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.aws.s3.S3Utils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;

import software.amazon.awssdk.services.s3.model.S3Object;

public class AwsS3InputStreamFactory extends AbstractExternalInputStreamFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public AsterixInputStream createInputStream(IHyracksTaskContext ctx, int partition) throws HyracksDataException {
        return new AwsS3InputStream(configuration, partitionWorkLoadsBasedOnSize.get(partition).getFilePaths());
    }

    @Override
    public void configure(IServiceContext ctx, Map<String, String> configuration, IWarningCollector warningCollector,
            IExternalFilterEvaluatorFactory filterEvaluatorFactory) throws AlgebricksException {
        super.configure(ctx, configuration, warningCollector, filterEvaluatorFactory);

        // Ensure the validity of include/exclude
        ExternalDataUtils.validateIncludeExclude(configuration);
        IncludeExcludeMatcher includeExcludeMatcher = ExternalDataUtils.getIncludeExcludeMatchers(configuration);

        //Get a list of S3 objects
        String prefix = configuration.get(ExternalDataConstants.DEFINITION_FIELD_NAME);
        ExternalDataPrefix externalDataPrefix = new ExternalDataPrefix(prefix);
        configuration.put(ExternalDataPrefix.PREFIX_ROOT_FIELD_NAME, externalDataPrefix.getRoot());

        // TODO(htowaileb): Since we're using the root to load the files then start filtering, it might end up being
        // very expensive since at the root of the prefix we might load millions of files, we should consider (when
        // possible) to get the value and add it
        List<S3Object> filesOnly = S3Utils.listS3Objects(configuration, includeExcludeMatcher, warningCollector);

        filesOnly = filterPrefixes(externalDataPrefix, filesOnly, () -> true);

        // Distribute work load amongst the partitions
        distributeWorkLoad(filesOnly, getPartitionsCount());
    }

    private List<S3Object> filterPrefixes(ExternalDataPrefix prefix, List<S3Object> filesOnly,
            Supplier<Boolean> evaluator) {

        // if no computed fields, return the original list
        if (prefix.getComputedFieldDetails().isEmpty()) {
            return filesOnly;
        }

        List<S3Object> filteredList = new ArrayList<>();
        for (S3Object file : filesOnly) {
            List<String> segments = ExternalDataPrefix.getPrefixSegments(file.key());
            boolean match = false;

            // if the object key has fewer segments than the expected prefix, then filter it out
            // TODO(htowaileb): potentially also exclude if the size matches, key should be longer than prefix
            if (segments.size() < prefix.getComputedFieldDetails().getComputedFieldNames().size()) {
                continue;
            }

            for (int i = 0; i < prefix.getComputedFieldDetails().getComputedFieldNames().size(); i++) {
                int index = prefix.getComputedFieldDetails().getComputedFieldIndexes().get(i);

                // TODO(htowaileb): evaluator will container an expression that evaluates whether to include an object or not
                match = evaluator.get();
                if (!match) {
                    break;
                }
            }

            if (match) {
                filteredList.add(file);
            }
        }

        return filteredList;
    }

    private ARecordType createRecord(String[] fieldNames, IAType[] fieldTypes) {
        return new ARecordType("root", fieldNames, fieldTypes, false);
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
     * @param fileObjects     AWS S3 file objects
     * @param partitionsCount Partitions count
     */
    private void distributeWorkLoad(List<S3Object> fileObjects, int partitionsCount) {
        PriorityQueue<PartitionWorkLoadBasedOnSize> workloadQueue = new PriorityQueue<>(partitionsCount,
                Comparator.comparingLong(PartitionWorkLoadBasedOnSize::getTotalSize));

        // Prepare the workloads based on the number of partitions
        for (int i = 0; i < partitionsCount; i++) {
            workloadQueue.add(new PartitionWorkLoadBasedOnSize());
        }

        for (S3Object object : fileObjects) {
            PartitionWorkLoadBasedOnSize workload = workloadQueue.poll();
            workload.addFilePath(object.key(), object.size());
            workloadQueue.add(workload);
        }
        partitionWorkLoadsBasedOnSize.addAll(workloadQueue);
    }
}
