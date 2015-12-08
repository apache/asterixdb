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

package org.apache.asterix.util;

import org.apache.asterix.common.config.AsterixCompilerProperties;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.runtime.operators.std.FlushDatasetOperatorDescriptor;
import org.apache.asterix.transaction.management.service.transaction.JobIdFactory;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.EmptyTupleSourceRuntimeFactory;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class FlushDatasetUtils {

    public static void flushDataset(IHyracksClientConnection hcc, AqlMetadataProvider metadataProvider,
            MetadataTransactionContext mdTxnCtx, String dataverseName, String datasetName, String indexName)
                    throws Exception {
        AsterixCompilerProperties compilerProperties = AsterixAppContextInfo.getInstance().getCompilerProperties();
        int frameSize = compilerProperties.getFrameSize();
        JobSpecification spec = new JobSpecification(frameSize);

        RecordDescriptor[] rDescs = new RecordDescriptor[] { new RecordDescriptor(new ISerializerDeserializer[] {}) };
        AlgebricksMetaOperatorDescriptor emptySource = new AlgebricksMetaOperatorDescriptor(spec, 0, 1,
                new IPushRuntimeFactory[] { new EmptyTupleSourceRuntimeFactory() }, rDescs);

        org.apache.asterix.common.transactions.JobId jobId = JobIdFactory.generateJobId();
        Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
        FlushDatasetOperatorDescriptor flushOperator = new FlushDatasetOperatorDescriptor(spec, jobId,
                dataset.getDatasetId());

        spec.connect(new OneToOneConnectorDescriptor(spec), emptySource, 0, flushOperator, 0);

        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> primarySplitsAndConstraint = metadataProvider
                .splitProviderAndPartitionConstraintsForDataset(dataverseName, datasetName, indexName,
                        dataset.getDatasetDetails().isTemp());
        AlgebricksPartitionConstraint primaryPartitionConstraint = primarySplitsAndConstraint.second;

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, emptySource,
                primaryPartitionConstraint);

        JobEventListenerFactory jobEventListenerFactory = new JobEventListenerFactory(jobId, true);
        spec.setJobletEventListenerFactory(jobEventListenerFactory);
        JobUtils.runJob(hcc, spec, true);
    }

}
