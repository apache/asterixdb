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

package org.apache.asterix.algebra.operators.physical;

import java.util.List;

import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.io.FileSplit;

public class CommitPOperator extends AbstractPhysicalOperator {

    private final List<LogicalVariable> primaryKeyLogicalVars;
    private final JobId jobId;
    private final int datasetId;
    private final String dataverse;
    private final String dataset;
    private final LogicalVariable upsertVar;
    private final boolean isSink;

    public CommitPOperator(JobId jobId, String dataverse, String dataset, int datasetId,
            List<LogicalVariable> primaryKeyLogicalVars, LogicalVariable upsertVar, boolean isSink) {
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.primaryKeyLogicalVars = primaryKeyLogicalVars;
        this.upsertVar = upsertVar;
        this.dataverse = dataverse;
        this.dataset = dataset;
        this.isSink = isSink;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.EXTENSION_OPERATOR;
    }

    @Override
    public String toString() {
        return "COMMIT";
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        return emptyUnaryRequirements();
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        deliveredProperties = op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
                    throws AlgebricksException {
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema,
                context);
        int[] primaryKeyFields = JobGenHelper.variablesToFieldIndexes(primaryKeyLogicalVars, inputSchemas[0]);

        //get dataset splits
        FileSplit[] splitsForDataset = metadataProvider.splitsForDataset(metadataProvider.getMetadataTxnContext(),
                dataverse, dataset, dataset, metadataProvider.isTemporaryDatasetWriteJob());
        int[] datasetPartitions = new int[splitsForDataset.length];
        for (int i = 0; i < splitsForDataset.length; i++) {
            datasetPartitions[i] = i;
        }

        int upsertVarIdx = -1;
        CommitRuntimeFactory runtime = null;
        if (upsertVar != null) {
            upsertVarIdx = inputSchemas[0].findVariable(upsertVar);
        }
        runtime = new CommitRuntimeFactory(jobId, datasetId, primaryKeyFields,
                metadataProvider.isTemporaryDatasetWriteJob(), metadataProvider.isWriteTransaction(), upsertVarIdx,
                datasetPartitions, isSink);
        builder.contributeMicroOperator(op, runtime, recDesc);
        ILogicalOperator src = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, op, 0);
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    @Override
    public boolean expensiveThanMaterialization() {
        return false;
    }
}
