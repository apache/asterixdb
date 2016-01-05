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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.metadata.declared.AqlDataSource;
import org.apache.asterix.metadata.declared.AqlDataSource.AqlDataSourceType;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.declared.AqlSourceId;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractScanPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.BroadcastPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;

public class ExternalDataLookupPOperator extends AbstractScanPOperator {

    private final List<LogicalVariable> ridVarList;
    private AqlSourceId datasetId;
    private Dataset dataset;
    private ARecordType recordType;
    private Index secondaryIndex;
    private boolean requiresBroadcast;
    private boolean retainInput;
    private boolean retainNull;

    public ExternalDataLookupPOperator(AqlSourceId datasetId, Dataset dataset, ARecordType recordType,
            Index secondaryIndex, List<LogicalVariable> ridVarList, boolean requiresBroadcast, boolean retainInput,
            boolean retainNull) {
        this.datasetId = datasetId;
        this.dataset = dataset;
        this.recordType = recordType;
        this.secondaryIndex = secondaryIndex;
        this.ridVarList = ridVarList;
        this.requiresBroadcast = requiresBroadcast;
        this.retainInput = retainInput;
        this.retainNull = retainNull;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public ARecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(ARecordType recordType) {
        this.recordType = recordType;
    }

    public AqlSourceId getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(AqlSourceId datasetId) {
        this.datasetId = datasetId;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.EXTERNAL_LOOKUP;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        AqlDataSource ds = new DatasetDataSource(datasetId, datasetId.getDataverseName(), datasetId.getDatasourceName(),
                recordType, AqlDataSourceType.EXTERNAL_DATASET);
        IDataSourcePropertiesProvider dspp = ds.getPropertiesProvider();
        AbstractScanOperator as = (AbstractScanOperator) op;
        deliveredProperties = dspp.computePropertiesVector(as.getVariables());
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
                    throws AlgebricksException {
        ExternalDataLookupOperator edabro = (ExternalDataLookupOperator) op;
        ILogicalExpression expr = edabro.getExpressionRef().getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new IllegalStateException();
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        if (!funcIdent.equals(AsterixBuiltinFunctions.EXTERNAL_LOOKUP)) {
            return;
        }
        int[] ridIndexes = getKeyIndexes(ridVarList, inputSchemas);
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(op);
        List<LogicalVariable> outputVars = new ArrayList<LogicalVariable>();
        if (retainInput) {
            VariableUtilities.getLiveVariables(edabro, outputVars);
        } else {
            VariableUtilities.getProducedVariables(edabro, outputVars);
        }

        AqlMetadataProvider metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> externalLoopup = AqlMetadataProvider
                .buildExternalDataLookupRuntime(builder.getJobSpec(), dataset, secondaryIndex, ridIndexes, retainInput,
                        typeEnv, outputVars, opSchema, context, metadataProvider, retainNull);
        builder.contributeHyracksOperator(edabro, externalLoopup.first);
        builder.contributeAlgebricksPartitionConstraint(externalLoopup.first, externalLoopup.second);
        ILogicalOperator srcExchange = edabro.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, edabro, 0);
    }

    protected int[] getKeyIndexes(List<LogicalVariable> keyVarList, IOperatorSchema[] inputSchemas) {
        if (keyVarList == null) {
            return null;
        }
        int[] keyIndexes = new int[keyVarList.size()];
        for (int i = 0; i < keyVarList.size(); i++) {
            keyIndexes[i] = inputSchemas[0].findVariable(keyVarList.get(i));
        }
        return keyIndexes;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        if (requiresBroadcast) {
            StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
            pv[0] = new StructuralPropertiesVector(new BroadcastPartitioningProperty(null), null);
            return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);

        } else {
            return super.getRequiredPropertiesForChildren(op, reqdByParent);
        }
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

}