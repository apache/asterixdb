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
package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.feeds.FeedActivity.FeedActivityDetails;
import org.apache.asterix.common.feeds.api.IFeedLifecycleListener.ConnectionLocation;
import org.apache.asterix.metadata.declared.AqlDataSource;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.declared.AqlSourceId;
import org.apache.asterix.metadata.declared.FeedDataSource;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedPolicy;
import org.apache.asterix.metadata.feeds.BuiltinFeedPolicies;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.util.EquivalenceClassUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class UnnestToDataScanRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnest = (UnnestOperator) op;
        ILogicalExpression unnestExpr = unnest.getExpressionRef().getValue();

        if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
            FunctionIdentifier fid = f.getFunctionIdentifier();

            if (fid.equals(AsterixBuiltinFunctions.DATASET)) {
                if (unnest.getPositionalVariable() != null) {
                    // TODO remove this after enabling the support of positional variables in data scan
                    throw new AlgebricksException("No positional variables are allowed over datasets.");
                }
                ILogicalExpression expr = f.getArguments().get(0).getValue();
                if (expr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                    return false;
                }
                ConstantExpression ce = (ConstantExpression) expr;
                IAlgebricksConstantValue acv = ce.getValue();
                if (!(acv instanceof AsterixConstantValue)) {
                    return false;
                }
                AsterixConstantValue acv2 = (AsterixConstantValue) acv;
                if (acv2.getObject().getType().getTypeTag() != ATypeTag.STRING) {
                    return false;
                }
                String datasetArg = ((AString) acv2.getObject()).getStringValue();

                AqlMetadataProvider metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();
                Pair<String, String> datasetReference = parseDatasetReference(metadataProvider, datasetArg);
                String dataverseName = datasetReference.first;
                String datasetName = datasetReference.second;
                Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
                if (dataset == null) {
                    throw new AlgebricksException("Could not find dataset " + datasetName + " in dataverse "
                            + dataverseName);
                }

                AqlSourceId asid = new AqlSourceId(dataverseName, datasetName);

                ArrayList<LogicalVariable> v = new ArrayList<LogicalVariable>();

                if (dataset.getDatasetType() == DatasetType.INTERNAL) {
                    int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
                    for (int i = 0; i < numPrimaryKeys; i++) {
                        v.add(context.newVar());
                    }
                }
                v.add(unnest.getVariable());
                AqlDataSource dataSource = metadataProvider.findDataSource(asid);
                DataSourceScanOperator scan = new DataSourceScanOperator(v, dataSource);
                List<Mutable<ILogicalOperator>> scanInpList = scan.getInputs();
                scanInpList.addAll(unnest.getInputs());
                opRef.setValue(scan);
                addPrimaryKey(v, context);
                context.computeAndSetTypeEnvironmentForOperator(scan);

                // Adds equivalence classes --- one equivalent class between a primary key
                // variable and a record field-access expression.
                IAType[] schemaTypes = dataSource.getSchemaTypes();
                ARecordType recordType = (ARecordType) schemaTypes[schemaTypes.length - 1];
                EquivalenceClassUtils.addEquivalenceClassesForPrimaryIndexAccess(scan, v, recordType, dataset, context);
                return true;
            }

            if (fid.equals(AsterixBuiltinFunctions.FEED_COLLECT)) {
                if (unnest.getPositionalVariable() != null) {
                    throw new AlgebricksException("No positional variables are allowed over datasets.");
                }

                String dataverse = getStringArgument(f, 0);
                String sourceFeedName = getStringArgument(f, 1);
                String getTargetFeed = getStringArgument(f, 2);
                String subscriptionLocation = getStringArgument(f, 3);
                String targetDataset = getStringArgument(f, 4);
                String outputType = getStringArgument(f, 5);

                AqlMetadataProvider metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();

                AqlSourceId asid = new AqlSourceId(dataverse, getTargetFeed);
                String policyName = metadataProvider.getConfig().get(FeedActivityDetails.FEED_POLICY_NAME);
                FeedPolicy policy = metadataProvider.findFeedPolicy(dataverse, policyName);
                if (policy == null) {
                    policy = BuiltinFeedPolicies.getFeedPolicy(policyName);
                    if (policy == null) {
                        throw new AlgebricksException("Unknown feed policy:" + policyName);
                    }
                }

                ArrayList<LogicalVariable> v = new ArrayList<LogicalVariable>();
                v.add(unnest.getVariable());

                String csLocations = metadataProvider.getConfig().get(FeedActivityDetails.COLLECT_LOCATIONS);
                DataSourceScanOperator scan = new DataSourceScanOperator(v, createFeedDataSource(asid, targetDataset,
                        sourceFeedName, subscriptionLocation, metadataProvider, policy, outputType, csLocations));

                List<Mutable<ILogicalOperator>> scanInpList = scan.getInputs();
                scanInpList.addAll(unnest.getInputs());
                opRef.setValue(scan);
                addPrimaryKey(v, context);
                context.computeAndSetTypeEnvironmentForOperator(scan);

                return true;
            }

        }

        return false;
    }

    public void addPrimaryKey(List<LogicalVariable> scanVariables, IOptimizationContext context) {
        int n = scanVariables.size();
        List<LogicalVariable> head = new ArrayList<LogicalVariable>(scanVariables.subList(0, n - 1));
        List<LogicalVariable> tail = new ArrayList<LogicalVariable>(1);
        tail.add(scanVariables.get(n - 1));
        FunctionalDependency pk = new FunctionalDependency(head, tail);
        context.addPrimaryKey(pk);
    }

    private AqlDataSource createFeedDataSource(AqlSourceId aqlId, String targetDataset, String sourceFeedName,
            String subscriptionLocation, AqlMetadataProvider metadataProvider, FeedPolicy feedPolicy,
            String outputType, String locations) throws AlgebricksException {
        if (!aqlId.getDataverseName().equals(
                metadataProvider.getDefaultDataverse() == null ? null : metadataProvider.getDefaultDataverse()
                        .getDataverseName())) {
            return null;
        }
        IAType feedOutputType = metadataProvider.findType(aqlId.getDataverseName(), outputType);
        Feed sourceFeed = metadataProvider.findFeed(aqlId.getDataverseName(), sourceFeedName);

        FeedDataSource feedDataSource = new FeedDataSource(aqlId, targetDataset, feedOutputType,
                AqlDataSource.AqlDataSourceType.FEED, sourceFeed.getFeedId(), sourceFeed.getFeedType(),
                ConnectionLocation.valueOf(subscriptionLocation), locations.split(","));
        feedDataSource.getProperties().put(BuiltinFeedPolicies.CONFIG_FEED_POLICY_KEY, feedPolicy);
        return feedDataSource;
    }

    private Pair<String, String> parseDatasetReference(AqlMetadataProvider metadataProvider, String datasetArg)
            throws AlgebricksException {
        String[] datasetNameComponents = datasetArg.split("\\.");
        String dataverseName;
        String datasetName;
        if (datasetNameComponents.length == 1) {
            Dataverse defaultDataverse = metadataProvider.getDefaultDataverse();
            if (defaultDataverse == null) {
                throw new AlgebricksException("Unresolved dataset " + datasetArg + " Dataverse not specified.");
            }
            dataverseName = defaultDataverse.getDataverseName();
            datasetName = datasetNameComponents[0];
        } else {
            dataverseName = datasetNameComponents[0];
            datasetName = datasetNameComponents[1];
        }
        return new Pair<String, String>(dataverseName, datasetName);
    }

    private String getStringArgument(AbstractFunctionCallExpression f, int index) {

        ILogicalExpression expr = f.getArguments().get(index).getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return null;
        }
        ConstantExpression ce = (ConstantExpression) expr;
        IAlgebricksConstantValue acv = ce.getValue();
        if (!(acv instanceof AsterixConstantValue)) {
            return null;
        }
        AsterixConstantValue acv2 = (AsterixConstantValue) acv;
        if (acv2.getObject().getType().getTypeTag() != ATypeTag.STRING) {
            return null;
        }
        String argument = ((AString) acv2.getObject()).getStringValue();
        return argument;
    }

}
