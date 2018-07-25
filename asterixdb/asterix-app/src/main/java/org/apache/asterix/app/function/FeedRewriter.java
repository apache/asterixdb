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
package org.apache.asterix.app.function;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.feed.watch.FeedActivityDetails;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.FeedDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedConnection;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.feeds.BuiltinFeedPolicies;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionToDataSourceRewriter;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.rules.UnnestToDataScanRule;
import org.apache.asterix.translator.util.PlanTranslationUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;

public class FeedRewriter implements IFunctionToDataSourceRewriter, IResultTypeComputer {
    public static final FeedRewriter INSTANCE = new FeedRewriter();

    private FeedRewriter() {
    }

    @Override
    public boolean rewrite(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractFunctionCallExpression f = UnnestToDataScanRule.getFunctionCall(opRef);
        UnnestOperator unnest = (UnnestOperator) opRef.getValue();
        if (unnest.getPositionalVariable() != null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, unnest.getSourceLocation(),
                    "No positional variables are allowed over feeds.");
        }
        String dataverse = ConstantExpressionUtil.getStringArgument(f, 0);
        String sourceFeedName = ConstantExpressionUtil.getStringArgument(f, 1);
        String getTargetFeed = ConstantExpressionUtil.getStringArgument(f, 2);
        String subscriptionLocation = ConstantExpressionUtil.getStringArgument(f, 3);
        String targetDataset = ConstantExpressionUtil.getStringArgument(f, 4);
        String outputType = ConstantExpressionUtil.getStringArgument(f, 5);
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        DataSourceId asid = new DataSourceId(dataverse, getTargetFeed);
        String policyName = (String) metadataProvider.getConfig().get(FeedActivityDetails.FEED_POLICY_NAME);
        FeedPolicyEntity policy = metadataProvider.findFeedPolicy(dataverse, policyName);
        if (policy == null) {
            policy = BuiltinFeedPolicies.getFeedPolicy(policyName);
            if (policy == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, unnest.getSourceLocation(),
                        "Unknown feed policy:" + policyName);
            }
        }
        ArrayList<LogicalVariable> feedDataScanOutputVariables = new ArrayList<>();
        String csLocations = (String) metadataProvider.getConfig().get(FeedActivityDetails.COLLECT_LOCATIONS);
        List<LogicalVariable> pkVars = new ArrayList<>();
        FeedDataSource ds = createFeedDataSource(asid, targetDataset, sourceFeedName, subscriptionLocation,
                metadataProvider, policy, outputType, csLocations, unnest.getVariable(), context, pkVars);
        // The order for feeds is <Record-Meta-PK>
        feedDataScanOutputVariables.add(unnest.getVariable());
        // Does it produce meta?
        if (ds.hasMeta()) {
            feedDataScanOutputVariables.add(context.newVar());
        }
        // Does it produce pk?
        if (ds.isChange()) {
            feedDataScanOutputVariables.addAll(pkVars);
        }
        DataSourceScanOperator scan = new DataSourceScanOperator(feedDataScanOutputVariables, ds);
        scan.setSourceLocation(unnest.getSourceLocation());
        List<Mutable<ILogicalOperator>> scanInpList = scan.getInputs();
        scanInpList.addAll(unnest.getInputs());
        opRef.setValue(scan);
        context.computeAndSetTypeEnvironmentForOperator(scan);
        return true;
    }

    private FeedDataSource createFeedDataSource(DataSourceId aqlId, String targetDataset, String sourceFeedName,
            String subscriptionLocation, MetadataProvider metadataProvider, FeedPolicyEntity feedPolicy,
            String outputType, String locations, LogicalVariable recordVar, IOptimizationContext context,
            List<LogicalVariable> pkVars) throws AlgebricksException {
        Dataset dataset = metadataProvider.findDataset(aqlId.getDataverseName(), targetDataset);
        ARecordType feedOutputType = (ARecordType) metadataProvider.findType(aqlId.getDataverseName(), outputType);
        Feed sourceFeed = metadataProvider.findFeed(aqlId.getDataverseName(), sourceFeedName);
        FeedConnection feedConnection =
                metadataProvider.findFeedConnection(aqlId.getDataverseName(), sourceFeedName, targetDataset);
        ARecordType metaType = null;
        // Does dataset have meta?
        if (dataset.hasMetaPart()) {
            String metaTypeName = FeedUtils.getFeedMetaTypeName(sourceFeed.getConfiguration());
            if (metaTypeName == null) {
                throw new AlgebricksException("Feed to a dataset with metadata doesn't have meta type specified");
            }
            String dataverseName = aqlId.getDataverseName();
            if (metaTypeName.contains(".")) {
                dataverseName = metaTypeName.substring(0, metaTypeName.indexOf('.'));
                metaTypeName = metaTypeName.substring(metaTypeName.indexOf('.') + 1);
            }
            metaType = (ARecordType) metadataProvider.findType(dataverseName, metaTypeName);
        }
        // Is a change feed?
        List<IAType> pkTypes = null;
        List<List<String>> partitioningKeys = null;
        List<Integer> keySourceIndicator = null;

        List<ScalarFunctionCallExpression> keyAccessScalarFunctionCallExpression;
        if (ExternalDataUtils.isChangeFeed(sourceFeed.getConfiguration())) {
            List<Mutable<ILogicalExpression>> keyAccessExpression = new ArrayList<>();
            keyAccessScalarFunctionCallExpression = new ArrayList<>();
            pkTypes = ((InternalDatasetDetails) dataset.getDatasetDetails()).getPrimaryKeyType();
            partitioningKeys = ((InternalDatasetDetails) dataset.getDatasetDetails()).getPartitioningKey();
            if (dataset.hasMetaPart()) {
                keySourceIndicator = ((InternalDatasetDetails) dataset.getDatasetDetails()).getKeySourceIndicator();
            }
            for (int i = 0; i < partitioningKeys.size(); i++) {
                List<String> key = partitioningKeys.get(i);
                if (keySourceIndicator == null || keySourceIndicator.get(i).intValue() == 0) {
                    PlanTranslationUtil.prepareVarAndExpression(key, recordVar, pkVars, keyAccessExpression, null,
                            context, null);
                } else {
                    PlanTranslationUtil.prepareMetaKeyAccessExpression(key, recordVar, keyAccessExpression, pkVars,
                            null, context, null);
                }
            }
            keyAccessExpression.forEach(
                    expr -> keyAccessScalarFunctionCallExpression.add((ScalarFunctionCallExpression) expr.getValue()));
        } else {
            keyAccessScalarFunctionCallExpression = null;
        }
        FeedDataSource feedDataSource = new FeedDataSource(sourceFeed, aqlId, targetDataset, feedOutputType, metaType,
                pkTypes, keyAccessScalarFunctionCallExpression, sourceFeed.getFeedId(),
                FeedRuntimeType.valueOf(subscriptionLocation), locations.split(","), context.getComputationNodeDomain(),
                feedConnection);
        feedDataSource.getProperties().put(BuiltinFeedPolicies.CONFIG_FEED_POLICY_KEY, feedPolicy);
        return feedDataSource;
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env, IMetadataProvider<?, ?> mp)
            throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        if (f.getArguments().size() != BuiltinFunctions.FEED_COLLECT.getArity()) {
            throw new AlgebricksException("Incorrect number of arguments -> arity is "
                    + BuiltinFunctions.FEED_COLLECT.getArity() + ", not " + f.getArguments().size());
        }
        ILogicalExpression a1 = f.getArguments().get(5).getValue();
        IAType t1 = (IAType) env.getType(a1);
        if (t1.getTypeTag() == ATypeTag.ANY) {
            return BuiltinType.ANY;
        }
        if (t1.getTypeTag() != ATypeTag.STRING) {
            throw new AlgebricksException("Illegal type " + t1 + " for feed-ingest argument.");
        }
        String typeArg = ConstantExpressionUtil.getStringConstant(a1);
        if (typeArg == null) {
            return BuiltinType.ANY;
        }
        MetadataProvider metadata = (MetadataProvider) mp;
        Pair<String, String> argInfo = DatasetUtil.getDatasetInfo(metadata, typeArg);
        String dataverseName = argInfo.first;
        String typeName = argInfo.second;
        if (dataverseName == null) {
            throw new AlgebricksException("Unspecified dataverse!");
        }
        IAType t2 = metadata.findType(dataverseName, typeName);
        if (t2 == null) {
            throw new AlgebricksException("Unknown type  " + typeName);
        }
        return t2;

    }

}
