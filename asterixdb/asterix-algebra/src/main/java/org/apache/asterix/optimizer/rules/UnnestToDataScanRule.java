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
import org.apache.asterix.external.feed.watch.FeedActivityDetails;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.asterix.external.util.FeedUtils.FeedRuntimeType;
import org.apache.asterix.metadata.declared.AqlDataSource;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.declared.AqlSourceId;
import org.apache.asterix.metadata.declared.FeedDataSource;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.metadata.feeds.BuiltinFeedPolicies;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.ConstantExpressionUtil;
import org.apache.asterix.optimizer.rules.util.EquivalenceClassUtils;
import org.apache.asterix.translator.util.PlanTranslationUtil;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class UnnestToDataScanRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
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
        if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        return handleFunction(opRef, context, unnest, (AbstractFunctionCallExpression) unnestExpr);
    }

    protected boolean handleFunction(Mutable<ILogicalOperator> opRef, IOptimizationContext context,
            UnnestOperator unnest, AbstractFunctionCallExpression f) throws AlgebricksException {
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
                throw new AlgebricksException(
                        "Could not find dataset " + datasetName + " in dataverse " + dataverseName);
            }
            AqlSourceId asid = new AqlSourceId(dataverseName, datasetName);
            List<LogicalVariable> variables = new ArrayList<>();
            if (dataset.getDatasetType() == DatasetType.INTERNAL) {
                int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
                for (int i = 0; i < numPrimaryKeys; i++) {
                    variables.add(context.newVar());
                }
            }
            variables.add(unnest.getVariable());
            AqlDataSource dataSource = metadataProvider.findDataSource(asid);
            boolean hasMeta = dataSource.hasMeta();
            if (hasMeta) {
                variables.add(context.newVar());
            }
            DataSourceScanOperator scan = new DataSourceScanOperator(variables, dataSource);
            List<Mutable<ILogicalOperator>> scanInpList = scan.getInputs();
            scanInpList.addAll(unnest.getInputs());
            opRef.setValue(scan);
            addPrimaryKey(variables, dataSource, context);
            context.computeAndSetTypeEnvironmentForOperator(scan);
            // Adds equivalence classes --- one equivalent class between a primary key
            // variable and a record field-access expression.
            IAType[] schemaTypes = dataSource.getSchemaTypes();
            ARecordType recordType = (ARecordType) (hasMeta ? schemaTypes[schemaTypes.length - 2]
                    : schemaTypes[schemaTypes.length - 1]);
            ARecordType metaRecordType = (ARecordType) (hasMeta ? schemaTypes[schemaTypes.length - 1] : null);
            EquivalenceClassUtils.addEquivalenceClassesForPrimaryIndexAccess(scan, variables, recordType,
                    metaRecordType, dataset, context);
            return true;
        } else if (fid.equals(AsterixBuiltinFunctions.FEED_COLLECT)) {
            if (unnest.getPositionalVariable() != null) {
                throw new AlgebricksException("No positional variables are allowed over feeds.");
            }
            String dataverse = ConstantExpressionUtil.getStringArgument(f, 0);
            String sourceFeedName = ConstantExpressionUtil.getStringArgument(f, 1);
            String getTargetFeed = ConstantExpressionUtil.getStringArgument(f, 2);
            String subscriptionLocation = ConstantExpressionUtil.getStringArgument(f, 3);
            String targetDataset = ConstantExpressionUtil.getStringArgument(f, 4);
            String outputType = ConstantExpressionUtil.getStringArgument(f, 5);
            AqlMetadataProvider metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();
            AqlSourceId asid = new AqlSourceId(dataverse, getTargetFeed);
            String policyName = metadataProvider.getConfig().get(FeedActivityDetails.FEED_POLICY_NAME);
            FeedPolicyEntity policy = metadataProvider.findFeedPolicy(dataverse, policyName);
            if (policy == null) {
                policy = BuiltinFeedPolicies.getFeedPolicy(policyName);
                if (policy == null) {
                    throw new AlgebricksException("Unknown feed policy:" + policyName);
                }
            }
            ArrayList<LogicalVariable> feedDataScanOutputVariables = new ArrayList<>();
            String csLocations = metadataProvider.getConfig().get(FeedActivityDetails.COLLECT_LOCATIONS);
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
            List<Mutable<ILogicalOperator>> scanInpList = scan.getInputs();
            scanInpList.addAll(unnest.getInputs());
            opRef.setValue(scan);
            context.computeAndSetTypeEnvironmentForOperator(scan);
            return true;
        }
        return false;
    }

    private void addPrimaryKey(List<LogicalVariable> scanVariables, AqlDataSource dataSource,
            IOptimizationContext context) {
        List<LogicalVariable> primaryKey = dataSource.getPrimaryKeyVariables(scanVariables);
        List<LogicalVariable> tail = new ArrayList<LogicalVariable>();
        tail.addAll(scanVariables);
        FunctionalDependency pk = new FunctionalDependency(primaryKey, tail);
        context.addPrimaryKey(pk);
    }

    private FeedDataSource createFeedDataSource(AqlSourceId aqlId, String targetDataset, String sourceFeedName,
            String subscriptionLocation, AqlMetadataProvider metadataProvider, FeedPolicyEntity feedPolicy,
            String outputType, String locations, LogicalVariable recordVar, IOptimizationContext context,
            List<LogicalVariable> pkVars) throws AlgebricksException {
        if (!aqlId.getDataverseName().equals(metadataProvider.getDefaultDataverse() == null ? null
                : metadataProvider.getDefaultDataverse().getDataverseName())) {
            return null;
        }
        Dataset dataset = metadataProvider.findDataset(aqlId.getDataverseName(), targetDataset);
        ARecordType feedOutputType = (ARecordType) metadataProvider.findType(aqlId.getDataverseName(), outputType);
        Feed sourceFeed = metadataProvider.findFeed(aqlId.getDataverseName(), sourceFeedName);
        ARecordType metaType = null;
        // Does dataset have meta?
        if (dataset.hasMetaPart()) {
            String metaTypeName = FeedUtils.getFeedMetaTypeName(sourceFeed.getAdapterConfiguration());
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
        List<Mutable<ILogicalExpression>> keyAccessExpression = null;
        List<ScalarFunctionCallExpression> keyAccessScalarFunctionCallExpression;
        if (ExternalDataUtils.isChangeFeed(sourceFeed.getAdapterConfiguration())) {
            keyAccessExpression = new ArrayList<>();
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
                            context);
                } else {
                    PlanTranslationUtil.prepareMetaKeyAccessExpression(key, recordVar, keyAccessExpression, pkVars,
                            null, context);
                }
            }
            keyAccessExpression.forEach(
                    expr -> keyAccessScalarFunctionCallExpression.add((ScalarFunctionCallExpression) expr.getValue()));
        } else {
            keyAccessExpression = null;
            keyAccessScalarFunctionCallExpression = null;
        }
        FeedDataSource feedDataSource = new FeedDataSource(sourceFeed, aqlId, targetDataset, feedOutputType, metaType,
                pkTypes, partitioningKeys, keyAccessScalarFunctionCallExpression, sourceFeed.getFeedId(),
                sourceFeed.getFeedType(), FeedRuntimeType.valueOf(subscriptionLocation), locations.split(","),
                context.getComputationNodeDomain());
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
        return new Pair<>(dataverseName, datasetName);
    }
}