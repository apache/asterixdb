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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.asterix.algebra.base.AsterixOperatorAnnotations;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.AsterixRuntimeException;
import org.apache.asterix.metadata.declared.AqlDataSource;
import org.apache.asterix.metadata.declared.AqlDataSource.AqlDataSourceType;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.declared.AqlSourceId;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.base.AnalysisUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class PushFieldAccessRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator access = (AssignOperator) op;
        ILogicalExpression expr = getFirstExpr(access);
        String finalAnnot = null;
        if (AnalysisUtil.isAccessToFieldRecord(expr)) {
            finalAnnot = AsterixOperatorAnnotations.PUSHED_FIELD_ACCESS;
        } else if (AnalysisUtil.isRunnableAccessToFieldRecord(expr)) {
            finalAnnot = AsterixOperatorAnnotations.PUSHED_RUNNABLE_FIELD_ACCESS;
        } else {
            return false;
        }
        return propagateFieldAccessRec(opRef, context, finalAnnot);
    }

    @SuppressWarnings("unchecked")
    private boolean isAccessToIndexedField(AssignOperator assign, IOptimizationContext context)
            throws AlgebricksException {
        AbstractFunctionCallExpression accessFun = (AbstractFunctionCallExpression) assign.getExpressions().get(0)
                .getValue();
        ILogicalExpression e0 = accessFun.getArguments().get(0).getValue();
        if (e0.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        LogicalVariable var = ((VariableReferenceExpression) e0).getVariableReference();
        if (context.findPrimaryKey(var) == null) {
            // not referring to a dataset record
            return false;
        }
        AbstractLogicalOperator op = assign;
        while (op.getInputs().size() == 1 && op.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            op = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        }
        if (op.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            return false;
        }
        DataSourceScanOperator scan = (DataSourceScanOperator) op;
        LogicalVariable recVar = scan.getVariables().get(scan.getVariables().size() - 1);
        if (recVar != var) {
            return false;
        }
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        AqlSourceId asid = ((IDataSource<AqlSourceId>) scan.getDataSource()).getId();

        Dataset dataset = mp.findDataset(asid.getDataverseName(), asid.getDatasourceName());
        if (dataset == null) {
            throw new AlgebricksException("Dataset " + asid.getDatasourceName() + " not found.");
        }
        if (dataset.getDatasetType() != DatasetType.INTERNAL) {
            return false;
        }
        ILogicalExpression e1 = accessFun.getArguments().get(1).getValue();
        if (e1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }
        ConstantExpression ce = (ConstantExpression) e1;
        IAObject obj = ((AsterixConstantValue) ce.getValue()).getObject();
        if (obj.getType().getTypeTag() != ATypeTag.STRING) {
            int pos = ((AInt32) obj).getIntegerValue();
            String tName = dataset.getItemTypeName();
            IAType t = mp.findType(dataset.getItemTypeDataverseName(), tName);
            if (t.getTypeTag() != ATypeTag.RECORD) {
                return false;
            }
            ARecordType rt = (ARecordType) t;
            if (pos >= rt.getFieldNames().length) {
                return false;
            }
        }

        List<Index> datasetIndexes = mp.getDatasetIndexes(dataset.getDataverseName(), dataset.getDatasetName());
        boolean hasSecondaryIndex = false;
        for (Index index : datasetIndexes) {
            if (index.isSecondaryIndex()) {
                hasSecondaryIndex = true;
                break;
            }
        }

        return hasSecondaryIndex;
    }

    private boolean tryingToPushThroughSelectionWithSameDataSource(AssignOperator access, AbstractLogicalOperator op2) {
        if (op2.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        ILogicalExpression e1 = (ILogicalExpression) access.getAnnotations()
                .get(AsterixOperatorAnnotations.FIELD_ACCESS);
        if (e1 == null) {
            return false;
        }
        ILogicalExpression e2 = (ILogicalExpression) op2.getAnnotations().get(AsterixOperatorAnnotations.FIELD_ACCESS);
        if (e2 == null) {
            return false;
        }
        return e1.equals(e2);
    }

    @SuppressWarnings("unchecked")
    private boolean propagateFieldAccessRec(Mutable<ILogicalOperator> opRef, IOptimizationContext context,
            String finalAnnot) throws AlgebricksException {
        AssignOperator access = (AssignOperator) opRef.getValue();
        Mutable<ILogicalOperator> opRef2 = access.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
        // If it's not an indexed field, it is pushed so that scan can be
        // rewritten into index search.
        if (op2.getOperatorTag() == LogicalOperatorTag.PROJECT || context.checkAndAddToAlreadyCompared(access, op2)
                && !(op2.getOperatorTag() == LogicalOperatorTag.SELECT && isAccessToIndexedField(access, context))) {
            return false;
        }
        if (tryingToPushThroughSelectionWithSameDataSource(access, op2)) {
            return false;
        }
        if (testAndModifyRedundantOp(access, op2)) {
            propagateFieldAccessRec(opRef2, context, finalAnnot);
            return true;
        }
        List<LogicalVariable> usedInAccess = new LinkedList<LogicalVariable>();
        VariableUtilities.getUsedVariables(access, usedInAccess);

        List<LogicalVariable> produced2 = new LinkedList<LogicalVariable>();
        if (op2.getOperatorTag() == LogicalOperatorTag.GROUP) {
            VariableUtilities.getLiveVariables(op2, produced2);
        } else {
            VariableUtilities.getProducedVariables(op2, produced2);
        }
        boolean pushItDown = false;
        List<LogicalVariable> inter = new ArrayList<LogicalVariable>(usedInAccess);
        if (inter.isEmpty()) { // ground value
            return false;
        }
        inter.retainAll(produced2);
        if (inter.isEmpty()) {
            pushItDown = true;
        } else if (op2.getOperatorTag() == LogicalOperatorTag.GROUP) {
            GroupByOperator g = (GroupByOperator) op2;
            List<Pair<LogicalVariable, LogicalVariable>> varMappings = new ArrayList<Pair<LogicalVariable, LogicalVariable>>();
            for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : g.getDecorList()) {
                ILogicalExpression e = p.second.getValue();
                if (e.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                    LogicalVariable decorVar = GroupByOperator.getDecorVariable(p);
                    if (inter.contains(decorVar)) {
                        inter.remove(decorVar);
                        LogicalVariable v1 = ((VariableReferenceExpression) e).getVariableReference();
                        varMappings.add(new Pair<LogicalVariable, LogicalVariable>(decorVar, v1));
                    }
                }
            }
            if (inter.isEmpty()) {
                boolean changed = false;
                for (Pair<LogicalVariable, LogicalVariable> m : varMappings) {
                    LogicalVariable v2 = context.newVar();
                    LogicalVariable oldVar = access.getVariables().get(0);
                    g.getDecorList().add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(oldVar,
                            new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v2))));
                    changed = true;
                    access.getVariables().set(0, v2);
                    VariableUtilities.substituteVariables(access, m.first, m.second, context);
                }
                if (changed) {
                    context.computeAndSetTypeEnvironmentForOperator(g);
                }
                usedInAccess.clear();
                VariableUtilities.getUsedVariables(access, usedInAccess);
                pushItDown = true;
            }
        }
        if (pushItDown) {
            if (op2.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                Mutable<ILogicalOperator> childOfSubplan = ((NestedTupleSourceOperator) op2).getDataSourceReference()
                        .getValue().getInputs().get(0);
                pushAccessDown(opRef, op2, childOfSubplan, context, finalAnnot);
                return true;
            }
            if (op2.getInputs().size() == 1 && !op2.hasNestedPlans()) {
                pushAccessDown(opRef, op2, op2.getInputs().get(0), context, finalAnnot);
                return true;
            } else {
                for (Mutable<ILogicalOperator> inp : op2.getInputs()) {
                    HashSet<LogicalVariable> v2 = new HashSet<LogicalVariable>();
                    VariableUtilities.getLiveVariables(inp.getValue(), v2);
                    if (v2.containsAll(usedInAccess)) {
                        pushAccessDown(opRef, op2, inp, context, finalAnnot);
                        return true;
                    }
                }
            }
            if (op2.hasNestedPlans()) {
                AbstractOperatorWithNestedPlans nestedOp = (AbstractOperatorWithNestedPlans) op2;
                for (ILogicalPlan plan : nestedOp.getNestedPlans()) {
                    for (Mutable<ILogicalOperator> root : plan.getRoots()) {
                        HashSet<LogicalVariable> v2 = new HashSet<LogicalVariable>();
                        VariableUtilities.getLiveVariables(root.getValue(), v2);
                        if (v2.containsAll(usedInAccess)) {
                            pushAccessDown(opRef, op2, root, context, finalAnnot);
                            return true;
                        }
                    }
                }
            }
            throw new AsterixRuntimeException("Field access " + access.getExpressions().get(0).getValue()
                    + " does not correspond to any input of operator " + op2);
        } else {
            // Check if the accessed field is not one of the partitioning key
            // fields. If yes, we can equate the two variables.
            if (op2.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                DataSourceScanOperator scan = (DataSourceScanOperator) op2;
                int n = scan.getVariables().size();
                LogicalVariable scanRecordVar = scan.getVariables().get(n - 1);
                AbstractFunctionCallExpression accessFun = (AbstractFunctionCallExpression) access.getExpressions()
                        .get(0).getValue();
                ILogicalExpression e0 = accessFun.getArguments().get(0).getValue();
                LogicalExpressionTag tag = e0.getExpressionTag();
                if (tag == LogicalExpressionTag.VARIABLE) {
                    VariableReferenceExpression varRef = (VariableReferenceExpression) e0;
                    if (varRef.getVariableReference() == scanRecordVar) {
                        ILogicalExpression e1 = accessFun.getArguments().get(1).getValue();
                        if (e1.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                            IDataSource<AqlSourceId> dataSource = (IDataSource<AqlSourceId>) scan.getDataSource();
                            AqlDataSourceType dsType = ((AqlDataSource) dataSource).getDatasourceType();
                            if (dsType == AqlDataSourceType.FEED || dsType == AqlDataSourceType.LOADABLE) {
                                return false;
                            }
                            AqlSourceId asid = dataSource.getId();
                            AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
                            Dataset dataset = mp.findDataset(asid.getDataverseName(), asid.getDatasourceName());
                            if (dataset == null) {
                                throw new AlgebricksException("Dataset " + asid.getDatasourceName() + " not found.");
                            }
                            if (dataset.getDatasetType() != DatasetType.INTERNAL) {
                                setAsFinal(access, context, finalAnnot);
                                return false;
                            }
                            ConstantExpression ce = (ConstantExpression) e1;
                            IAObject obj = ((AsterixConstantValue) ce.getValue()).getObject();
                            String fldName;
                            if (obj.getType().getTypeTag() == ATypeTag.STRING) {
                                fldName = ((AString) obj).getStringValue();
                            } else {
                                int pos = ((AInt32) obj).getIntegerValue();
                                String tName = dataset.getItemTypeName();
                                IAType t = mp.findType(dataset.getItemTypeDataverseName(), tName);
                                if (t.getTypeTag() != ATypeTag.RECORD) {
                                    return false;
                                }
                                ARecordType rt = (ARecordType) t;
                                if (pos >= rt.getFieldNames().length) {
                                    setAsFinal(access, context, finalAnnot);
                                    return false;
                                }
                                fldName = rt.getFieldNames()[pos];
                            }
                            int p = DatasetUtils.getPositionOfPartitioningKeyField(dataset, fldName);
                            if (p < 0) { // not one of the partitioning fields
                                setAsFinal(access, context, finalAnnot);
                                return false;
                            }
                            LogicalVariable keyVar = scan.getVariables().get(p);
                            access.getExpressions().get(0).setValue(new VariableReferenceExpression(keyVar));
                            return true;
                        }
                    }
                }
            }
            setAsFinal(access, context, finalAnnot);
            return false;
        }
    }

    private void setAsFinal(ILogicalOperator access, IOptimizationContext context, String finalAnnot) {
        access.getAnnotations().put(finalAnnot, true);
        context.addToDontApplySet(this, access);
    }

    private boolean testAndModifyRedundantOp(AssignOperator access, AbstractLogicalOperator op2) {
        if (op2.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator a2 = (AssignOperator) op2;
        if (getFirstExpr(access).equals(getFirstExpr(a2))) {
            access.getExpressions().get(0).setValue(new VariableReferenceExpression(a2.getVariables().get(0)));
            return true;
        } else {
            return false;
        }
    }

    // indirect recursivity with propagateFieldAccessRec
    private void pushAccessDown(Mutable<ILogicalOperator> fldAccessOpRef, ILogicalOperator op2,
            Mutable<ILogicalOperator> inputOfOp2, IOptimizationContext context, String finalAnnot)
                    throws AlgebricksException {
        ILogicalOperator fieldAccessOp = fldAccessOpRef.getValue();
        fldAccessOpRef.setValue(op2);
        List<Mutable<ILogicalOperator>> faInpList = fieldAccessOp.getInputs();
        faInpList.clear();
        faInpList.add(new MutableObject<ILogicalOperator>(inputOfOp2.getValue()));
        inputOfOp2.setValue(fieldAccessOp);
        // typing
        context.computeAndSetTypeEnvironmentForOperator(fieldAccessOp);
        context.computeAndSetTypeEnvironmentForOperator(op2);
        propagateFieldAccessRec(inputOfOp2, context, finalAnnot);
    }

    private ILogicalExpression getFirstExpr(AssignOperator assign) {
        return assign.getExpressions().get(0).getValue();
    }

}
