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

import org.apache.asterix.algebra.base.OperatorAnnotation;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
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
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
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
        if (!OperatorPropertiesUtil.isMovable(op)) {
            return false;
        }
        AssignOperator access = (AssignOperator) op;
        ILogicalExpression expr = getFirstExpr(access);
        String finalAnnot;
        if (AnalysisUtil.isAccessToFieldRecord(expr)) {
            finalAnnot = OperatorAnnotation.PUSHED_FIELD_ACCESS;
        } else if (AnalysisUtil.isRunnableAccessToFieldRecord(expr)) {
            finalAnnot = OperatorAnnotation.PUSHED_RUNNABLE_FIELD_ACCESS;
        } else {
            return false;
        }
        return pushDownFieldAccessRec(opRef, context, finalAnnot);
    }

    private boolean isAccessToIndexedField(AssignOperator assign, IOptimizationContext context)
            throws AlgebricksException {
        AbstractFunctionCallExpression accessFun =
                (AbstractFunctionCallExpression) assign.getExpressions().get(0).getValue();
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
        MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();
        DataSourceId asid = ((IDataSource<DataSourceId>) scan.getDataSource()).getId();

        Dataset dataset = mp.findDataset(asid.getDataverseName(), asid.getDatasourceName());
        if (dataset == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, scan.getSourceLocation(),
                    asid.getDatasourceName(), asid.getDataverseName());
        }
        if (dataset.getDatasetType() != DatasetType.INTERNAL) {
            return false;
        }
        final Integer pos = ConstantExpressionUtil.getIntConstant(accessFun.getArguments().get(1).getValue());
        if (pos != null) {
            String tName = dataset.getItemTypeName();
            IAType t = mp.findType(dataset.getItemTypeDataverseName(), tName);
            if (t.getTypeTag() != ATypeTag.OBJECT) {
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
        ILogicalExpression e1 = (ILogicalExpression) access.getAnnotations().get(OperatorAnnotation.FIELD_ACCESS);
        if (e1 == null) {
            return false;
        }
        ILogicalExpression e2 = (ILogicalExpression) op2.getAnnotations().get(OperatorAnnotation.FIELD_ACCESS);
        if (e2 == null) {
            return false;
        }
        return e1.equals(e2);
    }

    private boolean pushDownFieldAccessRec(Mutable<ILogicalOperator> opRef, IOptimizationContext context,
            String finalAnnot) throws AlgebricksException {
        AssignOperator assignOp = (AssignOperator) opRef.getValue();
        Mutable<ILogicalOperator> opRef2 = assignOp.getInputs().get(0);
        AbstractLogicalOperator inputOp = (AbstractLogicalOperator) opRef2.getValue();
        // If it's not an indexed field, it is pushed so that scan can be rewritten into index search.
        if (inputOp.getOperatorTag() == LogicalOperatorTag.PROJECT
                || context.checkAndAddToAlreadyCompared(assignOp, inputOp)
                        && !(inputOp.getOperatorTag() == LogicalOperatorTag.SELECT
                                && isAccessToIndexedField(assignOp, context))) {
            return false;
        }
        Object annotation = inputOp.getAnnotations().get(OperatorPropertiesUtil.MOVABLE);
        if (annotation != null && !((Boolean) annotation)) {
            return false;
        }
        if (tryingToPushThroughSelectionWithSameDataSource(assignOp, inputOp)) {
            return false;
        }
        if (testAndModifyRedundantOp(assignOp, inputOp)) {
            pushDownFieldAccessRec(opRef2, context, finalAnnot);
            return true;
        }
        HashSet<LogicalVariable> usedInAccess = new HashSet<>();
        VariableUtilities.getUsedVariables(assignOp, usedInAccess);

        HashSet<LogicalVariable> produced2 = new HashSet<>();
        if (inputOp.getOperatorTag() == LogicalOperatorTag.GROUP) {
            VariableUtilities.getLiveVariables(inputOp, produced2);
        } else {
            VariableUtilities.getProducedVariables(inputOp, produced2);
        }
        boolean pushItDown = false;
        HashSet<LogicalVariable> inter = new HashSet<>(usedInAccess);
        if (inter.isEmpty()) { // ground value
            return false;
        }
        inter.retainAll(produced2);
        if (inter.isEmpty()) {
            pushItDown = true;
        } else if (inputOp.getOperatorTag() == LogicalOperatorTag.GROUP) {
            GroupByOperator g = (GroupByOperator) inputOp;
            List<Pair<LogicalVariable, LogicalVariable>> varMappings = new ArrayList<>();
            for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : g.getDecorList()) {
                ILogicalExpression e = p.second.getValue();
                if (e.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                    LogicalVariable decorVar = GroupByOperator.getDecorVariable(p);
                    if (inter.contains(decorVar)) {
                        inter.remove(decorVar);
                        LogicalVariable v1 = ((VariableReferenceExpression) e).getVariableReference();
                        varMappings.add(new Pair<>(decorVar, v1));
                    }
                }
            }
            if (inter.isEmpty()) {
                boolean changed = false;
                for (Pair<LogicalVariable, LogicalVariable> m : varMappings) {
                    LogicalVariable v2 = context.newVar();
                    LogicalVariable oldVar = assignOp.getVariables().get(0);
                    VariableReferenceExpression v2Ref = new VariableReferenceExpression(v2);
                    v2Ref.setSourceLocation(g.getSourceLocation());
                    g.getDecorList().add(new Pair<>(oldVar, new MutableObject<>(v2Ref)));
                    changed = true;
                    assignOp.getVariables().set(0, v2);
                    VariableUtilities.substituteVariables(assignOp, m.first, m.second, context);
                }
                if (changed) {
                    context.computeAndSetTypeEnvironmentForOperator(g);
                }
                usedInAccess.clear();
                VariableUtilities.getUsedVariables(assignOp, usedInAccess);
                pushItDown = true;
            }
        }
        if (pushItDown) {
            if (inputOp.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                Mutable<ILogicalOperator> childOfSubplan =
                        ((NestedTupleSourceOperator) inputOp).getDataSourceReference().getValue().getInputs().get(0);
                pushAccessDown(opRef, inputOp, childOfSubplan, context, finalAnnot);
                return true;
            }
            if (inputOp.getInputs().size() == 1 && !inputOp.hasNestedPlans()) {
                pushAccessDown(opRef, inputOp, inputOp.getInputs().get(0), context, finalAnnot);
                return true;
            } else {
                for (Mutable<ILogicalOperator> inp : inputOp.getInputs()) {
                    HashSet<LogicalVariable> v2 = new HashSet<>();
                    VariableUtilities.getLiveVariables(inp.getValue(), v2);
                    if (v2.containsAll(usedInAccess)) {
                        pushAccessDown(opRef, inputOp, inp, context, finalAnnot);
                        return true;
                    }
                }
            }
            if (inputOp.hasNestedPlans()) {
                AbstractOperatorWithNestedPlans nestedOp = (AbstractOperatorWithNestedPlans) inputOp;
                for (ILogicalPlan plan : nestedOp.getNestedPlans()) {
                    for (Mutable<ILogicalOperator> root : plan.getRoots()) {
                        HashSet<LogicalVariable> v2 = new HashSet<>();
                        VariableUtilities.getLiveVariables(root.getValue(), v2);
                        if (v2.containsAll(usedInAccess)) {
                            pushAccessDown(opRef, inputOp, root, context, finalAnnot);
                            return true;
                        }
                    }
                }
            }
            return false;
        } else {
            // check if the accessed field is one of the partitioning key fields. If yes, we can equate the 2 variables
            if (inputOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                DataSourceScanOperator scan = (DataSourceScanOperator) inputOp;
                IDataSource<DataSourceId> dataSource = (IDataSource<DataSourceId>) scan.getDataSource();
                byte dsType = ((DataSource) dataSource).getDatasourceType();
                if (dsType != DataSource.Type.INTERNAL_DATASET && dsType != DataSource.Type.EXTERNAL_DATASET) {
                    return false;
                }
                DataSourceId asid = dataSource.getId();
                MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();
                Dataset dataset = mp.findDataset(asid.getDataverseName(), asid.getDatasourceName());
                if (dataset == null) {
                    throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, scan.getSourceLocation(),
                            asid.getDatasourceName(), asid.getDataverseName());
                }
                if (dataset.getDatasetType() != DatasetType.INTERNAL) {
                    setAsFinal(assignOp, context, finalAnnot);
                    return false;
                }

                List<LogicalVariable> allVars = scan.getVariables();
                LogicalVariable dataRecVarInScan = ((DataSource) dataSource).getDataRecordVariable(allVars);
                LogicalVariable metaRecVarInScan = ((DataSource) dataSource).getMetaVariable(allVars);

                // data part
                String dataTypeName = dataset.getItemTypeName();
                IAType dataType = mp.findType(dataset.getItemTypeDataverseName(), dataTypeName);
                if (dataType.getTypeTag() != ATypeTag.OBJECT) {
                    return false;
                }
                ARecordType dataRecType = (ARecordType) dataType;
                Pair<ILogicalExpression, List<String>> fieldPathAndVar = getFieldExpression(assignOp, dataRecType);
                ILogicalExpression targetRecVar = fieldPathAndVar.first;
                List<String> targetFieldPath = fieldPathAndVar.second;
                boolean rewrite = false;
                boolean fieldFromMeta = false;
                if (sameRecords(targetRecVar, dataRecVarInScan)) {
                    rewrite = true;
                } else {
                    // check meta part
                    IAType metaType = mp.findMetaType(dataset); // could be null
                    if (metaType != null && metaType.getTypeTag() == ATypeTag.OBJECT) {
                        fieldPathAndVar = getFieldExpression(assignOp, (ARecordType) metaType);
                        targetRecVar = fieldPathAndVar.first;
                        targetFieldPath = fieldPathAndVar.second;
                        if (sameRecords(targetRecVar, metaRecVarInScan)) {
                            rewrite = true;
                            fieldFromMeta = true;
                        }
                    }
                }

                if (rewrite) {
                    int p = DatasetUtil.getPositionOfPartitioningKeyField(dataset, targetFieldPath, fieldFromMeta);
                    if (p < 0) { // not one of the partitioning fields
                        setAsFinal(assignOp, context, finalAnnot);
                        return false;
                    }
                    LogicalVariable keyVar = scan.getVariables().get(p);
                    VariableReferenceExpression keyVarRef = new VariableReferenceExpression(keyVar);
                    keyVarRef.setSourceLocation(targetRecVar.getSourceLocation());
                    assignOp.getExpressions().get(0).setValue(keyVarRef);
                    return true;
                }
            }
            setAsFinal(assignOp, context, finalAnnot);
            return false;
        }
    }

    /**
     * @param recordInAssign the variable reference expression in assign op
     * @param recordInScan the record (payload) variable in scan op
     * @return true if the expression in the assign op is a variable and that variable = record variable in scan op
     */
    private boolean sameRecords(ILogicalExpression recordInAssign, LogicalVariable recordInScan) {
        return recordInAssign != null && recordInAssign.getExpressionTag() == LogicalExpressionTag.VARIABLE
                && ((VariableReferenceExpression) recordInAssign).getVariableReference().equals(recordInScan);
    }

    private Pair<ILogicalExpression, List<String>> getFieldExpression(AssignOperator access, ARecordType rt) {
        LinkedList<String> fieldPath = new LinkedList<>();
        ILogicalExpression e0 = access.getExpressions().get(0).getValue();
        while (AnalysisUtil.isAccessToFieldRecord(e0)) {
            ILogicalExpression e1 = ((AbstractFunctionCallExpression) e0).getArguments().get(1).getValue();
            if (e1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                return new Pair<>(null, null);
            }
            ConstantExpression ce = (ConstantExpression) e1;
            IAObject obj = ((AsterixConstantValue) ce.getValue()).getObject();
            String fldName;
            if (obj.getType().getTypeTag() == ATypeTag.STRING) {
                fldName = ((AString) obj).getStringValue();
            } else {
                int pos = ((AInt32) obj).getIntegerValue();
                if (pos >= rt.getFieldNames().length) {
                    return new Pair<>(null, null);
                }
                fldName = rt.getFieldNames()[pos];
            }
            fieldPath.addFirst(fldName);
            e0 = ((AbstractFunctionCallExpression) e0).getArguments().get(0).getValue();

        }
        return new Pair<>(e0, fieldPath);
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
        ILogicalExpression accessExpr0 = getFirstExpr(access);
        if (accessExpr0.equals(getFirstExpr(a2))) {
            VariableReferenceExpression varRef = new VariableReferenceExpression(a2.getVariables().get(0));
            varRef.setSourceLocation(accessExpr0.getSourceLocation());
            access.getExpressions().get(0).setValue(varRef);
            return true;
        } else {
            return false;
        }
    }

    // indirect recursivity with pushDownFieldAccessRec
    private void pushAccessDown(Mutable<ILogicalOperator> fldAccessOpRef, ILogicalOperator op2,
            Mutable<ILogicalOperator> inputOfOp2, IOptimizationContext context, String finalAnnot)
            throws AlgebricksException {
        ILogicalOperator fieldAccessOp = fldAccessOpRef.getValue();
        fldAccessOpRef.setValue(op2);
        List<Mutable<ILogicalOperator>> faInpList = fieldAccessOp.getInputs();
        faInpList.clear();
        faInpList.add(new MutableObject<>(inputOfOp2.getValue()));
        inputOfOp2.setValue(fieldAccessOp);
        // typing
        context.computeAndSetTypeEnvironmentForOperator(fieldAccessOp);
        context.computeAndSetTypeEnvironmentForOperator(op2);
        pushDownFieldAccessRec(inputOfOp2, context, finalAnnot);
    }

    private ILogicalExpression getFirstExpr(AssignOperator assign) {
        return assign.getExpressions().get(0).getValue();
    }

}
