package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.common.functions.FunctionUtils;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.optimizer.base.AnalysisUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;

public class IntroduceRTreeIndexSearchRule extends IntroduceTreeIndexSearchRule {

    /**
     * 
     * Matches spatial-intersect(var, spatialObject) , where var is bound to an
     * indexed field, and spatialObject is point, line, polygon, circle or
     * rectangle
     * 
     * @throws AlgebricksException
     * 
     */

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op1)) {
            return false;
        }
        if (op1.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }

        SelectOperator select = (SelectOperator) op1;
        ILogicalExpression expr = select.getCondition().getValue();

        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier fi = fce.getFunctionIdentifier();
            if (!AsterixBuiltinFunctions.isSpatialFilterFunction(fi) && fi != AlgebricksBuiltinFunctions.AND) {
                return false;
            }
        } else {
            return false;
        }
        ArrayList<IAlgebricksConstantValue> outFilters = new ArrayList<IAlgebricksConstantValue>();
        ArrayList<LogicalVariable> outComparedVars = new ArrayList<LogicalVariable>();

        if (!analyzeCondition(expr, outFilters, outComparedVars)) {
            return false;
        }
        Mutable<ILogicalOperator> opRef2 = op1.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();

        DataSourceScanOperator scanDataset;
        Mutable<ILogicalOperator> opRef3;
        AssignOperator assignFieldAccess = null;

        if (op2.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            assignFieldAccess = (AssignOperator) op2;
            opRef3 = op2.getInputs().get(0);
            AbstractLogicalOperator op3 = (AbstractLogicalOperator) opRef3.getValue();
            if (op3.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
                return false;
            }
            scanDataset = (DataSourceScanOperator) op3;
        } else {
            return false;
        }

        String datasetName = AnalysisUtil.getDatasetName(scanDataset);
        if (datasetName == null) {
            return false;
        }
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations metadata = mp.getMetadataDeclarations();
        AqlCompiledDatasetDecl adecl = metadata.findDataset(datasetName);
        if (adecl == null) {
            throw new AlgebricksException("No metadata for dataset " + datasetName);
        }
        if (adecl.getDatasetType() != DatasetType.INTERNAL && adecl.getDatasetType() != DatasetType.FEED) {
            return false;
        }
        IAType t = metadata.findType(adecl.getItemTypeName());
        if (t.getTypeTag() != ATypeTag.RECORD) {
            return false;
        }
        ARecordType recordType = (ARecordType) t;
        int fldPos = 0;
        boolean foundVar = false;
        int dimension = 0;
        AqlCompiledIndexDecl primIdxDecl = DatasetUtils.getPrimaryIndex(adecl);
        List<String> primIdxFields = primIdxDecl.getFieldExprs();

        HashMap<AqlCompiledIndexDecl, List<Pair<String, Integer>>> foundIdxExprs = new HashMap<AqlCompiledIndexDecl, List<Pair<String, Integer>>>();

        List<LogicalVariable> varList = assignFieldAccess.getVariables();

        for (LogicalVariable var : varList) {

            String fieldName = null;
            AbstractLogicalExpression exprP = (AbstractLogicalExpression) assignFieldAccess.getExpressions()
                    .get(fldPos).getValue();
            if (exprP.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) exprP;
            FunctionIdentifier fi = fce.getFunctionIdentifier();

            int fieldIndex = -1;

            if (fi == AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME) {
                ILogicalExpression nameArg = fce.getArguments().get(1).getValue();
                if (nameArg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                    return false;
                }
                ConstantExpression cNameExpr = (ConstantExpression) nameArg;
                fieldName = ((AString) ((AsterixConstantValue) cNameExpr.getValue()).getObject()).getStringValue();
            } else if (fi == AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX) {
                ILogicalExpression idxArg = fce.getArguments().get(1).getValue();
                if (idxArg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                    return false;
                }
                ConstantExpression cNameExpr = (ConstantExpression) idxArg;
                fieldIndex = ((AInt32) ((AsterixConstantValue) cNameExpr.getValue()).getObject()).getIntegerValue();
            } else {
                return false;
            }

            if (fieldName == null) {
                if (recordType.isOpen()) {
                    continue;
                }
                fieldName = recordType.getFieldNames()[fieldIndex];
            }

            foundVar = findIdxExprs(adecl, primIdxFields, primIdxDecl, foundIdxExprs, outComparedVars, var, fieldName);
            if (foundVar) {
                IAType spatialType = AqlCompiledIndexDecl.keyFieldType(fieldName, recordType);
                dimension = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
            }

            fldPos++;
        }
        if (!foundVar) {
            return false;
        }
        AqlCompiledIndexDecl picked = findUsableIndex(adecl, foundIdxExprs);
        boolean res;
        if (picked == null) {
            res = false;
        } else {
            res = pickIndex(opRef3, scanDataset, assignFieldAccess, outFilters, adecl, picked, picked == primIdxDecl,
                    context, dimension);
        }
        context.addToDontApplySet(this, op1);
        if (res) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }
        return res;
    }

    private boolean analyzeCondition(ILogicalExpression cond, List<IAlgebricksConstantValue> outFilters,
            List<LogicalVariable> outComparedVars) {
        if (cond.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) cond;
            FunctionIdentifier fi = fce.getFunctionIdentifier();
            if (AsterixBuiltinFunctions.isSpatialFilterFunction(fi)) {
                return analyzeSpatialFilterExpr(fce, outFilters, outComparedVars);
            }
            boolean found = false;

            for (Mutable<ILogicalExpression> arg : fce.getArguments()) {
                ILogicalExpression e = arg.getValue();
                if (e.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression f2 = (AbstractFunctionCallExpression) e;
                    if (AsterixBuiltinFunctions.isSpatialFilterFunction(f2.getFunctionIdentifier())) {
                        if (analyzeSpatialFilterExpr(f2, outFilters, outComparedVars)) {
                            found = true;
                        }
                    }
                }
            }
            return found;
        } else {
            throw new IllegalStateException();
        }
    }

    private boolean analyzeSpatialFilterExpr(AbstractFunctionCallExpression ce,
            List<IAlgebricksConstantValue> outFilters, List<LogicalVariable> outComparedVars) {
        IAlgebricksConstantValue constFilterVal = null;
        LogicalVariable fldVar = null;
        ILogicalExpression arg1 = ce.getArguments().get(0).getValue();
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ConstantExpression ce1 = (ConstantExpression) arg1;
            constFilterVal = ce1.getValue();
        } else if (arg1.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            VariableReferenceExpression ve1 = (VariableReferenceExpression) arg1;
            fldVar = ve1.getVariableReference();
        } else {
            return false;
        }
        ILogicalExpression arg2 = ce.getArguments().get(1).getValue();
        if (arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            if (constFilterVal != null) {
                return false;
            }
            ConstantExpression ce2 = (ConstantExpression) arg2;
            constFilterVal = ce2.getValue();
        } else if (arg2.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            if (fldVar != null) {
                return false;
            }
            VariableReferenceExpression ve2 = (VariableReferenceExpression) arg2;
            fldVar = ve2.getVariableReference();
        } else {
            return false;
        }

        if (constFilterVal == null || fldVar == null) {
            return false;
        }

        outFilters.add(constFilterVal);
        outComparedVars.add(fldVar);
        return true;
    }

    private boolean pickIndex(Mutable<ILogicalOperator> opRef3, DataSourceScanOperator scanDataset,
            AssignOperator assignFieldAccess, ArrayList<IAlgebricksConstantValue> filters,
            AqlCompiledDatasetDecl ddecl, AqlCompiledIndexDecl picked, boolean isPrimaryIdx,
            IOptimizationContext context, int dimension) throws AlgebricksException {
        int numKeys = dimension * 2;

        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        ArrayList<Mutable<ILogicalExpression>> rangeSearchFunArgs = new ArrayList<Mutable<ILogicalExpression>>();
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(mkStrConstExpr(picked.getIndexName())));
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(mkStrConstExpr(FunctionArgumentsConstants.RTREE_INDEX)));
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(mkStrConstExpr(ddecl.getName())));

        Mutable<ILogicalExpression> nkRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                new AsterixConstantValue(new AInt32(numKeys))));
        rangeSearchFunArgs.add(nkRef);
        for (int i = 0; i < numKeys; i++) {
            LogicalVariable keyVar = context.newVar();
            keyVarList.add(keyVar);

            AbstractFunctionCallExpression createMBR = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.CREATE_MBR));
            createMBR.getArguments().add(new MutableObject<ILogicalExpression>(new ConstantExpression(filters.get(0))));
            createMBR.getArguments().add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                            new AInt32(dimension)))));
            createMBR.getArguments().add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(i)))));
            keyExprList.add(new MutableObject<ILogicalExpression>(createMBR));
            Mutable<ILogicalExpression> keyVarRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                    keyVar));
            rangeSearchFunArgs.add(keyVarRef);
        }

        AssignOperator assignSearchKeys = new AssignOperator(keyVarList, keyExprList);
        assignSearchKeys.getInputs().add(scanDataset.getInputs().get(0));
        assignSearchKeys.setExecutionMode(scanDataset.getExecutionMode());

        IFunctionInfo finfo = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        UnnestingFunctionCallExpression rangeSearchFun = new UnnestingFunctionCallExpression(finfo, rangeSearchFunArgs);
        rangeSearchFun.setReturnsUniqueValues(true);

        List<LogicalVariable> primIdxVarList = scanDataset.getVariables();
        int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(ddecl).size();

        UnnestMapOperator primIdxUnnestMap;
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations metadata = mp.getMetadataDeclarations();

        String itemTypeName = ddecl.getItemTypeName();
        ARecordType itemType = (ARecordType) metadata.findType(itemTypeName);
        if (!isPrimaryIdx) {
            ArrayList<LogicalVariable> secIdxPrimKeysVarList = new ArrayList<LogicalVariable>(numPrimaryKeys);
            for (int i = 0; i < numPrimaryKeys; i++) {
                secIdxPrimKeysVarList.add(context.newVar());
            }
            ArrayList<LogicalVariable> secIdxUnnestVars = new ArrayList<LogicalVariable>(numKeys
                    + secIdxPrimKeysVarList.size());
            for (int i = 0; i < numKeys; i++) {
                secIdxUnnestVars.add(context.newVar());
            }
            secIdxUnnestVars.addAll(secIdxPrimKeysVarList);
            UnnestMapOperator secIdxUnnest = new UnnestMapOperator(secIdxUnnestVars, new MutableObject<ILogicalExpression>(
                    rangeSearchFun), secondaryIndexTypes(ddecl, picked, itemType, numKeys));
            secIdxUnnest.getInputs().add(new MutableObject<ILogicalOperator>(assignSearchKeys));
            secIdxUnnest.setExecutionMode(ExecutionMode.PARTITIONED);

            OrderOperator order = new OrderOperator();
            for (LogicalVariable v : secIdxPrimKeysVarList) {
                Mutable<ILogicalExpression> vRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v));
                order.getOrderExpressions().add(
                        new Pair<IOrder, Mutable<ILogicalExpression>>(OrderOperator.ASC_ORDER, vRef));
            }
            order.getInputs().add(new MutableObject<ILogicalOperator>(secIdxUnnest));
            order.setExecutionMode(ExecutionMode.LOCAL);

            List<Mutable<ILogicalExpression>> argList2 = new ArrayList<Mutable<ILogicalExpression>>();
            argList2.add(new MutableObject<ILogicalExpression>(mkStrConstExpr(ddecl.getName())));
            argList2.add(new MutableObject<ILogicalExpression>(mkStrConstExpr(FunctionArgumentsConstants.BTREE_INDEX)));
            argList2.add(new MutableObject<ILogicalExpression>(mkStrConstExpr(ddecl.getName())));
            argList2.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(
                    numPrimaryKeys)))));
            for (LogicalVariable v : secIdxPrimKeysVarList) {
                argList2.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v)));
            }
            argList2.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(
                    numPrimaryKeys)))));
            for (LogicalVariable v : secIdxPrimKeysVarList) {
                argList2.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v)));
            }
            argList2.add(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
            argList2.add(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
            IFunctionInfo finfoSearch2 = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
            AbstractFunctionCallExpression searchPrimIdxFun = new ScalarFunctionCallExpression(finfoSearch2, argList2);
            primIdxUnnestMap = new UnnestMapOperator(primIdxVarList, new MutableObject<ILogicalExpression>(searchPrimIdxFun),
                    primaryIndexTypes(metadata, ddecl, itemType));
            primIdxUnnestMap.getInputs().add(new MutableObject<ILogicalOperator>(order));

        } else {
            return false;
        }

        primIdxUnnestMap.setExecutionMode(ExecutionMode.PARTITIONED);
        opRef3.setValue(primIdxUnnestMap);

        return true;
    }

    private static List<Object> secondaryIndexTypes(AqlCompiledDatasetDecl ddecl, AqlCompiledIndexDecl acid,
            ARecordType itemType, int numKeys) throws AlgebricksException {
        List<Object> types = new ArrayList<Object>();
        IAType keyType = AqlCompiledIndexDecl.keyFieldType(acid.getFieldExprs().get(0), itemType);
        IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(keyType.getTypeTag());

        for (int i = 0; i < numKeys; i++) {
            types.add(nestedKeyType);
        }
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> t : DatasetUtils
                .getPartitioningFunctions(ddecl)) {
            types.add(t.third);
        }
        return types;
    }
}
