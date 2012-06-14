package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
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
import edu.uci.ics.asterix.optimizer.base.AnalysisUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;

public class IntroduceBTreeIndexSearchRule extends IntroduceTreeIndexSearchRule {

    private enum LimitType {
        LOW_INCLUSIVE, LOW_EXCLUSIVE, HIGH_INCLUSIVE, HIGH_EXCLUSIVE, EQUAL
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    /**
     * 
     * Matches one equality of the type var EQ const, where var is bound to an
     * indexed field.
     * 
     * @throws AlgebricksException
     * 
     */

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getValue();
        if (op0.getOperatorTag() == LogicalOperatorTag.SELECT) {
            return false;
        }
        List<Mutable<ILogicalOperator>> children = op0.getInputs();
        if (children == null || children.size() < 1) {
            return false;
        }
        Mutable<ILogicalOperator> opRef1 = children.get(0);
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef1.getValue();
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
            if (!AlgebricksBuiltinFunctions.isComparisonFunction(fi) && fi != AlgebricksBuiltinFunctions.AND) {
                return false;
            }
        } else {
            return false;
        }

        ArrayList<IAlgebricksConstantValue> outFilters = new ArrayList<IAlgebricksConstantValue>();
        ArrayList<LogicalVariable> outComparedVars = new ArrayList<LogicalVariable>();
        ArrayList<LimitType> outLimits = new ArrayList<LimitType>();
        ArrayList<Mutable<ILogicalExpression>> outRest = new ArrayList<Mutable<ILogicalExpression>>();
        ArrayList<Integer> foundedExprList = new ArrayList<Integer>();
        if (!analyzeCondition(expr, outFilters, outComparedVars, outLimits, outRest, foundedExprList)) {
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
        } else if (op2.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            scanDataset = (DataSourceScanOperator) op2;
            opRef3 = opRef2;
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

        AqlCompiledIndexDecl primIdxDecl = DatasetUtils.getPrimaryIndex(adecl);
        List<String> primIdxFields = primIdxDecl.getFieldExprs();

        HashMap<AqlCompiledIndexDecl, List<Pair<String, Integer>>> foundIdxExprs = new HashMap<AqlCompiledIndexDecl, List<Pair<String, Integer>>>();

        List<LogicalVariable> varList = (assignFieldAccess != null) ? assignFieldAccess.getVariables() : scanDataset
                .getVariables();

        for (LogicalVariable var : varList) {

            String fieldName = null;
            if (assignFieldAccess != null) {
                AbstractLogicalExpression exprP = (AbstractLogicalExpression) assignFieldAccess.getExpressions()
                        .get(fldPos).getValue();
                if (exprP.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                    continue;
                }
                AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) exprP;
                FunctionIdentifier fi = fce.getFunctionIdentifier();

                int fieldIndex = -1;
                if (fi.equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
                    ILogicalExpression nameArg = fce.getArguments().get(1).getValue();
                    if (nameArg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                        return false;
                    }
                    ConstantExpression cNameExpr = (ConstantExpression) nameArg;
                    fieldName = ((AString) ((AsterixConstantValue) cNameExpr.getValue()).getObject()).getStringValue();
                } else if (fi.equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
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
                    fieldName = recordType.getFieldNames()[fieldIndex];
                }
            } else { // it is a scan, not an assign
                if (fldPos >= varList.size() - 1) {
                    // the last var. is the record itself, so skip it
                    break;
                }
                // so the variable value is one of the partitioning fields
                fieldName = DatasetUtils.getPartitioningExpressions(adecl).get(fldPos);
            }
            foundVar = findIdxExprs(adecl, primIdxFields, primIdxDecl, foundIdxExprs, outComparedVars, var, fieldName);
            if (foundVar) {
                break;
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
            res = pickIndex(opRef1, opRef3, scanDataset, assignFieldAccess, outFilters, outLimits, adecl, picked,
                    picked == primIdxDecl, foundIdxExprs, context, outRest, foundedExprList);
        }
        context.addToDontApplySet(this, op1);
        if (res) {
            OperatorPropertiesUtil.typeOpRec(opRef1, context);
        }
        return res;
    }

    private boolean analyzeCondition(ILogicalExpression cond, List<IAlgebricksConstantValue> outFilters,
            List<LogicalVariable> outComparedVars, List<LimitType> outLimits, List<Mutable<ILogicalExpression>> outRest,
            List<Integer> foundedExprList) {
        if (cond.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) cond;
            FunctionIdentifier fi = fce.getFunctionIdentifier();
            if (AlgebricksBuiltinFunctions.isComparisonFunction(fi)) {
                return analyzeComparisonExpr(fce, outFilters, outComparedVars, outLimits);
            }
            boolean found = false;
            int i = 0;
            for (Mutable<ILogicalExpression> arg : fce.getArguments()) {
                outRest.add(arg);
                ILogicalExpression e = arg.getValue();
                if (e.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression f2 = (AbstractFunctionCallExpression) e;
                    if (AlgebricksBuiltinFunctions.isComparisonFunction(f2.getFunctionIdentifier())) {
                        if (analyzeComparisonExpr(f2, outFilters, outComparedVars, outLimits)) {
                            foundedExprList.add(i);
                            found = true;
                        }
                    }
                }
                i++;
            }
            return found;
        } else {
            throw new IllegalStateException();
        }
    }

    private boolean analyzeComparisonExpr(AbstractFunctionCallExpression ce, List<IAlgebricksConstantValue> outFilters,
            List<LogicalVariable> outComparedVars, List<LimitType> outLimits) {
        IAlgebricksConstantValue constFilterVal = null;
        LogicalVariable fldVar = null;
        boolean filterIsLeft = false;
        {
            ILogicalExpression arg1 = ce.getArguments().get(0).getValue();
            if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                ConstantExpression ce1 = (ConstantExpression) arg1;
                constFilterVal = ce1.getValue();
                filterIsLeft = true;
            } else if (arg1.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression ve1 = (VariableReferenceExpression) arg1;
                fldVar = ve1.getVariableReference();
            } else {
                return false;
            }
        }

        {
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
        }

        if (constFilterVal == null || fldVar == null) {
            return false;
        }
        outFilters.add(constFilterVal);
        outComparedVars.add(fldVar);
        LimitType limit;
        ComparisonKind ck = AlgebricksBuiltinFunctions.getComparisonType(ce.getFunctionIdentifier());
        switch (ck) {
            case EQ: {
                limit = LimitType.EQUAL;
                break;
            }
            case GE: {
                limit = filterIsLeft ? LimitType.HIGH_INCLUSIVE : LimitType.LOW_INCLUSIVE;
                break;
            }
            case GT: {
                limit = filterIsLeft ? LimitType.HIGH_EXCLUSIVE : LimitType.LOW_EXCLUSIVE;
                break;
            }
            case LE: {
                limit = filterIsLeft ? LimitType.LOW_INCLUSIVE : LimitType.HIGH_INCLUSIVE;
                break;
            }
            case LT: {
                limit = filterIsLeft ? LimitType.LOW_EXCLUSIVE : LimitType.HIGH_EXCLUSIVE;
                break;
            }
            case NEQ: {
                return false;
            }
            default: {
                throw new IllegalStateException();
            }
        }
        outLimits.add(limit);
        return true;
    }

    private boolean pickIndex(Mutable<ILogicalOperator> opRef1, Mutable<ILogicalOperator> opRef3,
            DataSourceScanOperator scanDataset, AssignOperator assignFieldAccess,
            ArrayList<IAlgebricksConstantValue> filters, ArrayList<LimitType> limits, AqlCompiledDatasetDecl ddecl,
            AqlCompiledIndexDecl picked, boolean isPrimaryIdx,
            HashMap<AqlCompiledIndexDecl, List<Pair<String, Integer>>> foundIdxExprs, IOptimizationContext context,
            List<Mutable<ILogicalExpression>> outRest, List<Integer> foundedExprList) throws AlgebricksException {
        int numKeys = picked.getFieldExprs().size();
        IAlgebricksConstantValue[] loFilter = new IAlgebricksConstantValue[numKeys];
        IAlgebricksConstantValue[] hiFilter = new IAlgebricksConstantValue[numKeys];
        LimitType[] loLimit = new LimitType[numKeys];
        LimitType[] hiLimit = new LimitType[numKeys];
        boolean[] loInclusive = new boolean[numKeys];
        boolean[] hiInclusive = new boolean[numKeys];
        List<Pair<String, Integer>> psiList = foundIdxExprs.get(picked);

        boolean couldntFigureOut = false;
        for (Pair<String, Integer> psi : psiList) {
            int keyPos = indexOf(psi.first, picked.getFieldExprs());
            if (keyPos < 0) {
                throw new InternalError();
            }
            if (!outRest.isEmpty()) {
                int exprIdxToBeDeleted = foundedExprList.get(psi.second);
                outRest.set(exprIdxToBeDeleted, null);
            }
            LimitType lim = limits.get(psi.second);
            boolean out = false;
            switch (lim) {
                case EQUAL: {
                    if (loLimit[keyPos] == null && hiLimit[keyPos] == null) {
                        loLimit[keyPos] = hiLimit[keyPos] = lim;
                        loInclusive[keyPos] = hiInclusive[keyPos] = true;
                        loFilter[keyPos] = hiFilter[keyPos] = filters.get(psi.second);
                    } else {
                        couldntFigureOut = true;
                    }
                    // hmmm, we would need an inference system here
                    out = true;
                    break;
                }
                case HIGH_EXCLUSIVE: {
                    if (hiLimit[keyPos] == null || (hiLimit[keyPos] != null && hiInclusive[keyPos])) {
                        hiLimit[keyPos] = lim;
                        hiFilter[keyPos] = filters.get(psi.second);
                        hiInclusive[keyPos] = false;
                    } else {
                        couldntFigureOut = true;
                        out = true;
                    }
                    break;
                }
                case HIGH_INCLUSIVE: {
                    if (hiLimit[keyPos] == null) {
                        hiLimit[keyPos] = lim;
                        hiFilter[keyPos] = filters.get(psi.second);
                        hiInclusive[keyPos] = true;
                    } else {
                        couldntFigureOut = true;
                        out = true;
                    }
                    break;
                }
                case LOW_EXCLUSIVE: {
                    if (loLimit[keyPos] == null || (loLimit[keyPos] != null && loInclusive[keyPos])) {
                        loLimit[keyPos] = lim;
                        loFilter[keyPos] = filters.get(psi.second);
                        loInclusive[keyPos] = false;
                    } else {
                        couldntFigureOut = true;
                        out = true;
                    }
                    break;
                }
                case LOW_INCLUSIVE: {
                    if (loLimit[keyPos] == null) {
                        loLimit[keyPos] = lim;
                        loFilter[keyPos] = filters.get(psi.second);
                        loInclusive[keyPos] = true;
                    } else {
                        couldntFigureOut = true;
                        out = true;
                    }
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }
            if (out) {
                break;
            }
        }
        if (couldntFigureOut) {
            return false;
        }

        // rule out the cases unsupported by the current btree search
        // implementation
        for (int i = 1; i < numKeys; i++) {
            if (loInclusive[i] != loInclusive[0] || hiInclusive[i] != hiInclusive[0]) {
                return false;
            }
            if (loLimit[0] == null && loLimit[i] != null || loLimit[0] != null && loLimit[i] == null) {
                return false;
            }
            if (hiLimit[0] == null && hiLimit[i] != null || hiLimit[0] != null && hiLimit[i] == null) {
                return false;
            }
        }
        if (loLimit[0] == null) {
            loInclusive[0] = true;
        }
        if (hiLimit[0] == null) {
            hiInclusive[0] = true;
        }

        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        ArrayList<Mutable<ILogicalExpression>> rangeSearchFunArgs = new ArrayList<Mutable<ILogicalExpression>>();
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(mkStrConstExpr(picked.getIndexName())));
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(mkStrConstExpr(FunctionArgumentsConstants.BTREE_INDEX)));
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(mkStrConstExpr(ddecl.getName())));

        if (loLimit[0] != null) {
            Mutable<ILogicalExpression> nkRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                    new AsterixConstantValue(new AInt32(numKeys))));
            rangeSearchFunArgs.add(nkRef);
            for (int i = 0; i < numKeys; i++) {
                LogicalVariable lokVar = context.newVar();
                keyVarList.add(lokVar);
                keyExprList.add(new MutableObject<ILogicalExpression>(new ConstantExpression(loFilter[i])));
                Mutable<ILogicalExpression> loRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                        lokVar));
                rangeSearchFunArgs.add(loRef);
            }
        } else {
            Mutable<ILogicalExpression> zeroRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                    new AsterixConstantValue(new AInt32(0))));
            rangeSearchFunArgs.add(zeroRef);
        }

        if (hiLimit[0] != null) {
            Mutable<ILogicalExpression> nkRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                    new AsterixConstantValue(new AInt32(numKeys))));
            rangeSearchFunArgs.add(nkRef);
            for (int i = 0; i < numKeys; i++) {
                LogicalVariable hikVar = context.newVar();
                keyVarList.add(hikVar);
                keyExprList.add(new MutableObject<ILogicalExpression>(new ConstantExpression(hiFilter[i])));
                Mutable<ILogicalExpression> hiRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                        hikVar));
                rangeSearchFunArgs.add(hiRef);
            }
        } else {
            Mutable<ILogicalExpression> zeroRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                    new AsterixConstantValue(new AInt32(0))));
            rangeSearchFunArgs.add(zeroRef);
        }

        ILogicalExpression loExpr = loInclusive[0] ? ConstantExpression.TRUE : ConstantExpression.FALSE;
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(loExpr));
        ILogicalExpression hiExpr = hiInclusive[0] ? ConstantExpression.TRUE : ConstantExpression.FALSE;
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(hiExpr));

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
                    rangeSearchFun), secondaryIndexTypes(ddecl, picked, itemType));
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
            primIdxUnnestMap = new UnnestMapOperator(primIdxVarList, new MutableObject<ILogicalExpression>(rangeSearchFun),
                    primaryIndexTypes(metadata, ddecl, itemType));
            primIdxUnnestMap.getInputs().add(new MutableObject<ILogicalOperator>(assignSearchKeys));
        }

        primIdxUnnestMap.setExecutionMode(ExecutionMode.PARTITIONED);

        validateRemainingPreds(outRest);
        if (!outRest.isEmpty()) {
            ILogicalExpression pulledCond = makeCondition(outRest, context);
            SelectOperator selectRest = new SelectOperator(new MutableObject<ILogicalExpression>(pulledCond));
            if (assignFieldAccess != null) {
                opRef3.setValue(primIdxUnnestMap);
                selectRest.getInputs().add(new MutableObject<ILogicalOperator>(assignFieldAccess));
            } else {
                selectRest.getInputs().add(new MutableObject<ILogicalOperator>(primIdxUnnestMap));
            }
            selectRest.setExecutionMode(((AbstractLogicalOperator) opRef1.getValue()).getExecutionMode());
            opRef1.setValue(selectRest);
        } else {
            primIdxUnnestMap.setExecutionMode(ExecutionMode.PARTITIONED);
            if (assignFieldAccess != null) {
                opRef3.setValue(primIdxUnnestMap);
                opRef1.setValue(assignFieldAccess);
            } else {
                opRef1.setValue(primIdxUnnestMap);
            }
        }

        return true;
    }

    private void validateRemainingPreds(List<Mutable<ILogicalExpression>> predList) {
        for (int i = 0; i < predList.size();) {
            if (predList.get(i) == null) {
                predList.remove(i);
            } else {
                i++;
            }
        }
    }

    private ILogicalExpression makeCondition(List<Mutable<ILogicalExpression>> predList, IOptimizationContext context) {
        if (predList.size() > 1) {
            IFunctionInfo finfo = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.AND);
            return new ScalarFunctionCallExpression(finfo, predList);
        } else {
            return predList.get(0).getValue();
        }
    }

    private static List<Object> secondaryIndexTypes(AqlCompiledDatasetDecl ddecl, AqlCompiledIndexDecl acid,
            ARecordType itemType) throws AlgebricksException {
        List<Object> types = new ArrayList<Object>();
        for (String sk : acid.getFieldExprs()) {
            Pair<IAType, Boolean> keyPair = AqlCompiledIndexDecl.getNonNullableKeyFieldType(sk, itemType);
            types.add(keyPair.first);
        }
        for (Triple<ICopyEvaluatorFactory, ScalarFunctionCallExpression, IAType> t : DatasetUtils
                .getPartitioningFunctions(ddecl)) {
            types.add(t.third);
        }
        return types;
    }

    private <T> int indexOf(T value, List<T> coll) {
        int i = 0;
        for (T member : coll) {
            if (member.equals(value)) {
                return i;
            }
            i++;
        }
        return -1;
    }

}