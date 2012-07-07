package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;

/**
 * Class for helping rewrite rules to choose and apply RTree indexes.
 */
public class RTreeAccessMethod implements IAccessMethod {

    private static List<FunctionIdentifier> funcIdents = new ArrayList<FunctionIdentifier>();
    static {
        funcIdents.add(AsterixBuiltinFunctions.SPATIAL_INTERSECT);
    }

    public static RTreeAccessMethod INSTANCE = new RTreeAccessMethod();

    @Override
    public List<FunctionIdentifier> getOptimizableFunctions() {
        return funcIdents;
    }

    @Override
    public boolean analyzeFuncExprArgs(AbstractFunctionCallExpression funcExpr, List<AssignOperator> assigns,
            AccessMethodAnalysisContext analysisCtx) {
        return AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVar(funcExpr, analysisCtx);
    }

    @Override
    public boolean matchAllIndexExprs() {
        return true;
    }

    @Override
    public boolean matchPrefixIndexExprs() {
        return false;
    }

    @Override
    public boolean applySelectPlanTransformation(Mutable<ILogicalOperator> selectRef,
            OptimizableOperatorSubTree subTree, Index index, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context) throws AlgebricksException {
        Dataset dataset = subTree.dataset;
        ARecordType recordType = subTree.recordType;
        // TODO: We can probably do something smarter here based on selectivity or MBR area.
        // Pick the first expr optimizable by this index.
        List<Integer> indexExprs = analysisCtx.getIndexExprs(index);
        int firstExprIndex = indexExprs.get(0);
        IOptimizableFuncExpr optFuncExpr = analysisCtx.matchedFuncExprs.get(firstExprIndex);

        // Get the number of dimensions corresponding to the field indexed by
        // chosenIndex.
        Pair<IAType, Boolean> keyPairType = Index.getNonNullableKeyFieldType(optFuncExpr.getFieldName(0), recordType);
        IAType spatialType = keyPairType.first;
        int numDimensions = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        int numSecondaryKeys = numDimensions * 2;

        DataSourceScanOperator dataSourceScan = subTree.dataSourceScan;
        // TODO: For now retainInput and requiresBroadcast are always false.
        RTreeJobGenParams jobGenParams = new RTreeJobGenParams(index.getIndexName(), IndexType.RTREE,
                dataset.getDatasetName(), false, false);
        // A spatial object is serialized in the constant of the func expr we are optimizing.
        // The R-Tree expects as input an MBR represented with 1 field per dimension. 
        // Here we generate vars and funcs for extracting MBR fields from the constant into fields of a tuple (as the R-Tree expects them).
        // List of variables for the assign.
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        // List of expressions for the assign.
        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
        for (int i = 0; i < numSecondaryKeys; i++) {
            // The create MBR function "extracts" one field of an MBR around the given spatial object.
            AbstractFunctionCallExpression createMBR = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.CREATE_MBR));
            // Spatial object is the constant from the func expr we are optimizing.
            createMBR.getArguments().add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(optFuncExpr.getConstantVal(0))));
            // The number of dimensions.
            createMBR.getArguments().add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(
                            numDimensions)))));
            // Which part of the MBR to extract.
            createMBR.getArguments().add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(
                            new AsterixConstantValue(new AInt32(i)))));
            // Add a variable and its expr to the lists which will be passed into an assign op.
            LogicalVariable keyVar = context.newVar();
            keyVarList.add(keyVar);
            keyExprList.add(new MutableObject<ILogicalExpression>(createMBR));
        }
        jobGenParams.setKeyVarList(keyVarList);

        // Assign operator that "extracts" the MBR fields from the func-expr constant into a tuple.
        AssignOperator assignSearchKeys = new AssignOperator(keyVarList, keyExprList);
        // Input to this assign is the EmptyTupleSource (which the dataSourceScan also must have had as input).
        assignSearchKeys.getInputs().add(dataSourceScan.getInputs().get(0));
        assignSearchKeys.setExecutionMode(dataSourceScan.getExecutionMode());

        UnnestMapOperator secondaryIndexUnnestOp = AccessMethodUtils.createSecondaryIndexUnnestMap(dataset, recordType,
                index, assignSearchKeys, jobGenParams, context, false, false);
        // Generate the rest of the upstream plan which feeds the search results into the primary index.
        UnnestMapOperator primaryIndexUnnestOp = AccessMethodUtils.createPrimaryIndexUnnestMap(dataSourceScan, dataset,
                recordType, secondaryIndexUnnestOp, context, true, false, false);
        // Replace the datasource scan with the new plan rooted at primaryIndexUnnestMap.
        subTree.dataSourceScanRef.setValue(primaryIndexUnnestOp);
        return true;
    }

    @Override
    public boolean applyJoinPlanTransformation(Mutable<ILogicalOperator> joinRef,
            OptimizableOperatorSubTree leftSubTree, OptimizableOperatorSubTree rightSubTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context) throws AlgebricksException {
        // TODO Implement this.
        return false;
    }

    @Override
    public boolean exprIsOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) {
        // No additional analysis required.
        return true;
    }
}
