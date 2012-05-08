package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl.IndexKind;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlDataSource;
import edu.uci.ics.asterix.metadata.declared.AqlIndex;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class IntroduceSecondaryIndexInsertDeleteRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getValue();
        if (op0.getOperatorTag() != LogicalOperatorTag.SINK) {
            return false;
        }
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) op0.getInputs().get(0).getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE) {
            return false;
        }

        FunctionIdentifier fid = null;
        AbstractLogicalOperator op2 = op1;
        List<LogicalVariable> recordVar = new ArrayList<LogicalVariable>();
        while (fid != AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR) {
            if (op2.getInputs().size() == 0)
                return false;
            op2 = (AbstractLogicalOperator) op2.getInputs().get(0).getValue();
            if (op2.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
                continue;
            } else {
                AssignOperator assignOp = (AssignOperator) op2;
                ILogicalExpression assignExpr = assignOp.getExpressions().get(0).getValue();
                if (assignExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    ScalarFunctionCallExpression funcExpr = (ScalarFunctionCallExpression) assignOp.getExpressions()
                            .get(0).getValue();
                    fid = funcExpr.getFunctionIdentifier();
                }
            }
        }
        AssignOperator assignOp2 = (AssignOperator) op2;
        recordVar.addAll(assignOp2.getVariables());
        InsertDeleteOperator insertOp = (InsertDeleteOperator) op1;
        AqlDataSource datasetSource = (AqlDataSource) insertOp.getDataSource();
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations metadata = mp.getMetadataDeclarations();
        String datasetName = datasetSource.getId().getDatasetName();
        AqlCompiledDatasetDecl adecl = metadata.findDataset(datasetName);
        if (adecl == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        if (adecl.getDatasetType() == DatasetType.EXTERNAL)
            return false;

        List<LogicalVariable> projectVars = new ArrayList<LogicalVariable>();
        VariableUtilities.getUsedVariables(op1, projectVars);
        // create operators for secondary index insert
        String itemTypeName = adecl.getItemTypeName();
        IAType itemType = metadata.findType(itemTypeName);
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Only record types can be indexed.");
        }
        ARecordType recType = (ARecordType) itemType;
        List<AqlCompiledIndexDecl> secondaryIndexes = DatasetUtils.getSecondaryIndexes(adecl);
        if (secondaryIndexes.size() <= 0)
            return false;
        ILogicalOperator currentTop = op1;
        for (AqlCompiledIndexDecl index : secondaryIndexes) {
            List<String> secondaryKeyFields = index.getFieldExprs();
            List<LogicalVariable> secondaryKeyVars = new ArrayList<LogicalVariable>();
            List<Mutable<ILogicalExpression>> expressions = new ArrayList<Mutable<ILogicalExpression>>();
            List<Mutable<ILogicalExpression>> secondaryExpressions = new ArrayList<Mutable<ILogicalExpression>>();
            for (String secondaryKey : secondaryKeyFields) {
                Mutable<ILogicalExpression> varRef = new MutableObject<ILogicalExpression>(
                        new VariableReferenceExpression(recordVar.get(0)));
                String[] fieldNames = recType.getFieldNames();
                int pos = -1;
                for (int j = 0; j < fieldNames.length; j++)
                    if (fieldNames[j].equals(secondaryKey))
                        pos = j;
                Mutable<ILogicalExpression> indexRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                        new AsterixConstantValue(new AInt32(pos))));
                AbstractFunctionCallExpression func = new ScalarFunctionCallExpression(
                        FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX), varRef, indexRef);
                expressions.add(new MutableObject<ILogicalExpression>(func));
                LogicalVariable newVar = context.newVar();
                secondaryKeyVars.add(newVar);
            }

            AssignOperator assign = new AssignOperator(secondaryKeyVars, expressions);
            ProjectOperator project = new ProjectOperator(projectVars);
            if (index.getKind() == IndexKind.BTREE) {
                for (LogicalVariable secondaryKeyVar : secondaryKeyVars)
                    secondaryExpressions.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                            secondaryKeyVar)));
                AqlIndex dataSourceIndex = new AqlIndex(index, metadata, datasetName);
                IndexInsertDeleteOperator indexUpdate = new IndexInsertDeleteOperator(dataSourceIndex,
                        insertOp.getPrimaryKeyExpressions(), secondaryExpressions, insertOp.getOperation());
                indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(assign));
                assign.getInputs().add(new MutableObject<ILogicalOperator>(project));
                project.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
                currentTop = indexUpdate;
                context.computeAndSetTypeEnvironmentForOperator(project);
                context.computeAndSetTypeEnvironmentForOperator(assign);
                context.computeAndSetTypeEnvironmentForOperator(indexUpdate);
            } else if (index.getKind() == IndexKind.RTREE) {
                IAType spatialType = null;
                for (String secondaryKey : secondaryKeyFields) {
                    spatialType = keyFieldType(secondaryKey, recType);
                }
                int dimension = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
                int numKeys = dimension * 2;
                List<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
                List<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
                for (int i = 0; i < numKeys; i++) {
                    LogicalVariable keyVar = context.newVar();
                    keyVarList.add(keyVar);
                    AbstractFunctionCallExpression createMBR = new ScalarFunctionCallExpression(
                            FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.CREATE_MBR));
                    createMBR.getArguments().add(
                            new MutableObject<ILogicalExpression>(new VariableReferenceExpression(secondaryKeyVars
                                    .get(0))));
                    createMBR.getArguments().add(
                            new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                                    new AInt32(dimension)))));
                    createMBR.getArguments().add(
                            new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                                    new AInt32(i)))));
                    keyExprList.add(new MutableObject<ILogicalExpression>(createMBR));
                }
                for (LogicalVariable secondaryKeyVar : keyVarList)
                    secondaryExpressions.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                            secondaryKeyVar)));
                AqlIndex dataSourceIndex = new AqlIndex(index, metadata, datasetName);
                IndexInsertDeleteOperator indexUpdate = new IndexInsertDeleteOperator(dataSourceIndex,
                        insertOp.getPrimaryKeyExpressions(), secondaryExpressions, insertOp.getOperation());
                AssignOperator assignCoordinates = new AssignOperator(keyVarList, keyExprList);
                indexUpdate.getInputs().add(new MutableObject<ILogicalOperator>(assignCoordinates));
                assignCoordinates.getInputs().add(new MutableObject<ILogicalOperator>(assign));
                assign.getInputs().add(new MutableObject<ILogicalOperator>(project));
                project.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
                currentTop = indexUpdate;
                context.computeAndSetTypeEnvironmentForOperator(project);
                context.computeAndSetTypeEnvironmentForOperator(assign);
                context.computeAndSetTypeEnvironmentForOperator(assignCoordinates);
                context.computeAndSetTypeEnvironmentForOperator(indexUpdate);
            }

        }
        op0.getInputs().clear();
        op0.getInputs().add(new MutableObject<ILogicalOperator>(currentTop));
        return true;
    }

    public static IAType keyFieldType(String expr, ARecordType recType) throws AlgebricksException {
        String[] names = recType.getFieldNames();
        int n = names.length;
        for (int i = 0; i < n; i++) {
            if (names[i].equals(expr)) {
                return recType.getFieldTypes()[i];
            }
        }
        throw new AlgebricksException("Could not find field " + expr + " in the schema.");
    }
}
