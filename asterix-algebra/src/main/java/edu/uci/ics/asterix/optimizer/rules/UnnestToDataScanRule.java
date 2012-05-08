package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledFeedDatasetDetails;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlDataSource;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.metadata.declared.ExternalFeedDataSource;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

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
                String datasetName = ((AString) acv2.getObject()).getStringValue();

                AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
                AqlCompiledMetadataDeclarations metadata = mp.getMetadataDeclarations();
                AqlCompiledDatasetDecl acdd = metadata.findDataset(datasetName);
                if (acdd == null) {
                    throw new AlgebricksException("Could not find dataset " + datasetName);
                }

                AqlSourceId asid = new AqlSourceId(metadata.getDataverseName(), datasetName);

                ArrayList<LogicalVariable> v = new ArrayList<LogicalVariable>();

                if (acdd.getDatasetType() == DatasetType.INTERNAL || acdd.getDatasetType() == DatasetType.FEED) {

                    int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(acdd).size();
                    for (int i = 0; i < numPrimaryKeys; i++) {
                        v.add(context.newVar());
                    }
                }
                v.add(unnest.getVariable());

                DataSourceScanOperator scan = new DataSourceScanOperator(v, mp.findDataSource(asid));
                List<Mutable<ILogicalOperator>> scanInpList = scan.getInputs();
                scanInpList.addAll(unnest.getInputs());
                opRef.setValue(scan);
                addPrimaryKey(v, context);
                context.computeAndSetTypeEnvironmentForOperator(scan);

                return true;
            }
            
            if (fid.equals(AsterixBuiltinFunctions.FEED_INGEST)) {
                if (unnest.getPositionalVariable() != null) {
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
                String datasetName = ((AString) acv2.getObject()).getStringValue();

                AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
                AqlCompiledMetadataDeclarations metadata = mp.getMetadataDeclarations();
                AqlCompiledDatasetDecl acdd = metadata.findDataset(datasetName);

                if (acdd == null) {
                    throw new AlgebricksException("Could not find dataset " + datasetName);
                }

                if (acdd.getDatasetType() != DatasetType.FEED) {
                    throw new IllegalArgumentException("invalid dataset type:" + acdd.getDatasetType());
                }

                AqlSourceId asid = new AqlSourceId(metadata.getDataverseName(), datasetName);

                ArrayList<LogicalVariable> v = new ArrayList<LogicalVariable>();

                /*
                int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(acdd).size();
                for (int i = 0; i < numPrimaryKeys; i++) {
                    v.add(context.newVar());
                }*/

                v.add(unnest.getVariable());

                DataSourceScanOperator scan = new DataSourceScanOperator(v, createDummyFeedDataSource(asid, acdd,
                        metadata));

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

    private AqlDataSource createDummyFeedDataSource(AqlSourceId aqlId, AqlCompiledDatasetDecl acdd,
            AqlCompiledMetadataDeclarations metadata) throws AlgebricksException {

        AqlCompiledFeedDatasetDetails feedDetails = (AqlCompiledFeedDatasetDetails) acdd.getAqlCompiledDatasetDetails();

        if (!aqlId.getDataverseName().equals(metadata.getDataverseName())) {
            return null;
        }

        String tName = acdd.getItemTypeName();
        IAType itemType;
        try {
            itemType = metadata.findType(tName);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }

        AqlCompiledDatasetDecl dummySourceDecl = new AqlCompiledDatasetDecl(acdd.getName(), tName,
                DatasetType.EXTERNAL, feedDetails);

        ExternalFeedDataSource extDataSource = new ExternalFeedDataSource(aqlId, dummySourceDecl, itemType,
                AqlDataSource.AqlDataSourceType.EXTERNAL_FEED);
        return extDataSource;
    }
    
    public void addPrimaryKey(List<LogicalVariable> scanVariables, IOptimizationContext context) {
        int n = scanVariables.size();
        List<LogicalVariable> head = new ArrayList<LogicalVariable>(scanVariables.subList(0, n - 1));
        List<LogicalVariable> tail = new ArrayList<LogicalVariable>(1);
        tail.add(scanVariables.get(n - 1));
        FunctionalDependency pk = new FunctionalDependency(head, tail);
        context.addPrimaryKey(pk);
    }
}
