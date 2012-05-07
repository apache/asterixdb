package edu.uci.ics.asterix.algebra.operators.physical;


import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;

public class BTreeSearchPOperator extends TreeSearchPOperator {

    public BTreeSearchPOperator(IDataSourceIndex<String, AqlSourceId> idx) {
        super(idx);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.BTREE_SEARCH;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        UnnestMapOperator unnestMap = (UnnestMapOperator) op;
        ILogicalExpression unnestExpr = unnestMap.getExpressionRef().getValue();

        if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
            FunctionIdentifier fid = f.getFunctionIdentifier();
            if (fid.equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
                try {
                    contributeBtreeSearch(builder, context, unnestMap, opSchema, inputSchemas);
                } catch (AlgebricksException e) {
                    throw new AlgebricksException(e);
                }
                return;
            }
        }
        throw new IllegalStateException();
    }

    private void contributeBtreeSearch(IHyracksJobBuilder builder, JobGenContext context, UnnestMapOperator unnestMap,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas) throws AlgebricksException, AlgebricksException {
        Mutable<ILogicalExpression> unnestExpr = unnestMap.getExpressionRef();
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr.getValue();

        String idxType = getStringArgument(f, 1);
        if (idxType != FunctionArgumentsConstants.BTREE_INDEX) {
            throw new NotImplementedException(idxType + " indexes are not implemented.");
        }
        String idxName = getStringArgument(f, 0);
        String datasetName = getStringArgument(f, 2);

        Pair<int[], Integer> keysLeft = getKeys(f, 3, inputSchemas);
        Pair<int[], Integer> keysRight = getKeys(f, 4 + keysLeft.second, inputSchemas);

        int p = 5 + keysLeft.second + keysRight.second;
        boolean loInclusive = isTrue((ConstantExpression) f.getArguments().get(p).getValue());
        boolean hiInclusive = isTrue((ConstantExpression) f.getArguments().get(p + 1).getValue());
        buildBtreeSearch(builder, context, unnestMap, opSchema, datasetName, idxName, keysLeft.first, keysRight.first,
                loInclusive, hiInclusive);
    }

    private boolean isTrue(ConstantExpression k) {
        return k.getValue().isTrue();
    }

    private static void buildBtreeSearch(IHyracksJobBuilder builder, JobGenContext context, AbstractScanOperator scan,
            IOperatorSchema opSchema, String datasetName, String indexName, int[] lowKeyFields, int[] highKeyFields,
            boolean lowKeyInclusive, boolean highKeyInclusive) throws AlgebricksException, AlgebricksException {
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations metadata = mp.getMetadataDeclarations();
        AqlCompiledDatasetDecl adecl = metadata.findDataset(datasetName);
        if (adecl == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        if (adecl.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("Trying to run btree search over external dataset (" + datasetName + ").");
        }
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> btreeSearch = AqlMetadataProvider.buildBtreeRuntime(
                metadata, context, builder.getJobSpec(), datasetName, adecl, indexName, lowKeyFields, highKeyFields,
                lowKeyInclusive, highKeyInclusive);
        builder.contributeHyracksOperator(scan, btreeSearch.first);
        builder.contributeAlgebricksPartitionConstraint(btreeSearch.first, btreeSearch.second);

        ILogicalOperator srcExchange = scan.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, scan, 0);
    }

}