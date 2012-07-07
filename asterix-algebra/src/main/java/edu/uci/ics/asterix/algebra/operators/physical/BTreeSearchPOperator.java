package edu.uci.ics.asterix.algebra.operators.physical;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.optimizer.rules.am.BTreeJobGenParams;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;

/**
 * Contributes the runtime operator for an unnest-map representing a BTree search.
 */
public class BTreeSearchPOperator extends IndexSearchPOperator {

    public BTreeSearchPOperator(IDataSourceIndex<String, AqlSourceId> idx, boolean requiresBroadcast) {
        super(idx, requiresBroadcast);
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
        if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new IllegalStateException();
        }
        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr;
        FunctionIdentifier funcIdent = unnestFuncExpr.getFunctionIdentifier();
        if (!funcIdent.equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
            return;
        }
        BTreeJobGenParams jobGenParams = new BTreeJobGenParams();
        jobGenParams.readFromFuncArgs(unnestFuncExpr.getArguments());
        int[] lowKeyIndexes = getKeyIndexes(jobGenParams.getLowKeyVarList(), inputSchemas);
        int[] highKeyIndexes = getKeyIndexes(jobGenParams.getHighKeyVarList(), inputSchemas);
        AqlMetadataProvider metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations metadata = metadataProvider.getMetadataDeclarations();
        Dataset dataset = metadata.findDataset(jobGenParams.getDatasetName());
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(op);
        List<LogicalVariable> outputVars = unnestMap.getVariables();
        if (jobGenParams.getRetainInput()) {
            outputVars = new ArrayList<LogicalVariable>();
            VariableUtilities.getLiveVariables(unnestMap, outputVars);
        }
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> btreeSearch = AqlMetadataProvider.buildBtreeRuntime(
                builder.getJobSpec(), outputVars, opSchema, typeEnv, metadata, context, jobGenParams.getRetainInput(),
                jobGenParams.getDatasetName(), dataset, jobGenParams.getIndexName(), lowKeyIndexes, highKeyIndexes,
                jobGenParams.isLowKeyInclusive(), jobGenParams.isHighKeyInclusive());
        builder.contributeHyracksOperator(unnestMap, btreeSearch.first);
        builder.contributeAlgebricksPartitionConstraint(btreeSearch.first, btreeSearch.second);

        ILogicalOperator srcExchange = unnestMap.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, unnestMap, 0);
    }
}