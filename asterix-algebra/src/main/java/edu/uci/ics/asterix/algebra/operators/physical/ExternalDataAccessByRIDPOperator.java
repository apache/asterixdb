package edu.uci.ics.asterix.algebra.operators.physical;

import edu.uci.ics.asterix.metadata.declared.AqlDataSource;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ExternalDataAccessByRIDOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.AbstractScanPOperator;
import edu.uci.ics.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;

public class ExternalDataAccessByRIDPOperator extends AbstractScanPOperator{

    private AqlSourceId datasetId;
    private Dataset dataset;
    private ARecordType recordType;
    private Index secondaryIndex;
	public ExternalDataAccessByRIDPOperator(AqlSourceId datasetId, Dataset dataset, ARecordType recordType,Index secondaryIndex)
    {
    	this.datasetId = datasetId;
    	this.dataset = dataset;
    	this.recordType = recordType;
    	this.secondaryIndex = secondaryIndex;
    }
    
	public Dataset getDataset() {
		return dataset;
	}

	public void setDataset(Dataset dataset) {
		this.dataset = dataset;
	}

	public ARecordType getRecordType() {
		return recordType;
	}

	public void setRecordType(ARecordType recordType) {
		this.recordType = recordType;
	}
	
	public AqlSourceId getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(AqlSourceId datasetId) {
		this.datasetId = datasetId;
	}
	
	@Override
	public PhysicalOperatorTag getOperatorTag() {
		return PhysicalOperatorTag.EXTERNAL_ACCESS_BY_RID;
	}

	@Override
	public void computeDeliveredProperties(ILogicalOperator op,
			IOptimizationContext context) throws AlgebricksException {
		AqlDataSource ds = new AqlDataSource(datasetId, dataset, recordType);
        IDataSourcePropertiesProvider dspp = ds.getPropertiesProvider();
        AbstractScanOperator as = (AbstractScanOperator) op;
        deliveredProperties = dspp.computePropertiesVector(as.getVariables());
	}

	@Override
	public void contributeRuntimeOperator(IHyracksJobBuilder builder,
			JobGenContext context, ILogicalOperator op,
			IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas,
			IOperatorSchema outerPlanSchema) throws AlgebricksException {
		ExternalDataAccessByRIDOperator edabro = (ExternalDataAccessByRIDOperator) op;
        ILogicalExpression expr = edabro.getExpressionRef().getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new IllegalStateException();
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        if (!funcIdent.equals(AsterixBuiltinFunctions.EXTERNAL_ACCESS_BY_RID)) {
            return;
        }
        AqlMetadataProvider metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> externalAccessByRID = metadataProvider.buildExternalDataAccesByRIDRuntime(
        		builder.getJobSpec(), dataset,secondaryIndex);
        builder.contributeHyracksOperator(edabro, externalAccessByRID.first);
        builder.contributeAlgebricksPartitionConstraint(externalAccessByRID.first, externalAccessByRID.second);
        ILogicalOperator srcExchange = edabro.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, edabro, 0);
	}
	
	@Override
	public boolean isMicroOperator() {
		return false;
	}

}