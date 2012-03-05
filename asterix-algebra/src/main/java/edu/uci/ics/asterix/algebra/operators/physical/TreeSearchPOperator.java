package edu.uci.ics.asterix.algebra.operators.physical;


import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.AbstractScanPOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;

public abstract class TreeSearchPOperator extends AbstractScanPOperator {

    private IDataSourceIndex<String, AqlSourceId> idx;

    public TreeSearchPOperator(IDataSourceIndex<String, AqlSourceId> idx) {
        this.idx = idx;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        IDataSource<?> ds = idx.getDataSource();
        IDataSourcePropertiesProvider dspp = ds.getPropertiesProvider();
        AbstractScanOperator as = (AbstractScanOperator) op;
        deliveredProperties = dspp.computePropertiesVector(as.getVariables());
    }

    protected Pair<int[], Integer> getKeys(AbstractFunctionCallExpression f, int k, IOperatorSchema[] inputSchemas) {
        IAObject obj = ((AsterixConstantValue) ((ConstantExpression) f.getArguments().get(k).getValue())
                .getValue()).getObject();
        int numKeys = ((AInt32) obj).getIntegerValue();
        int[] keys = null;
        if (numKeys > 0) {
            keys = new int[numKeys];
            for (int i = 0; i < numKeys; i++) {
                LogicalVariable var = ((VariableReferenceExpression) f.getArguments().get(k + 1 + i).getValue())
                        .getVariableReference();
                keys[i] = inputSchemas[0].findVariable(var);
            }
        }
        return new Pair<int[], Integer>(keys, numKeys);
    }

    protected String getStringArgument(AbstractFunctionCallExpression f, int k) throws AlgebricksException {
        ILogicalExpression arg = f.getArguments().get(k).getValue();
        if (arg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            throw new NotImplementedException("Index search calls with non-constant " + k
                    + "-th argument are not implemented.");
        }
        ConstantExpression ce = (ConstantExpression) arg;
        if (!(ce.getValue() instanceof AsterixConstantValue)) {
            throw new AlgebricksException("Third argument to index-search() should be a string.");
        }
        IAObject v = ((AsterixConstantValue) ce.getValue()).getObject();
        if (v.getType().getTypeTag() != ATypeTag.STRING) {
            throw new AlgebricksException("Third argument to index-search() should be a string.");
        }
        return ((AString) v).getStringValue();
    }

}
