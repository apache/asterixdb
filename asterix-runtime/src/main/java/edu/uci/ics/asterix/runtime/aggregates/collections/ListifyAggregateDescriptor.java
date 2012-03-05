package edu.uci.ics.asterix.runtime.aggregates.collections;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IAggregateFunctionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class ListifyAggregateDescriptor extends AbstractAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "listify", 1,
            true);

    private AOrderedListType oltype;

    public void reset(AOrderedListType orderedListType) {
        this.oltype = orderedListType;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    @Override
    public IAggregateFunctionFactory createAggregateFunctionFactory(final IEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ListifyAggregateFunctionEvalFactory(args, oltype);
    }

}
