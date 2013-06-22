package edu.uci.ics.asterix.external.library;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.functions.IExternalFunctionInfo;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;

public class ExternalFunctionDescriptorProvider {

    public static IFunctionDescriptor getExternalFunctionDescriptor(IExternalFunctionInfo finfo) throws AsterixException {
        switch (finfo.getKind()) {
            case SCALAR:
                return new ExternalScalarFunctionDescriptor(finfo);
            case AGGREGATE:
            case UNNEST:
                throw new AsterixException("Unsupported function kind :" + finfo.getKind());
            default:
                break;
        }
        return null;
    }

}

class ExternalScalarFunctionDescriptor extends AbstractScalarFunctionDynamicDescriptor implements IFunctionDescriptor {

    private final IFunctionInfo finfo;
    private ICopyEvaluatorFactory evaluatorFactory;
    private ICopyEvaluatorFactory[] args;

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(ICopyEvaluatorFactory[] args) throws AlgebricksException {
        evaluatorFactory = new ExternalScalarFunctionEvaluatorFactory((IExternalFunctionInfo) finfo, args);
        return evaluatorFactory;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return finfo.getFunctionIdentifier();
    }

    public ExternalScalarFunctionDescriptor(IFunctionInfo finfo) {
        this.finfo = finfo;
    }

}

