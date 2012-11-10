package edu.uci.ics.asterix.om.functions;

import java.io.Serializable;
import java.util.List;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public interface IExternalFunctionInfo extends IFunctionInfo, Serializable {

    public IResultTypeComputer getResultTypeComputer();
    
    public IAType getReturnType();

    public String getFunctionBody();

    public List<IAType> getParamList();

    public String getLanguage();

    public FunctionKind getKind();

}
