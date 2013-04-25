package edu.uci.ics.asterix.external.library;

public interface IExternalScalarFunction extends IExternalFunction {

    public void initialize(IFunctionHelper functionHelper) throws Exception;

    public void evaluate(IFunctionHelper functionHelper) throws Exception;

}
