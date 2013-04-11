package edu.uci.ics.asterix.external.library;


public interface IExternalScalarFunction extends IExternalFunction {

    public void initialize(IFunctionHelper functionHelper);
    
    public void evaluate(IFunctionHelper functionHelper) throws Exception;

}
