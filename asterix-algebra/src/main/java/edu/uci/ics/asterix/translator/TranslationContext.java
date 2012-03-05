package edu.uci.ics.asterix.translator;

import java.util.HashMap;

import edu.uci.ics.asterix.aql.expression.VariableExpr;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.Counter;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public final class TranslationContext {

    private Counter varCounter;
    private HashMap<Integer, LogicalVariable> varEnv = new HashMap<Integer, LogicalVariable>();
    private boolean topFlwor = true;

    public TranslationContext(Counter varCounter) {
        this.varCounter = varCounter;
    }

    public int getVarCounter() {
        return varCounter.get();
    }

    public boolean isTopFlwor() {
        return topFlwor;
    }

    public void setTopFlwor(boolean b) {
        topFlwor = b;
    }

    public LogicalVariable getVar(Integer varId) {
        return varEnv.get(varId);
    }

    public LogicalVariable getVar(VariableExpr v) {
        return varEnv.get(v.getVar().getId());
    }

    public LogicalVariable newVar(VariableExpr v) {
        Integer i = v.getVar().getId();
        LogicalVariable var = new LogicalVariable(i);
        varEnv.put(i, var);
        return var;
    }

    public void setVar(VariableExpr v, LogicalVariable var) {
        varEnv.put(v.getVar().getId(), var);
    }

    public LogicalVariable newVar() {
        varCounter.inc();
        LogicalVariable var = new LogicalVariable(varCounter.get());
        varEnv.put(varCounter.get(), var);
        return var;
    }
}
