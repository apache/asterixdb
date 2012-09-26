package edu.uci.ics.hyracks.algebricks.core.algebra.properties;

import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypeEnvPointer;

public abstract class TypePropagationPolicy {
    public static final TypePropagationPolicy ALL = new TypePropagationPolicy() {

        @Override
        public Object getVarType(LogicalVariable var, INullableTypeComputer ntc,
                List<LogicalVariable> nonNullVariableList, ITypeEnvPointer... typeEnvs) throws AlgebricksException {
            for (ITypeEnvPointer p : typeEnvs) {
                IVariableTypeEnvironment env = p.getTypeEnv();
                if (env == null) {
                    throw new AlgebricksException("Null environment for pointer " + p + " in getVarType for var=" + var);
                }
                Object t = env.getVarType(var, nonNullVariableList);
                if (t != null) {
                    return t;
                }
            }
            return null;
        }
    };

    public static final TypePropagationPolicy LEFT_OUTER = new TypePropagationPolicy() {

        @Override
        public Object getVarType(LogicalVariable var, INullableTypeComputer ntc,
                List<LogicalVariable> nonNullVariableList, ITypeEnvPointer... typeEnvs) throws AlgebricksException {
            int n = typeEnvs.length;
            for (int i = 0; i < n; i++) {
                Object t = typeEnvs[i].getTypeEnv().getVarType(var, nonNullVariableList);
                if (t != null) {
                    if (i == 0) { // inner branch
                        return t;
                    } else { // outer branch
                        boolean nonNullVarIsProduced = false;
                        for (LogicalVariable v : nonNullVariableList) {
                            if (v == var) {
                                nonNullVarIsProduced = true;
                                break;
                            }
                            if (typeEnvs[i].getTypeEnv().getVarType(v) != null) {
                                nonNullVarIsProduced = true;
                                break;
                            }
                        }
                        if (nonNullVarIsProduced) {
                            return t;
                        } else {
                            return ntc.makeNullableType(t);
                        }
                    }
                }
            }
            return null;
        }
    };

    public abstract Object getVarType(LogicalVariable var, INullableTypeComputer ntc,
            List<LogicalVariable> nonNullVariableList, ITypeEnvPointer... typeEnvs) throws AlgebricksException;
}
