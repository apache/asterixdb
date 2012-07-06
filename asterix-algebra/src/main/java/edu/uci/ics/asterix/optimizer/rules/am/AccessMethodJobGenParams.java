package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl.IndexKind;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;

/**
 * Helper class for reading and writing job-gen parameters for access methods to
 * and from a list of function arguments, typically of an unnest-map.
 */
public class AccessMethodJobGenParams {
    protected String indexName;
    protected IndexKind indexKind;
    protected String datasetName;
    protected boolean retainInput;
    protected boolean requiresBroadcast;
    
    private final int NUM_PARAMS = 5;
    
    public AccessMethodJobGenParams() {
    }
    
    public AccessMethodJobGenParams(String indexName, IndexKind indexKind, String datasetName, boolean retainInput, boolean requiresBroadcast) {
        this.indexName = indexName;
        this.indexKind = indexKind;
        this.datasetName = datasetName;
        this.retainInput = retainInput;
        this.requiresBroadcast = requiresBroadcast;
    }
    
    public void writeToFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(indexName)));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createInt32Constant(indexKind.ordinal())));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(datasetName)));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createBooleanConstant(retainInput)));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createBooleanConstant(requiresBroadcast)));
    }

    public void readFromFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        indexName = AccessMethodUtils.getStringConstant(funcArgs.get(0));
        indexKind = IndexKind.values()[AccessMethodUtils.getInt32Constant(funcArgs.get(1))];
        datasetName = AccessMethodUtils.getStringConstant(funcArgs.get(2));
        retainInput = AccessMethodUtils.getBooleanConstant(funcArgs.get(3));
        requiresBroadcast = AccessMethodUtils.getBooleanConstant(funcArgs.get(4));
    }
    
    public String getIndexName() {
        return indexName;
    }

    public IndexKind getIndexKind() {
        return indexKind;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public boolean getRetainInput() {
        return retainInput;
    }

    public boolean getRequiresBroadcast() {
        return requiresBroadcast;
    }
    
    protected void writeVarList(List<LogicalVariable> varList, List<Mutable<ILogicalExpression>> funcArgs) {
        Mutable<ILogicalExpression> numKeysRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                new AsterixConstantValue(new AInt32(varList.size()))));
        funcArgs.add(numKeysRef);
        for (LogicalVariable keyVar : varList) {
            Mutable<ILogicalExpression> keyVarRef = new MutableObject<ILogicalExpression>(
                    new VariableReferenceExpression(keyVar));
            funcArgs.add(keyVarRef);
        }
    }
    
    protected int readVarList(List<Mutable<ILogicalExpression>> funcArgs, int index, List<LogicalVariable> varList) {
        int numLowKeys = AccessMethodUtils.getInt32Constant(funcArgs.get(index));
        if (numLowKeys > 0) {
            for (int i = 0; i < numLowKeys; i++) {
                LogicalVariable var = ((VariableReferenceExpression) funcArgs.get(index + 1 + i).getValue())
                        .getVariableReference();
                varList.add(var);
            }
        }
        return index + numLowKeys + 1;
    }
    
    protected int getNumParams() {
        return NUM_PARAMS;
    }
}
