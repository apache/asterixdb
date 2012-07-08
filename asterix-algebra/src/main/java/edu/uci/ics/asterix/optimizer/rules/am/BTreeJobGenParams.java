package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;

/**
 * Helper class for reading and writing job-gen parameters for BTree access methods to
 * and from a list of function arguments, typically of an unnest-map.
 */
public class BTreeJobGenParams extends AccessMethodJobGenParams {

    protected List<LogicalVariable> lowKeyVarList;
    protected List<LogicalVariable> highKeyVarList;

    protected boolean lowKeyInclusive;
    protected boolean highKeyInclusive;

    public BTreeJobGenParams() {
        super();
    }

    public BTreeJobGenParams(String indexName, IndexType indexType, String datasetName, boolean retainInput,
            boolean requiresBroadcast) {
        super(indexName, indexType, datasetName, retainInput, requiresBroadcast);
    }

    public void setLowKeyVarList(List<LogicalVariable> keyVarList, int startIndex, int numKeys) {
        lowKeyVarList = new ArrayList<LogicalVariable>(numKeys);
        setKeyVarList(keyVarList, lowKeyVarList, startIndex, numKeys);
    }

    public void setHighKeyVarList(List<LogicalVariable> keyVarList, int startIndex, int numKeys) {
        highKeyVarList = new ArrayList<LogicalVariable>(numKeys);
        setKeyVarList(keyVarList, highKeyVarList, startIndex, numKeys);
    }

    private void setKeyVarList(List<LogicalVariable> src, List<LogicalVariable> dest, int startIndex, int numKeys) {
        for (int i = 0; i < numKeys; i++) {
            dest.add(src.get(startIndex + i));
        }
    }

    public void setLowKeyInclusive(boolean lowKeyInclusive) {
        this.lowKeyInclusive = lowKeyInclusive;
    }

    public void setHighKeyInclusive(boolean highKeyInclusive) {
        this.highKeyInclusive = highKeyInclusive;
    }

    public void writeToFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.writeToFuncArgs(funcArgs);
        writeVarList(lowKeyVarList, funcArgs);
        writeVarList(highKeyVarList, funcArgs);
        writeKeyInclusive(lowKeyInclusive, funcArgs);
        writeKeyInclusive(highKeyInclusive, funcArgs);
    }

    public void readFromFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.readFromFuncArgs(funcArgs);
        int index = super.getNumParams();
        lowKeyVarList = new ArrayList<LogicalVariable>();
        highKeyVarList = new ArrayList<LogicalVariable>();
        int nextIndex = readVarList(funcArgs, index, lowKeyVarList);
        nextIndex = readVarList(funcArgs, nextIndex, highKeyVarList);
        readKeyInclusives(funcArgs, nextIndex);
    }

    private void readKeyInclusives(List<Mutable<ILogicalExpression>> funcArgs, int index) {
        lowKeyInclusive = ((ConstantExpression) funcArgs.get(index).getValue()).getValue().isTrue();
        highKeyInclusive = ((ConstantExpression) funcArgs.get(index + 1).getValue()).getValue().isTrue();
    }

    private void writeKeyInclusive(boolean keyInclusive, List<Mutable<ILogicalExpression>> funcArgs) {
        ILogicalExpression keyExpr = keyInclusive ? ConstantExpression.TRUE : ConstantExpression.FALSE;
        funcArgs.add(new MutableObject<ILogicalExpression>(keyExpr));
    }

    public List<LogicalVariable> getLowKeyVarList() {
        return lowKeyVarList;
    }

    public List<LogicalVariable> getHighKeyVarList() {
        return highKeyVarList;
    }

    public boolean isLowKeyInclusive() {
        return lowKeyInclusive;
    }

    public boolean isHighKeyInclusive() {
        return highKeyInclusive;
    }
}
