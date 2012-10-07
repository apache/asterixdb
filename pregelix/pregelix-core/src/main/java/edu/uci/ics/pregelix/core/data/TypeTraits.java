package edu.uci.ics.pregelix.core.data;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;

public class TypeTraits implements ITypeTraits {

    private static final long serialVersionUID = 1L;
    private final int length;
    private final boolean isFixedLength;

    public TypeTraits(boolean isFixedLength) {
        this.isFixedLength = isFixedLength;
        this.length = 0;
    }

    public TypeTraits(int length) {
        this.isFixedLength = true;
        this.length = length;
    }

    @Override
    public boolean isFixedLength() {
        return isFixedLength;
    }

    @Override
    public int getFixedLength() {
        return length;
    }

}
