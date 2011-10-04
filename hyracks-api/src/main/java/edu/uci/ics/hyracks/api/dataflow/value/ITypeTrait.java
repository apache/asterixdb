package edu.uci.ics.hyracks.api.dataflow.value;

import java.io.Serializable;

public interface ITypeTrait extends Serializable {
	public static final int VARIABLE_LENGTH = -1;
	
	public static final ITypeTrait INTEGER_TYPE_TRAIT = new TypeTrait(4);
    public static final ITypeTrait INTEGER64_TYPE_TRAIT = new TypeTrait(8);
    public static final ITypeTrait FLOAT_TYPE_TRAIT = new TypeTrait(4);
    public static final ITypeTrait DOUBLE_TYPE_TRAIT = new TypeTrait(8);
    public static final ITypeTrait BOOLEAN_TYPE_TRAIT = new TypeTrait(1);
    public static final ITypeTrait VARLEN_TYPE_TRAIT = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);	
	int getStaticallyKnownDataLength();
}
