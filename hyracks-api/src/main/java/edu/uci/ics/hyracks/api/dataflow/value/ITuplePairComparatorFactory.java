package edu.uci.ics.hyracks.api.dataflow.value;

import java.io.Serializable;


public interface ITuplePairComparatorFactory extends Serializable{
    
    public ITuplePairComparator createTuplePairComparator();
}
