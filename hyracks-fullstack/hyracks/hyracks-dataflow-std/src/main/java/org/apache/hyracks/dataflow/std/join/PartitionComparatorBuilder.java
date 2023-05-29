package org.apache.hyracks.dataflow.std.join;

import java.util.Comparator;

public class PartitionComparatorBuilder {
    Comparator<Partition> comparator;

    public void addInMemoryTupleComparator(boolean reversed) {
        Comparator<Partition> newComparator = Comparator.comparing(Partition::getTuplesInMemory);
        addComparator(reversed ? newComparator.reversed() :  newComparator );
    }
    public void addBufferSizeComparator(boolean reversed) {
        Comparator<Partition> newComparator = Comparator.comparing(Partition::getMemoryUsed);
        addComparator(reversed ? newComparator.reversed() :  newComparator );
    }
    public void addStatusComparator(boolean reversed) {
        Comparator<Partition> newComparator = Comparator.comparing(Partition::getSpilledStatus);
        addComparator(reversed ? newComparator.reversed() :  newComparator );
    }
    private void addComparator(Comparator<Partition> newComparator){
        if(comparator == null){
            comparator = newComparator;
        }
        else{
            comparator.thenComparing(newComparator);
        }
    }
    public Comparator<Partition> build() {
        return comparator;
    }
}
