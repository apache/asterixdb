package edu.uci.ics.hyracks.api.dataflow.value;

import java.io.Serializable;

public interface ITuplePartitionComputerGeneratorFactory extends Serializable {
    public ITuplePartitionComputer createPartitioner(int seed);
}
