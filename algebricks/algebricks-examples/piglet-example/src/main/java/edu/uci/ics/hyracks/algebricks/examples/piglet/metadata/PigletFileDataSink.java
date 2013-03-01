package edu.uci.ics.hyracks.algebricks.examples.piglet.metadata;

import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FileSplitDomain;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class PigletFileDataSink implements IDataSink {
    private String file;

    private FileSplit[] fileSplits;

    private IPartitioningProperty partProp;

    public PigletFileDataSink(String file) {
        this.file = file;
        fileSplits = FileSplitUtils.parseFileSplits(file);
        partProp = new RandomPartitioningProperty(new FileSplitDomain(fileSplits));
    }

    @Override
    public Object getId() {
        return file;
    }

    public FileSplit[] getFileSplits() {
        return fileSplits;
    }

    @Override
    public Object[] getSchemaTypes() {
        return null;
    }

    @Override
    public IPartitioningProperty getPartitioningProperty() {
        return partProp;
    }
}