package edu.uci.ics.hyracks.algebricks.examples.piglet.metadata;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FileSplitDomain;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class PigletFileDataSource implements IDataSource<String> {
    private final String file;

    private final Object[] types;

    private final FileSplit[] fileSplits;

    private IDataSourcePropertiesProvider propProvider;

    public PigletFileDataSource(String file, Object[] types) {
        this.file = file;
        this.types = types;
        fileSplits = FileSplitUtils.parseFileSplits(file);
        final IPhysicalPropertiesVector vec = new StructuralPropertiesVector(new RandomPartitioningProperty(
                new FileSplitDomain(fileSplits)), new ArrayList<ILocalStructuralProperty>());
        propProvider = new IDataSourcePropertiesProvider() {
            @Override
            public IPhysicalPropertiesVector computePropertiesVector(List<LogicalVariable> scanVariables) {
                return vec;
            }
        };
    }

    @Override
    public String getId() {
        return file;
    }

    @Override
    public Object[] getSchemaTypes() {
        return types;
    }

    public FileSplit[] getFileSplits() {
        return fileSplits;
    }

    @Override
    public IDataSourcePropertiesProvider getPropertiesProvider() {
        return propProvider;
    }

    @Override
    public void computeFDs(List<LogicalVariable> scanVariables, List<FunctionalDependency> fdList) {
    }
}