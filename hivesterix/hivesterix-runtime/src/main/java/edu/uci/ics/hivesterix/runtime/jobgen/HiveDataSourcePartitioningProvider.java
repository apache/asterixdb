package edu.uci.ics.hivesterix.runtime.jobgen;

import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;

public class HiveDataSourcePartitioningProvider implements IDataSourcePropertiesProvider {

    @Override
    public IPhysicalPropertiesVector computePropertiesVector(List<LogicalVariable> scanVariables) {
        IPartitioningProperty property = new RandomPartitioningProperty(new HiveDomain());
        IPhysicalPropertiesVector vector = new StructuralPropertiesVector(property,
                new LinkedList<ILocalStructuralProperty>());
        return vector;
    }
}
