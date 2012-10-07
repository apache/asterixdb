package edu.uci.ics.pregelix.core.jobgen.provider;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.pregelix.core.base.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.pregelix.runtime.touchpoint.VLongAscNormalizedKeyComputerFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.VLongDescNormalizedKeyComputerFactory;

public class NormalizedKeyComputerFactoryProvider implements INormalizedKeyComputerFactoryProvider {

    public static INormalizedKeyComputerFactoryProvider INSTANCE = new NormalizedKeyComputerFactoryProvider();

    private NormalizedKeyComputerFactoryProvider() {

    }

    @SuppressWarnings("rawtypes")
    @Override
    public INormalizedKeyComputerFactory getAscINormalizedKeyComputerFactory(Class keyClass) {
        if (keyClass.getName().indexOf("VLongWritable") > 0)
            return new VLongAscNormalizedKeyComputerFactory();
        else
            return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public INormalizedKeyComputerFactory getDescINormalizedKeyComputerFactory(Class keyClass) {
        if (keyClass.getName().indexOf("VLongWritable") > 0)
            return new VLongDescNormalizedKeyComputerFactory();
        else
            return null;
    }
}
