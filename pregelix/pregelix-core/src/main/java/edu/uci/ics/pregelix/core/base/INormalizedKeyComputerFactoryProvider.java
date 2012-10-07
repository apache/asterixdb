package edu.uci.ics.pregelix.core.base;

import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;

public interface INormalizedKeyComputerFactoryProvider {

    @SuppressWarnings("rawtypes")
    INormalizedKeyComputerFactory getAscINormalizedKeyComputerFactory(Class keyClass);

    @SuppressWarnings("rawtypes")
    INormalizedKeyComputerFactory getDescINormalizedKeyComputerFactory(Class keyClass);
}
