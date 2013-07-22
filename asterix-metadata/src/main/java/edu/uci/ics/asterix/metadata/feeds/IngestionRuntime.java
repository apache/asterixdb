package edu.uci.ics.asterix.metadata.feeds;

public class IngestionRuntime extends FeedRuntime {

    private AdapterRuntimeManager adapterRuntimeManager;

    public IngestionRuntime(FeedConnectionId feedId, int partition, FeedRuntimeType feedRuntimeType,
            AdapterRuntimeManager adaptorRuntimeManager) {
        super(feedId, partition, feedRuntimeType);
        this.adapterRuntimeManager = adaptorRuntimeManager;
    }

    public AdapterRuntimeManager getAdapterRuntimeManager() {
        return adapterRuntimeManager;
    }

}
