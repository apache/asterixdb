package edu.uci.ics.asterix.external.adapter.factory;


public interface IFeedDatasetAdapterFactory extends IAdapterFactory {

    public enum FeedAdapterType {
        GENERIC,
        TYPED
    }

    public FeedAdapterType getFeedAdapterType();

}
