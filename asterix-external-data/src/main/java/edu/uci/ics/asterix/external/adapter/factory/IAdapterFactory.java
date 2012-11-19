package edu.uci.ics.asterix.external.adapter.factory;

public interface IAdapterFactory {

    public enum AdapterType {
        EXTERNAL_DATASET,
        FEED
    }

    public AdapterType getAdapterType();

    public String getName();
}
