package edu.uci.ics.asterix.common.config;


public interface IAsterixPropertiesProvider {
    public AsterixStorageProperties getStorageProperties();

    public AsterixTransactionProperties getTransactionProperties();

    public AsterixCompilerProperties getCompilerProperties();

    public AsterixMetadataProperties getMetadataProperties();

    public AsterixExternalProperties getExternalProperties();
}
