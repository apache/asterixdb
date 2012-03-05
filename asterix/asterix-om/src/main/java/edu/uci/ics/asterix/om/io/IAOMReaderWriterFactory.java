package edu.uci.ics.asterix.om.io;

public interface IAOMReaderWriterFactory {
    public IAOMWriter createOMWriter();

    public IAOMReader createOMReader();
}
