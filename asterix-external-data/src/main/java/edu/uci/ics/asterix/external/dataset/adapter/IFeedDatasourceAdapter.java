package edu.uci.ics.asterix.external.dataset.adapter;

import edu.uci.ics.asterix.om.types.ARecordType;

public interface IFeedDatasourceAdapter extends IDatasourceAdapter {

    /**
     * Represents the kind of data exchange that happens between the adapter and
     * the external data source. The data exchange can be either pull based or
     * push based. In the former case (pull), the request for data transfer is
     * initiated by the adapter. In the latter case (push) the adapter is
     * required to submit an initial request to convey intent for data.
     * Subsequently all data transfer requests are initiated by the external
     * data source.
     */
    public enum AdapterDataFlowType {
        PULL,
        PUSH
    }

    /**
     * An adapter can be a pull or a push based adapter. This method returns the
     * kind of adapter, that is whether it is a pull based adapter or a push
     * based adapter.
     * 
     * @caller Compiler or wrapper operator: Compiler uses this API to choose
     *         the right wrapper (push-based) operator that wraps around the
     *         adapter and provides an iterator interface. If we decide to form
     *         a single operator that handles both pull and push based adapter
     *         kinds, then this method will be used by the wrapper operator for
     *         switching between the logic for interacting with a pull based
     *         adapter versus a push based adapter.
     * @return AdapterDataFlowType
     */
    public AdapterDataFlowType getAdapterDataFlowType();

    public ARecordType getAdapterOutputType();

}
