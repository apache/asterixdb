package edu.uci.ics.asterix.metadata.declared;

import edu.uci.ics.asterix.metadata.feeds.IAdapterFactory;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class FieldExtractingAdapterFactory implements IAdapterFactory {

    private static final long serialVersionUID = 1L;

    private final IAdapterFactory wrappedAdapterFactory;

    private final RecordDescriptor inRecDesc;

    private final RecordDescriptor outRecDesc;

    private final int[][] extractFields;

    private final ARecordType rType;

    public FieldExtractingAdapterFactory(IAdapterFactory wrappedAdapterFactory, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc, int[][] extractFields, ARecordType rType) {
        this.wrappedAdapterFactory = wrappedAdapterFactory;
        this.inRecDesc = inRecDesc;
        this.outRecDesc = outRecDesc;
        this.extractFields = extractFields;
        this.rType = rType;
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return wrappedAdapterFactory.getSupportedOperations();
    }

    @Override
    public String getName() {
        return "FieldExtractingAdapter[ " + wrappedAdapterFactory.getName() + " ]";
    }

    @Override
    public AdapterType getAdapterType() {
        return wrappedAdapterFactory.getAdapterType();
    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return wrappedAdapterFactory.getPartitionConstraint();
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        IDatasourceAdapter wrappedAdapter = wrappedAdapterFactory.createAdapter(ctx, partition);
        return new FieldExtractingAdapter(ctx, inRecDesc, outRecDesc, extractFields, rType, wrappedAdapter);
    }

}
