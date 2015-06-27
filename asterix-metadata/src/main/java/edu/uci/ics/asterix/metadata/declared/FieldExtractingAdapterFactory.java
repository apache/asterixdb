package edu.uci.ics.asterix.metadata.declared;

import java.util.Map;

import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.metadata.external.IAdapterFactory;
import edu.uci.ics.asterix.metadata.external.IAdapterFactory.SupportedOperation;
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
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return wrappedAdapterFactory.getPartitionConstraint();
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        IDatasourceAdapter wrappedAdapter = wrappedAdapterFactory.createAdapter(ctx, partition);
        return new FieldExtractingAdapter(ctx, inRecDesc, outRecDesc, extractFields, rType, wrappedAdapter);
    }
    
    @Override
    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception {
        wrappedAdapterFactory.configure(configuration, outputType);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return wrappedAdapterFactory.getAdapterOutputType();
    }

}
