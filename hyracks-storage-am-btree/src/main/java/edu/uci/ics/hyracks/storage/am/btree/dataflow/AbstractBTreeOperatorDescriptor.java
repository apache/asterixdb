package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessorFactory;

public abstract class AbstractBTreeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
	
	private static final long serialVersionUID = 1L;
	
	protected String btreeFileName;
	protected int btreeFileId;
		
	protected IFieldAccessorFactory[] fieldAccessorFactories;
	protected IBinaryComparatorFactory[] comparatorFactories;	
	
	protected IBTreeInteriorFrameFactory interiorFrameFactory;
	protected IBTreeLeafFrameFactory leafFrameFactory;
	
	protected IBufferCacheProvider bufferCacheProvider;
	protected IBTreeRegistryProvider btreeRegistryProvider;
	
	public AbstractBTreeOperatorDescriptor(JobSpecification spec, int inputArity, int outputArity, IFileSplitProvider fileSplitProvider, RecordDescriptor recDesc, IBufferCacheProvider bufferCacheProvider, IBTreeRegistryProvider btreeRegistryProvider,  int btreeFileId, String btreeFileName, IBTreeInteriorFrameFactory interiorFactory, IBTreeLeafFrameFactory leafFactory, IFieldAccessorFactory[] fieldAccessorFactories, IBinaryComparatorFactory[] comparatorFactories) {
        super(spec, inputArity, outputArity);
        this.btreeFileId = btreeFileId;
        this.btreeFileName = btreeFileName;
        this.bufferCacheProvider = bufferCacheProvider;
        this.btreeRegistryProvider = btreeRegistryProvider;        
        this.interiorFrameFactory = interiorFactory;
        this.leafFrameFactory = leafFactory;
        this.fieldAccessorFactories = fieldAccessorFactories;
        this.comparatorFactories = comparatorFactories;        
        if(outputArity > 0) recordDescriptors[0] = recDesc;   
    }

	public String getBtreeFileName() {
		return btreeFileName;
	}

	public int getBtreeFileId() {
		return btreeFileId;
	}

	public IBinaryComparatorFactory[] getComparatorFactories() {
		return comparatorFactories;
	}
	
	public IFieldAccessorFactory[] getFieldAccessorFactories() {
		return fieldAccessorFactories;
	}
		
	public IBTreeInteriorFrameFactory getInteriorFactory() {
		return interiorFrameFactory;
	}

	public IBTreeLeafFrameFactory getLeafFactory() {
		return leafFrameFactory;
	}

	public IBufferCacheProvider getBufferCacheProvider() {
		return bufferCacheProvider;
	}

	public IBTreeRegistryProvider getBtreeRegistryProvider() {
		return btreeRegistryProvider;
	}
	
	public RecordDescriptor getRecordDescriptor() {
		return recordDescriptors[0];    
	}
}
