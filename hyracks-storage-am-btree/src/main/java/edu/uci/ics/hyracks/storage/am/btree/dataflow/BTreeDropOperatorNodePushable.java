package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.File;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.FileManager;

public class BTreeDropOperatorNodePushable implements IOperatorNodePushable {

	private String btreeFileName;
	private IBTreeRegistryProvider btreeRegistryProvider;
	private IBufferCacheProvider bufferCacheProvider;
	private int btreeFileId;
	private boolean isLocalCluster;
	
	public BTreeDropOperatorNodePushable(IBufferCacheProvider bufferCacheProvider, IBTreeRegistryProvider btreeRegistryProvider, int btreeFileId, String btreeFileName, boolean isLocalCluster) {
		this.btreeFileName = btreeFileName;
		this.btreeFileId = btreeFileId;
		this.bufferCacheProvider = bufferCacheProvider;
		this.btreeRegistryProvider = btreeRegistryProvider;
		this.isLocalCluster = isLocalCluster;
	}

	@Override
	public void deinitialize() throws HyracksDataException {		
	}

	@Override
	public int getInputArity() {
		return 0;
	}
	
	@Override
	public IFrameWriter getInputFrameWriter(int index) {
		return null;
	}

	@Override
	public void initialize() throws HyracksDataException {
		
		BTreeRegistry btreeRegistry = btreeRegistryProvider.getBTreeRegistry();		
		FileManager fileManager = bufferCacheProvider.getFileManager();
		IBufferCache bufferCache = bufferCacheProvider.getBufferCache();			
		
		// unregister btree instance            
		btreeRegistry.lock();
		try {
			btreeRegistry.unregister(btreeFileId);		
		} finally {
			btreeRegistry.unlock();
		}

		// unregister file
		fileManager.unregisterFile(btreeFileId);
		
		
		String fileName = btreeFileName;
        if(isLocalCluster) {
        	String s = bufferCache.toString();
            String[] splits = s.split("\\.");
        	String bufferCacheAddr = splits[splits.length-1].replaceAll("BufferCache@", "");
        	fileName = fileName + bufferCacheAddr;
        }
					        
        File f = new File(fileName);
        if (f.exists()) {
			f.delete();
		}       
	}

	@Override
	public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {				
	}		
}
